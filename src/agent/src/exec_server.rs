// Copyright (c) 2026 Edgeless Systems GmbH
//
// SPDX-License-Identifier: Apache-2.0
//
// exec_server hosts an ExecNoninteractiveService ttrpc server over vsock.
// It is only started when both `agent.debug_console` and
// `agent.exec_noninteractive_vport` are configured.

use anyhow::{Context, Result};
use async_trait::async_trait;
use nix::sys::socket::{self, AddressFamily, SockFlag, SockType, VsockAddr};
use protocols::exec_noninteractive::{
    ExecCommandRequest, ExecCommandResponse, ExitStatus, StreamEvent,
};
use protocols::exec_noninteractive_ttrpc_async as exec_ttrpc;
use slog::Logger;
use std::os::fd::AsRawFd;
use std::os::unix::process::ExitStatusExt;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch::Receiver;
use ttrpc::asynchronous::SSSender;
use ttrpc::r#async::ServerStream;
use ttrpc::r#async::{Server as TtrpcServer, TtrpcContext};

pub async fn exec_noninteractive_handler(
    logger: Logger,
    port: u32,
    mut shutdown: Receiver<bool>,
) -> Result<()> {
    let logger = logger.new(o!("subsystem" => "exec-noninteractive"));

    let listenfd = socket::socket(
        AddressFamily::Vsock,
        SockType::Stream,
        SockFlag::SOCK_CLOEXEC,
        None,
    )?;
    let addr = VsockAddr::new(libc::VMADDR_CID_ANY, port);
    socket::bind(listenfd.as_raw_fd(), &addr)?;
    socket::listen(&listenfd, socket::Backlog::new(10)?)?;

    let service = Arc::new(ExecService {
        logger: logger.clone(),
    });
    let svc = exec_ttrpc::create_exec_noninteractive_service(service);

    let mut server = TtrpcServer::new()
        .bind(&format!("vsock://-1:{}", port))?
        .register_service(svc);

    server.start().await?;
    info!(logger, "server listening"; "port" => port);

    shutdown.changed().await.ok();
    info!(logger, "server shutting down");
    server.shutdown().await?;

    Ok(())
}

struct ExecService {
    logger: Logger,
}

#[async_trait]
impl exec_ttrpc::ExecNoninteractiveService for ExecService {
    async fn run_command(
        &self,
        _ctx: &TtrpcContext,
        stream: ServerStream<ExecCommandResponse, ExecCommandRequest>,
    ) -> ttrpc::Result<()> {
        let (tx, mut rx) = stream.split();

        let first = rx
            .recv()
            .await
            .map_err(|e| ttrpc::Error::Others(e.to_string()))?
            .ok_or_else(|| ttrpc::Error::Others("no command provided".into()))?;

        if !first.has_cmd() {
            return Err(ttrpc::Error::RpcStatus(ttrpc::get_status(
                ttrpc::Code::INVALID_ARGUMENT,
                "command is empty",
            )));
        }

        let mut child = tokio::process::Command::new(&first.cmd().args()[0])
            .args(&first.cmd().args()[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ttrpc::Error::Others(format!("spawn failed: {e}")))?;
        let logger = self.logger.new(o!("pid" => child.id()));
        info!(logger, "spawned command"; "command" => format!("{:?}", first.cmd().args()));

        let mut child_stdin = child.stdin.take().unwrap();
        let child_stdout = child.stdout.take().unwrap();
        let child_stderr = child.stderr.take().unwrap();

        let tx_stdout = tx.clone();
        let tx_stderr = tx.clone();

        let stdin_logger = logger.new(o!("stream" => "stdin"));
        let stdin_fwd = tokio::spawn(async move {
            while let Ok(Some(req)) = rx.recv().await {
                if !req.has_stdin() {
                    warn!(stdin_logger, "client sent invalid ExecCommandRequest"; "request" => format!("{}", req));
                    break;
                }
                let stdin_event = req.stdin();
                if stdin_event.has_eof() && stdin_event.eof() {
                    info!(stdin_logger, "client sent EOF");
                    break;
                }
                if stdin_event.has_error() {
                    warn!(stdin_logger, "client sent an error"; "error" => stdin_event.error());
                    break;
                }
                if stdin_event.has_data() {
                    // TODO(burgerdev): should this only break on EPIPE or something like that?
                    if let Err(e) = child_stdin.write_all(stdin_event.data()).await {
                        warn!(stdin_logger, "failed writing to child stdin"; "error" => &e);
                        break;
                    }
                }
            }
            info!(stdin_logger, "closing");
            // EOF on the receive stream closes process stdin
            drop(child_stdin);
        });

        let stdout_logger = logger.new(o!("stream" => "stdout"));
        let stdout_fwd = tokio::spawn(async move {
            match forward(child_stdout, tx_stdout, |evt| {
                let mut resp = ExecCommandResponse::new();
                resp.set_stdout(evt);
                resp
            })
            .await
            {
                Err(e) => warn!(stdout_logger, "forwarding failed"; "error" => e.to_string()),
                Ok(_) => info!(stdout_logger, "forwarding done"),
            };
        });

        let stderr_logger = logger.new(o!("stream" => "stderr"));
        let stderr_fwd = tokio::spawn(async move {
            match forward(child_stderr, tx_stderr, |evt| {
                let mut resp = ExecCommandResponse::new();
                resp.set_stderr(evt);
                resp
            })
            .await
            {
                Err(e) => warn!(stderr_logger, "forwarding failed"; "error" => e.to_string()),
                Ok(_) => info!(stderr_logger, "forwarding done"),
            }
        });

        let mut exit_status = ExitStatus::new();
        match child.wait().await {
            Ok(pstatus) => {
                if let Some(signo) = pstatus.signal() {
                    exit_status.set_signaled(signo)
                } else if let Some(code) = pstatus.code() {
                    exit_status.set_terminated(code)
                } else {
                    exit_status.set_unknown("ExitStatus had no exit code".to_owned())
                }
            }
            Err(e) => {
                warn!(logger, "waiting for child failed"; "error" => &e);
                exit_status.set_unknown(e.to_string())
            }
        };

        let _ = tokio::join!(stdout_fwd, stderr_fwd);
        // TODO(burgerdev): think about what to do with stdin.
        stdin_fwd.abort();

        info!(logger, "exec command exited"; "exit_code" => format!("{:?}", exit_status));

        let mut resp = ExecCommandResponse::new();
        resp.set_exit_status(exit_status);
        tx.send(&resp)
            .await
            .map_err(|e| ttrpc::Error::Others(format!("writing final response failed: {e}")))?;

        Ok(())
    }
}

async fn forward<R, C>(mut r: R, tx: SSSender<ExecCommandResponse>, cons: C) -> Result<()>
where
    R: AsyncRead + Unpin,
    C: Fn(StreamEvent) -> ExecCommandResponse,
{
    let mut buf = vec![0u8; 8192];
    let mut keep_going = true;
    while keep_going {
        let mut event = StreamEvent::new();
        match r.read(&mut buf).await {
            Ok(0) => {
                event.set_eof(true);
                keep_going = false;
            }
            Err(e) => {
                event.set_error(e.to_string());
                keep_going = false;
            }
            Ok(n) => {
                event.set_data(buf[..n].to_vec());
            }
        }
        tx.send(&cons(event)).await.context("sending event")?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocols::exec_noninteractive::exit_status::Status;
    use protocols::exec_noninteractive::stream_event::Event;
    use protocols::exec_noninteractive::{exec_command_response::Output, ExecCommandRequest};
    use protocols::exec_noninteractive_ttrpc_async::ExecNoninteractiveServiceClient;
    use ttrpc::r#async::Server as TtrpcServer;

    fn test_logger() -> Logger {
        slog::Logger::root(slog::Discard, o!())
    }

    async fn start_test_server(addr: &str) -> TtrpcServer {
        let service = Arc::new(ExecService {
            logger: test_logger(),
        });
        let svc = exec_ttrpc::create_exec_noninteractive_service(service);
        let mut server = TtrpcServer::new().bind(addr).unwrap().register_service(svc);
        server.start().await.unwrap();
        server
    }

    // Run a command through the exec service and collect all output.
    async fn run(addr: &str, cmd: Vec<&str>, stdin: &[u8]) -> (Vec<u8>, Vec<u8>, i32) {
        let client = ttrpc::r#async::Client::connect(addr).await.unwrap();
        let svc = ExecNoninteractiveServiceClient::new(client);
        let bidi = svc
            .run_command(ttrpc::context::with_timeout(0))
            .await
            .unwrap();
        let (tx, mut rx) = bidi.split();

        // Send command.
        let mut req = ExecCommandRequest::new();
        req.mut_cmd()
            .set_args(cmd.iter().map(|s| s.to_string()).collect());
        tx.send(&req).await.unwrap();

        // Send stdin.
        let mut req = ExecCommandRequest::new();
        req.mut_stdin().set_data(stdin.to_vec());
        tx.send(&req).await.unwrap();

        // Close stdin.
        let mut req = ExecCommandRequest::new();
        req.mut_stdin().set_eof(true);
        tx.send(&req).await.unwrap();

        let mut stdout_data = Vec::new();
        let mut stderr_data = Vec::new();
        loop {
            match rx.recv().await {
                Ok(resp) => match resp.output {
                    Some(Output::Stdout(d)) => handle_stream_event(&mut stdout_data, &d),
                    Some(Output::Stderr(d)) => handle_stream_event(&mut stderr_data, &d),
                    Some(Output::ExitStatus(status)) => {
                        let code = match status.status.unwrap() {
                            Status::Unknown(msg) => {
                                panic!("server reported unknown status: {}", msg)
                            }
                            Status::Signaled(signo) => 128 + signo,
                            Status::Terminated(code) => code,
                            s => panic!("server sent unsupported status message: {:?}", s),
                        };
                        return (stdout_data, stderr_data, code);
                    }
                    _ => {}
                },
                Err(e) => panic!("recv error: {}", e),
            }
        }
    }

    fn handle_stream_event(collector: &mut Vec<u8>, evt: &StreamEvent) {
        if let Some(e) = &evt.event {
            match e {
                Event::Data(d) => collector.extend_from_slice(&d),
                Event::Eof(_) => (),
                Event::Error(err) => panic!("{}", err),
                _ => panic!("unexpected event type: {:?}", e),
            }
        }
    }

    #[tokio::test]
    async fn test_stdout() {
        let addr = "unix://@/tmp/kata-exec-test-stdout";
        let mut server = start_test_server(addr).await;

        let (stdout, _stderr, code) = run(addr, vec!["echo", "hello world"], &[]).await;

        assert_eq!(code, 0);
        assert_eq!(String::from_utf8(stdout).unwrap().trim(), "hello world");

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_exit_code() {
        let addr = "unix://@/tmp/kata-exec-test-exit-code";
        let mut server = start_test_server(addr).await;

        let (_stdout, _stderr, code) = run(addr, vec!["sh", "-c", "exit 42"], &[]).await;

        assert_eq!(code, 42);

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_signal() {
        let addr = "unix://@/tmp/kata-exec-test-signal";
        let mut server = start_test_server(addr).await;

        let (_stdout, _stderr, code) = run(addr, vec!["sh", "-c", "kill $$"], &[]).await;

        assert_eq!(code, 143);

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_stderr() {
        let addr = "unix://@/tmp/kata-exec-test-stderr";
        let mut server = start_test_server(addr).await;

        let (_stdout, stderr, code) = run(addr, vec!["sh", "-c", "echo error >&2"], &[]).await;

        assert_eq!(code, 0);
        assert_eq!(String::from_utf8(stderr).unwrap().trim(), "error");

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_stdin_passthrough() {
        let addr = "unix://@/tmp/kata-exec-test-stdin";
        let mut server = start_test_server(addr).await;

        let (stdout, _stderr, code) = run(addr, vec!["cat"], b"hello from stdin").await;

        assert_eq!(code, 0);
        assert_eq!(stdout, b"hello from stdin");

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_command_not_found() {
        let addr = "unix://@/tmp/kata-exec-test-not-found";
        let mut server = start_test_server(addr).await;

        // A nonexistent command causes spawn() to fail.  The service returns a
        // ttrpc error rather than panicking, and the client sees the stream
        // close with an error on recv().
        let client = ttrpc::r#async::Client::connect(addr).await.unwrap();
        let svc = ExecNoninteractiveServiceClient::new(client);
        let bidi = svc
            .run_command(ttrpc::context::with_timeout(0))
            .await
            .unwrap();
        let (tx, mut rx) = bidi.split();

        let mut req = ExecCommandRequest::new();
        req.mut_cmd()
            .set_args(vec!["__nonexistent_command__".to_string()]);
        tx.send(&req).await.unwrap();

        // The server fails to spawn and returns a ttrpc error; the client
        // stream should close with Err rather than hanging.
        let result = rx.recv().await;
        assert!(
            result.is_err(),
            "expected ttrpc error for nonexistent command, got Ok"
        );

        server.shutdown().await.unwrap();
    }
}
