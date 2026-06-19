// Copyright (c) 2024 Edgeless Systems GmbH
//
// SPDX-License-Identifier: Apache-2.0
//
// exec_server hosts an ExecNoninteractiveService ttrpc server over vsock.
// It is only started when both `agent.debug_console` and
// `agent.exec_noninteractive_vport` are configured.

use anyhow::Result;
use async_trait::async_trait;
use nix::sys::socket::{self, AddressFamily, SockFlag, SockType, VsockAddr};
use protocols::exec_noninteractive::exec_command_response::Payload;
use protocols::exec_noninteractive::{ExecCommandRequest, ExecCommandResponse};
use protocols::exec_noninteractive_ttrpc_async as exec_ttrpc;
use slog::Logger;
use ttrpc::asynchronous::SSSender;
use std::io::Read;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch::Receiver;
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
    socket::bind(listenfd, &addr)?;
    socket::listen(listenfd, 10)?;

    let service = Arc::new(ExecService {
        logger: logger.clone(),
    });
    let svc = exec_ttrpc::create_exec_noninteractive_service(service);

    let mut server = TtrpcServer::new()
        .add_listener(listenfd)?
        .set_domain_vsock()
        .register_service(svc);

    server.start().await?;
    info!(logger, "exec noninteractive server listening"; "port" => port);

    shutdown.changed().await.ok();
    info!(logger, "exec noninteractive server shutting down");
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
            .args(&first.args()[1..])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| ttrpc::Error::Others(format!("spawn failed: {e}")))?;

        let mut child_stdin = child.stdin.take().unwrap();
        let mut child_stdout = child.stdout.take().unwrap();
        let mut child_stderr = child.stderr.take().unwrap();

        let tx_stdout = tx.clone();
        let tx_stderr = tx.clone();
        let logger = self.logger.clone();

        let stdin_fwd = tokio::spawn(async move {
            while let Ok(Some(req)) = rx.recv().await {
                if !req.has_stdin() {
                    continue
                }
                let event = req.stdin();
                if event.has_eof() && event.eof() {
                    break
                }
                if event.has_error() {
                    // TODO(burgerdev): now what?
                    break
                }
                if event.has_data() {
                    // TODO(burgerdev): should this only break on EPIPE or something like that?
                    if child_stdin.write_all(req.data()).await.is_err() {
                        break;
                    }
                }
            }
            // EOF on the receive stream closes process stdin
            drop(child_stdin);
        });

        let stdout_fwd = tokio::spawn(async move {
            forward(child_stdout, tx_stdout, Payload::Stdout);
        });

        let stderr_fwd = tokio::spawn(async move {
            forward(child_stderr, tx_stderr, Payload::Stderr);
        });

        let _ = tokio::join!(stdout_fwd, stderr_fwd);
        // TODO(burgerdev): just because stdout is closed does not mean stdin is done!
        // However, reading the docs of .wait(), it seems like stdin will be closed now anyway.
        // We should insist on a clean stdin close before continuing!
        stdin_fwd.abort();

        let exit_code = child
            .wait()
            .await
            .map(|s| s.code().unwrap_or(-1)) // TODO(burgerdev): use ExitStatusExt instead of default -1.
            .unwrap_or(-1);

        info!(logger, "exec command exited"; "exit_code" => exit_code);

        let mut resp = ExecCommandResponse::new();
        resp.payload = Some(Payload::ExitCode(exit_code));
        let _ = tx.send(&resp).await;

        Ok(())
    }
}

async fn forward<R, C>(r: R, tx: SSSender<ExecCommandResponse>, cons: C) -> ()
where R: Read, C: FnOnce(Vec<u8>) -> Payload {
    let mut buf = vec![0u8; 8192];
    loop {
        match r.read(&mut buf).await {
            // TODO(burgerdev): inspect the error and forward to caller
            Ok(0) | Err(_) => break,
            Ok(n) => {
                let mut resp = ExecCommandResponse::new();
                resp.payload = Some(cons(buf[..n].to_vec()));
                if tx.send(&resp).await.is_err() {
                    break;
                }
            }
        }
    }
    // TODO(burgerdev): signal closing
}


#[cfg(test)]
mod tests {
    use super::*;
    use protocols::exec_noninteractive::exec_command_response::Payload;
    use protocols::exec_noninteractive::ExecCommandRequest;
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
        let mut server = TtrpcServer::new()
            .bind(addr)
            .unwrap()
            .register_service(svc);
        server.start().await.unwrap();
        server
    }

    // Run a command through the exec service and collect all output.
    // Calls close_send() after the initial request so commands that read
    // stdin (e.g. cat) receive EOF and exit cleanly.
    async fn run(
        addr: &str,
        cmd: Vec<&str>,
        stdin: &[u8],
    ) -> (Vec<u8>, Vec<u8>, i32) {
        let client = ttrpc::r#async::Client::connect(addr).unwrap();
        let svc = ExecNoninteractiveServiceClient::new(client);
        let bidi = svc
            .run_command(ttrpc::context::with_timeout(0))
            .await
            .unwrap();
        let (tx, mut rx) = bidi.split();

        let mut req = ExecCommandRequest::new();
        req.cmd = cmd.iter().map(|s| s.to_string()).collect();
        req.stdin = stdin.to_vec();
        tx.send(&req).await.unwrap();
        // Signal EOF on the client send direction so stdin-consuming
        // commands know there is no more input.  Ignore errors here:
        // if the server closed the stream early the send side may
        // already be gone.
        let _ = tx.close_send().await;

        let mut stdout_data = Vec::new();
        let mut stderr_data = Vec::new();
        let mut exit_code = -1i32;
        loop {
            match rx.recv().await {
                Ok(resp) => match resp.payload {
                    Some(Payload::Stdout(d)) => stdout_data.extend_from_slice(&d),
                    Some(Payload::Stderr(d)) => stderr_data.extend_from_slice(&d),
                    Some(Payload::ExitCode(c)) => {
                        exit_code = c;
                        break;
                    }
                    None => {}
                },
                Err(_) => break,
            }
        }
        (stdout_data, stderr_data, exit_code)
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

        let (_stdout, _stderr, code) =
            run(addr, vec!["sh", "-c", "exit 42"], &[]).await;

        assert_eq!(code, 42);

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_stderr() {
        let addr = "unix://@/tmp/kata-exec-test-stderr";
        let mut server = start_test_server(addr).await;

        let (_stdout, stderr, code) =
            run(addr, vec!["sh", "-c", "echo error >&2"], &[]).await;

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
        let client = ttrpc::r#async::Client::connect(addr).unwrap();
        let svc = ExecNoninteractiveServiceClient::new(client);
        let bidi = svc
            .run_command(ttrpc::context::with_timeout(0))
            .await
            .unwrap();
        let (tx, mut rx) = bidi.split();

        let mut req = ExecCommandRequest::new();
        req.cmd = vec!["__nonexistent_command__".to_string()];
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
