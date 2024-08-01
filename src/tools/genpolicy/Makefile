# Copyright (c) 2020 Intel Corporation
#
# SPDX-License-Identifier: Apache-2.0
#

include ../../../utils.mk

FEATURES := 

ifeq ($(ARCH), ppc64le)
     override ARCH = powerpc64le
	 override FEATURES = --no-default-features
 endif

.DEFAULT_GOAL := default
default: build

build:
	@RUSTFLAGS="$(EXTRA_RUSTFLAGS) --deny warnings" cargo build --target $(TRIPLE) $(FEATURES) --$(BUILD_TYPE)

static-checks-build:
	@echo "INFO: static-checks-build do nothing.."

clean:
	cargo clean

vendor:
	cargo vendor

test:
	@RUSTFLAGS="$(EXTRA_RUSTFLAGS) --deny warnings" cargo test --all-targets --target $(TRIPLE) $(FEATURES)

install:
	@RUSTFLAGS="$(EXTRA_RUSTFLAGS) --deny warnings" cargo install --locked --target $(TRIPLE) $(FEATURES) --path .

check: standard_rust_check

.PHONY: \
	build \
	check \
	clean \
	install \
	test \
	vendor
