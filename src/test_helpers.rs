// SPDX-License-Identifier: Apache-2.0

//! Shared test utilities used across the nomad-rs crate.
//!
//! Provides a minimal single-threaded executor for driving futures to
//! completion without pulling in a runtime dependency like tokio.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Drive a future to completion on the current thread using a no-op waker.
///
/// Busy-polls — fine because every test future returns `Ready` on first
/// poll. Swap for a real executor when the code under test actually awaits.
#[must_use]
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut pinned = pin!(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(val) = pinned.as_mut().poll(&mut cx) {
            return val;
        }
    }
}
