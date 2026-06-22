// SPDX-License-Identifier: Apache-2.0

//! Internal utilities shared across test modules.
//!
//! These are `#[doc(hidden)]` and exposed only so inline-test modules
//! can use them without duplicating code.

use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Drive a `Ready`-on-first-poll future synchronously using a no-op waker.
///
/// Fine for stub futures that return `Ready` immediately.  Swap for a real
/// executor (e.g. `tokio::test`) when the stub futures start to `await`.
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
