// Unit tests replace the Windows swarm/app handles so production handlers can be exercised
// without loading platform networking DLLs; the excluded production-only paths are intentional.
#![cfg_attr(test, allow(dead_code, unused_imports))]

mod behaviour;
mod discovery;
mod resumable_transfer;
mod runtime;
mod transfer;

pub use runtime::{NetworkCommand, NetworkHandle, spawn_network};
pub(crate) use transfer::return_pending_incoming_decision_to_manual;
