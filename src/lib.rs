//#![feature(alloc_system)]

extern crate futures;
//extern crate alloc_system;

//mod ownedcell;
//mod spmc;
mod spmc_lockfree;

pub use ::spmc_lockfree::{channel,Sender,Receiver};