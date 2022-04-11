#![deny(missing_docs)]
//! Caching library for JSON-RPC server. The library provides simple API to store and retrieve
//! solana account information. Information is kept up to date, by using websocket interface to
//! subscribe to particular account/program updates. Besides that, library also keeps track of the
//! latest slot in each of the available commitment levels.
//!

/// Main module for exposing caching API
pub mod cache;
/// Different cache entry related types
pub mod types;
mod ws;
