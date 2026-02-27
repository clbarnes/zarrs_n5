#![doc = include_str!("../README.md")]
//! ## Usage
//!
//! This crate is comprises
//!
//! - [storage::N5Store], which wraps other [zarrs] stores
//!   - implements reading and listing, blocking and async, as supported by the wrapped store
//! - [chunk_key_encoding::N5ChunkKeyEncoding], which handles the N5 block layout
//! - [codec::N5Codec], an array-to-bytes codec which handles the N5 block header, bigendian byte order, and compression
//!   - varlen and object chunk modes are not supported
//!   - not all N5 compressors are supported
//!
//! When `zarr.json` metadata is requested from the [storage::N5Store],
//! it is read from the corresponding N5 `attributes.json` and converted to Zarr v3 metadata on the fly.
//! This converted metadata contains configuration for the N5-specific chunk key encoding and codec plugins,
//! so regular [zarrs] APIs can be used transparently.
//!
//! Alternatively, N5 data can additionally contain a `zarr.json` with specific configuration to allow reading as zarr without the [storage::N5Store].

pub mod chunk;
pub mod chunk_key_encoding;
pub mod codec;
mod error;
pub mod metadata;
pub mod storage;

pub use zarrs;

pub use error::{Error, Result};
