#![doc = include_str!("../README.md")]
//! ## Usage
//!
//! This crate is comprises
//!
//! - [storage::N5Store], which wraps other [zarrs] stores
//!   - implements reading and listing, blocking and async, as supported by the wrapped store
//! - [codec::N5Codec], an array-to-bytes codec which handles the N5 block header, bigendian byte order, block data transposition, and compression
//!   - varlen and object chunk modes are not supported
//!   - not all N5 compressors are supported
//!
//! When `zarr.json` metadata is requested from the [storage::N5Store],
//! it is read from the corresponding N5 `attributes.json` and converted to Zarr v3 metadata on the fly.
//! This converted metadata contains configuration for the N5-specific chunk key encoding and codec plugins,
//! so regular [zarrs] APIs can be used transparently.
//!
//! Alternatively, N5 data can additionally contain a `zarr.json` with specific configuration to allow reading as zarr without the [storage::N5Store].

mod chunk;
pub use chunk::{N5BlockHeader, N5BlockMode};

mod codec;
pub use codec::{N5Codec, N5CodecConfiguration};

mod error;
pub use error::{Error, Result};

mod metadata;
pub use metadata::{N5ArrayMetadata, N5Compression, N5GroupMetadata, N5Metadata};

mod storage;
pub use storage::N5Store;

pub use zarrs;
