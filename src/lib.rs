#![doc = include_str!("../README.md")]
//! ## Usage
//!
//! This crate is comprises
//!
//! - [N5Store], which wraps other [zarrs] stores
//!   - implements reading and listing, blocking and async, as supported by the wrapped store
//! - [N5Codec], an array-to-bytes codec which handles the N5 block header, bigendian byte order, block data transposition, and compression
//!   - varlen and object chunk modes are not supported
//!   - not all N5 compressors are supported
//! - [convert_n5_node] and [convert_n5_hierarchy], which adds Zarr metadata to N5 objects to allow them to be read as Zarr without the [N5Store] wrapper
//!   - this functionality is experimental and relies on unstable Zarr extensions which may not be supported by other implementations
//!
//! When `zarr.json` (the usual home of Zarr metadata) is requested from the [N5Store],
//! it is read from the corresponding N5 `attributes.json` and converted to Zarr v3 metadata on the fly.
//! This converted metadata contains configuration for the N5-specific chunk key encoding and codec plugins,
//! so regular [zarrs] APIs can be used transparently.

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

mod convert;
pub use convert::{convert_n5_hierarchy, convert_n5_node};

pub use zarrs;
