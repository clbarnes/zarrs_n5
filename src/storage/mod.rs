use bytes::{Buf, Bytes};
use zarrs::{
    group::GroupMetadataV3,
    metadata::v3::NodeMetadataV3,
    storage::{
        ListableStorageTraits, MaybeBytes, MaybeBytesIterator, ReadableListableStorageTraits,
        ReadableStorageTraits, StorageError, StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
        byte_range::{ByteRange, ByteRangeIterator},
    },
};

#[cfg(feature = "async")]
mod asynch;

use crate::{N5BlockHeader, N5BlockMode, metadata::N5Metadata};

/// Which array type to assume when converting N5 array metadata to Zarr metadata.
///
/// VarLength and Object are not yet supported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum N5ArrayMode {
    #[default]
    Default,
    VarLength,
    Object,
}

impl From<N5BlockMode> for N5ArrayMode {
    fn from(value: N5BlockMode) -> Self {
        match value {
            N5BlockMode::Default => N5ArrayMode::Default,
            N5BlockMode::VarLength { .. } => N5ArrayMode::VarLength,
            N5BlockMode::Object => N5ArrayMode::Object,
        }
    }
}

/// An N5 store wrapping another Zarr store,
/// which handles converting metadata.
///
/// You may also want to wrap this in an [ImplicitGroupStoreAdapter] if you want to treat missing N5 metadata as empty groups,
/// per the N5 spec.
#[derive(Debug, Clone)]
pub struct N5StoreAdapter<S> {
    inner: S,
    array_mode: N5ArrayMode,
}

impl<S> N5StoreAdapter<S> {
    /// Create an N5 store wrapping some other store.
    /// The wrapper inherits the inner store's capabilities
    /// (sync, async, readable, listable).
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            array_mode: N5ArrayMode::Default,
        }
    }

    /// Set a new array mode, returning the old mode.
    pub fn set_array_mode(&mut self, mode: N5ArrayMode) -> N5ArrayMode {
        std::mem::replace(&mut self.array_mode, mode)
    }

    /// Map requests for zarr.json to attributes.json.
    ///
    /// Returns None if the request was _not_ for a zarr.json object.
    /// Otherwise, returns the key of the equivalent attributes.json object.
    fn intercept_zarr_json(&self, key: &StoreKey) -> Option<StoreKey> {
        // Intercept requests for attributes metadata and return the appropriate metadata if found.
        let s = key.as_str();
        let (prefix, suffix) = match s.rsplit_once('/') {
            Some(tup) => tup,
            None => ("", s),
        };
        if suffix == "zarr.json" {
            let new_suff = "attributes.json";
            let k = if prefix.is_empty() {
                StoreKey::new(new_suff).expect("simple key should be valid")
            } else {
                StoreKey::new(format!("{prefix}/{new_suff}"))
                    .expect("reconstructed key should be valid")
            };
            Some(k)
        } else {
            None
        }
    }

    fn convert_metadata(
        &self,
        store_key: &StoreKey,
        n5_meta_bytes: Option<Bytes>,
    ) -> Result<Option<Bytes>, StorageError> {
        let Some(b) = n5_meta_bytes else {
            return Ok(None);
        };
        let n5meta = serde_json::from_reader(b.reader()).map_err(|e| {
            StorageError::InvalidMetadata(
                StoreKey::new("attributes.json").unwrap(),
                format!("could not parse N5 metadata: {e}",),
            )
        })?;
        let node_meta = match n5meta {
            N5Metadata::Group(g) => NodeMetadataV3::Group(g.into()),
            N5Metadata::Array(a) => {
                let ameta = a.try_into_zarr(self.array_mode).map_err(|e| {
                    StorageError::InvalidMetadata(
                        store_key.clone(),
                        format!("could not convert N5 array metadata to Zarr metadata: {e}"),
                    )
                })?;
                NodeMetadataV3::Array(ameta)
            }
        };
        match serde_json::to_vec(&node_meta) {
            Ok(v) => Ok(Some(Bytes::from_owner(v))),
            Err(e) => Err(StorageError::InvalidMetadata(
                store_key.clone(),
                format!("could not serialize Zarr metadata: {e}"),
            )),
        }
    }

    /// Retrieve the inner store.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Iterate through the keys which look like N5 blocks in the given prefix.
    fn filter_chunk_keys(
        &self,
        prefix: &StorePrefix,
        keys: Vec<StoreKey>,
    ) -> impl Iterator<Item = StoreKey> {
        let prefix_len = prefix.as_str().len();

        keys.into_iter().filter(move |k| {
            let s = &k.as_str()[prefix_len..];
            s.chars().all(|c| c.is_ascii_digit() || c == '/')
        })
    }

    /// Read the block header and, if possible, return the block mode.
    fn block_mode(&self, value: Option<Bytes>) -> Option<N5BlockMode> {
        let v = value?;
        let header = N5BlockHeader::from_bytes(&v).ok()?;
        Some(header.mode)
    }
}

impl<S: ReadableStorageTraits> ReadableStorageTraits for N5StoreAdapter<S> {
    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        if let Some(k) = self.intercept_zarr_json(key) {
            self.inner.size_key(&k)
        } else {
            self.inner.size_key(key)
        }
    }

    fn supports_get_partial(&self) -> bool {
        false
    }

    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        if let Some(meta_key) = self.intercept_zarr_json(key) {
            self.convert_metadata(&meta_key, self.inner.get(&meta_key)?)
        } else {
            self.inner.get(key)
        }
    }

    fn get_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<MaybeBytesIterator<'a>, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial_many not supported".into(),
        ))
    }

    fn get_partial(
        &self,
        _key: &StoreKey,
        _byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial not supported".into(),
        ))
    }
}

impl<S: ReadableListableStorageTraits> N5StoreAdapter<S> {
    pub fn infer_array_mode(
        &self,
        prefix: &StorePrefix,
    ) -> Result<Option<N5ArrayMode>, StorageError> {
        // TODO: could parallelise
        for key in self.filter_chunk_keys(prefix, self.list_prefix(prefix)?) {
            let Some(mode) = self.block_mode(self.get(&key)?) else {
                continue;
            };
            return Ok(Some(mode.into()));
        }
        Ok(None)
    }
}

/// Iterate through the keys which look like N5 blocks in the given prefix.
fn filter_chunk_keys(prefix: &StorePrefix, keys: Vec<StoreKey>) -> impl Iterator<Item = StoreKey> {
    let prefix_len = prefix.as_str().len();

    keys.into_iter().filter(move |k| {
        let s = &k.as_str()[prefix_len..];
        s.chars().all(|c| c.is_ascii_digit() || c == '/')
    })
}

/// Read the block header and, if possible, return the block mode.
fn block_mode(value: Option<Bytes>) -> Option<N5BlockMode> {
    let v = value?;
    let header = N5BlockHeader::from_bytes(&v).ok()?;
    Some(header.mode)
}

pub fn infer_array_mode<S: ReadableListableStorageTraits + ?Sized>(
    store: &S,
    prefix: &StorePrefix,
) -> Result<Option<N5ArrayMode>, StorageError> {
    for key in filter_chunk_keys(prefix, store.list_prefix(prefix)?) {
        let Some(mode) = block_mode(store.get(&key)?) else {
            continue;
        };
        return Ok(Some(mode.into()));
    }
    Ok(None)
}

impl<S: ListableStorageTraits> ListableStorageTraits for N5StoreAdapter<S> {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        self.inner.list()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.inner.list_prefix(prefix)
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.inner.list_dir(prefix)
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.inner.size_prefix(prefix)
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.inner.size()
    }
}

/// A store adapter which treats missing Zarr metadata as empty groups.
///
/// This is useful for N5 hierarchies which follow the N5 spec of treating directories without attributes.json files as groups.
#[derive(Debug, Clone)]
pub struct ImplicitGroupStoreAdapter<S> {
    inner: S,
    implicit_metadata: Bytes,
}

impl<S> ImplicitGroupStoreAdapter<S> {
    /// Create an implicit group adapter wrapping some other store.
    /// The wrapper inherits the inner store's capabilities
    /// (sync, async, readable, listable).
    ///
    /// All implicit groups will have the given attributes.
    pub fn new_with_attributes(
        inner_store: S,
        attributes: serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        let metadata = GroupMetadataV3::default().with_attributes(attributes);
        Self::new_with_metadata(inner_store, metadata)
    }

    /// Create an implicit group adapter wrapping some other store.
    /// The wrapper inherits the inner store's capabilities
    /// (sync, async, readable, listable).
    ///
    /// See [Self::new_with_attributes] to apply some attributes to all implicit groups.
    pub fn new(inner_store: S) -> Self {
        Self::new_with_metadata(inner_store, Default::default())
    }

    fn new_with_metadata(inner_store: S, metadata: GroupMetadataV3) -> Self {
        let meta = NodeMetadataV3::Group(metadata);
        let v = serde_json::to_vec(&meta).expect("metadata should serialize");
        let implicit_metadata = Bytes::from_owner(v);

        Self {
            inner: inner_store,
            implicit_metadata,
        }
    }

    /// Retrieve the inner store.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Get a reference to the inner store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Get a mutable reference to the inner store.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }

    /// If the key is a Zarr metadata file and the value is missing, return metadata for an implicit group.
    pub fn maybe_infer_metadata(&self, key: &StoreKey, value: Option<Bytes>) -> Option<Bytes> {
        if let Some(v) = value {
            return Some(v);
        }

        let s = key.as_str();
        let suffix = match s.rsplit_once('/') {
            Some((_, suf)) => suf,
            None => s,
        };
        if suffix == "zarr.json" {
            return Some(self.implicit_metadata.clone());
        }
        None
    }
}

impl<S: ReadableStorageTraits> ReadableStorageTraits for ImplicitGroupStoreAdapter<S> {
    fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.inner.size_key(key)
    }

    fn supports_get_partial(&self) -> bool {
        false
    }

    fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let val = self.inner.get(key)?;
        Ok(self.maybe_infer_metadata(key, val))
    }

    fn get_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<MaybeBytesIterator<'a>, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial_many not supported".into(),
        ))
    }

    fn get_partial(
        &self,
        _key: &StoreKey,
        _byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial not supported".into(),
        ))
    }
}

impl<S: ListableStorageTraits> ListableStorageTraits for ImplicitGroupStoreAdapter<S> {
    fn list(&self) -> Result<StoreKeys, StorageError> {
        self.inner.list()
    }

    fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.inner.list_prefix(prefix)
    }

    fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.inner.list_dir(prefix)
    }

    fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.inner.size_prefix(prefix)
    }

    fn size(&self) -> Result<u64, StorageError> {
        self.inner.size()
    }
}
