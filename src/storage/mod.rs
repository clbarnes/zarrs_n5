use bytes::{Buf, Bytes};
use zarrs::{
    metadata::v3::NodeMetadataV3,
    storage::{
        ListableStorageTraits, MaybeBytes, MaybeBytesIterator, ReadableStorageTraits, StorageError,
        StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
        byte_range::{ByteRange, ByteRangeIterator},
    },
};

#[cfg(feature = "async")]
mod asynch;

use crate::metadata::N5Metadata;

/// An N5 store wrapping another Zarr store,
/// which handles converting metadata.
#[derive(Debug, Clone)]
pub struct N5Store<S> {
    inner: S,
    infer_missing_metadata: bool,
}

impl<S> N5Store<S> {
    /// Create an N5 store wrapping some other store.
    /// The wrapper inherits the inner store's capabilities
    /// (sync, async, readable, listable).
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            infer_missing_metadata: false,
        }
    }

    /// N5 stores may omit group metadata documents, but Zarr stores may not.
    /// When `infer_missing_metadata == true`, a request for a metadata document will supply default group metadata if not found.
    pub fn set_infer_missing_metadata(&mut self, infer: bool) {
        self.infer_missing_metadata = infer;
    }

    /// N5 stores may omit group metadata documents, but Zarr stores may not.
    /// When `infer_missing_metadata == true`, a request for a metadata document will supply default group metadata if not found.
    pub fn infer_missing_metadata(&self) -> bool {
        self.infer_missing_metadata
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

    fn default_group_metadata() -> NodeMetadataV3 {
        NodeMetadataV3::Group(Default::default())
    }

    /// Convert N5 metadata to Zarr metadata.
    fn convert_metadata(&self, bytes: Option<Bytes>) -> Result<Option<Bytes>, StorageError> {
        let zarr = if let Some(b) = bytes {
            let n5: N5Metadata = serde_json::from_reader(b.reader()).map_err(|e| {
                StorageError::InvalidMetadata(
                    StoreKey::new("attributes.json").unwrap(),
                    format!("could not parse N5 metadata: {e}",),
                )
            })?;
            log::trace!("parsed N5 metadata: {:?}", n5);
            n5.try_into().map_err(|e| {
                StorageError::InvalidMetadata(
                    StoreKey::new("attributes.json").unwrap(),
                    format!("could not convert N5 metadata to Zarr metadata: {e}"),
                )
            })?
        } else if self.infer_missing_metadata {
            log::debug!("no N5 metadata found, using default Zarr group metadata");
            Self::default_group_metadata()
        } else {
            return Ok(None);
        };

        log::trace!("generated Zarr metadata: {}", zarr);
        match serde_json::to_vec(&zarr) {
            Ok(v) => Ok(Some(Bytes::from_owner(v))),
            Err(e) => Err(StorageError::InvalidMetadata(
                StoreKey::new("attributes.json").unwrap(),
                format!("could not serialize Zarr metadata: {e}"),
            )),
        }
    }

    /// Retrieve the inner store.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S: ReadableStorageTraits> ReadableStorageTraits for N5Store<S> {
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
        if let Some(k) = self.intercept_zarr_json(key) {
            self.convert_metadata(self.inner.get(&k)?)
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

impl<S: ListableStorageTraits> ListableStorageTraits for N5Store<S> {
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

#[allow(unused)]
pub fn n5_metadata(
    store: &impl ReadableStorageTraits,
    path: &StoreKey,
) -> Result<Option<N5Metadata>, StorageError> {
    let attr_key = StoreKey::new(format!("{path}/attributes.json"))?;
    let Some(b) = store.get(&attr_key)? else {
        return Ok(None);
    };
    let meta: N5Metadata = serde_json::from_reader(b.reader()).map_err(|e| {
        StorageError::InvalidMetadata(
            attr_key.clone(),
            format!("could not parse N5 metadata: {e}"),
        )
    })?;
    Ok(Some(meta))
}
