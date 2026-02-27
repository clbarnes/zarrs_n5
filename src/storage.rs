use bytes::{Buf, Bytes};
use zarrs::{
    metadata::v3::NodeMetadataV3,
    storage::{
        ListableStorageTraits, MaybeBytes, MaybeBytesIterator, ReadableStorageTraits, StorageError,
        StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
        byte_range::{ByteRange, ByteRangeIterator},
    },
};

use crate::metadata::N5Metadata;

pub struct N5Store<R> {
    inner: R,
}

impl<R> N5Store<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
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

    /// Convert N5 metadata to Zarr metadata.
    fn convert_metadata(&self, bytes: Option<Bytes>) -> Result<Option<Bytes>, StorageError> {
        let Some(b) = bytes else {
            return Ok(None);
        };
        let n5: N5Metadata = serde_json::from_reader(b.reader()).map_err(|e| {
            StorageError::InvalidMetadata(
                StoreKey::new("attributes.json").unwrap(),
                format!("could not parse N5 metadata: {e}",),
            )
        })?;
        let zarr: NodeMetadataV3 = n5.try_into().map_err(|e| {
            StorageError::InvalidMetadata(
                StoreKey::new("attributes.json").unwrap(),
                format!("could not convert N5 metadata to Zarr metadata: {e}"),
            )
        })?;
        match serde_json::to_vec(&zarr) {
            Ok(v) => Ok(Some(Bytes::from_owner(v))),
            Err(e) => Err(StorageError::InvalidMetadata(
                StoreKey::new("attributes.json").unwrap(),
                format!("could not serialize Zarr metadata: {e}"),
            )),
        }
    }
}

impl<R: ReadableStorageTraits> ReadableStorageTraits for N5Store<R> {
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

// TODO: AsyncReadableStorageTraits?
// TODO: AsyncListableStorageTraits?

impl<R: ListableStorageTraits> ListableStorageTraits for N5Store<R> {
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
