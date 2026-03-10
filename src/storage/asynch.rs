use zarrs::storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableListableStorageTraits,
    AsyncReadableStorageTraits, MaybeBytes, StorageError, StoreKey, StoreKeys, StoreKeysPrefixes,
    StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use super::{ImplicitGroupStoreAdapter, N5StoreAdapter};

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<S: AsyncReadableStorageTraits> AsyncReadableStorageTraits for N5StoreAdapter<S> {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        if let Some(k) = self.intercept_zarr_json(key) {
            self.convert_metadata(&k, self.inner.get(&k).await?)
        } else {
            self.inner.get(key).await
        }
    }

    async fn get_partial(
        &self,
        _key: &StoreKey,
        _byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial not supported".into(),
        ))
    }

    async fn get_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial_many not supported".into(),
        ))
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        if let Some(k) = self.intercept_zarr_json(key) {
            self.inner.size_key(&k).await
        } else {
            self.inner.size_key(key).await
        }
    }

    fn supports_get_partial(&self) -> bool {
        false
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<S: AsyncListableStorageTraits> AsyncListableStorageTraits for N5StoreAdapter<S> {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.inner.list().await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.inner.list_prefix(prefix).await
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.inner.list_dir(prefix).await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.inner.size_prefix(prefix).await
    }

    async fn size(&self) -> Result<u64, StorageError> {
        self.inner.size().await
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S: AsyncReadableListableStorageTraits> N5StoreAdapter<S> {
    pub async fn async_infer_array_mode(
        &self,
        prefix: &StorePrefix,
    ) -> Result<Option<super::N5ArrayMode>, StorageError> {
        // TODO: could parallelise
        for key in self.filter_chunk_keys(prefix, self.list_prefix(prefix).await?) {
            let Some(mode) = self.block_mode(self.get(&key).await?) else {
                continue;
            };
            return Ok(Some(mode.into()));
        }
        Ok(None)
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<S: AsyncReadableStorageTraits> AsyncReadableStorageTraits for ImplicitGroupStoreAdapter<S> {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        let value = self.inner.get(key).await?;
        Ok(self.maybe_infer_metadata(key, value))
    }

    async fn get_partial(
        &self,
        _key: &StoreKey,
        _byte_range: ByteRange,
    ) -> Result<MaybeBytes, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial not supported".into(),
        ))
    }

    async fn get_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        Err(StorageError::Unsupported(
            "get_partial_many not supported".into(),
        ))
    }

    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        self.inner.size_key(key).await
    }

    fn supports_get_partial(&self) -> bool {
        false
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<S: AsyncListableStorageTraits> AsyncListableStorageTraits for ImplicitGroupStoreAdapter<S> {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        self.inner.list().await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        self.inner.list_prefix(prefix).await
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        self.inner.list_dir(prefix).await
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        self.inner.size_prefix(prefix).await
    }

    async fn size(&self) -> Result<u64, StorageError> {
        self.inner.size().await
    }
}
