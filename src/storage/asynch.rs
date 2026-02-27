use zarrs::storage::{
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits, MaybeBytes,
    StorageError, StoreKey, StoreKeys, StoreKeysPrefixes, StorePrefix,
    byte_range::{ByteRange, ByteRangeIterator},
};

use super::N5Store;

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<R: AsyncReadableStorageTraits> AsyncReadableStorageTraits for N5Store<R> {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        if let Some(k) = self.intercept_zarr_json(key) {
            self.convert_metadata(self.inner.get(&k).await?)
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
impl<R: AsyncListableStorageTraits> AsyncListableStorageTraits for N5Store<R> {
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
