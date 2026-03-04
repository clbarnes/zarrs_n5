use zarrs::{
    node::{Node, NodePath, meta_key_v3},
    storage::{
        Bytes, ReadableStorageTraits, ReadableWritableListableStorage, ReadableWritableStorage,
        StorageError,
    },
};

use crate::N5Store;

/// Convert an N5 node to a Zarr v3 node by reading the N5 metadata and writing the corresponding Zarr metadata.
///
/// Then in future it can be opened by regular Zarr APIs without needing the N5Store wrapper.
///
/// `infer_missing_metadata` controls whether missing N5 metadata documents will be treated as empty groups (true) or an error (false).
/// If you are reasonably certain that the given path is an N5 node, this should probably be `true`.
///
/// <section class="warning">
/// This functionality is experimental and relies on unstable Zarr extensions which may not be supported by other implementations.
/// </section>
pub fn convert_n5_node(
    inner_store: ReadableWritableStorage,
    path: &NodePath,
    infer_missing_metadata: bool,
) -> Result<(), StorageError> {
    let mut n5_store = N5Store::new(inner_store.clone());
    n5_store.set_infer_missing_metadata(infer_missing_metadata);
    let k = meta_key_v3(path);

    // the N5Store will redirect the zarr.json key to the N5 attributes.json key,
    // parse and validate it, and convert it to Zarr v3 metadata,
    // so we don't need to parse these bytes
    let Some(b) = n5_store.get(&k)? else {
        return Err(StorageError::MissingMetadata(k.to_prefix()));
    };

    inner_store.set(&k, b)
}

/// Convert an N5 hierarchy to a Zarr v3 hierarchy by reading the N5 metadata for each node and writing the corresponding Zarr metadata.
///
/// Then in future it can be opened by regular Zarr APIs without needing the N5Store wrapper.
///
/// Does not verify that the given path is an N5 hierarchy root.
/// `infer_missing_metadata` controls whether missing N5 metadata documents will be treated as empty groups (true) or an error (false).
/// If you are reasonably certain that the given path is an N5 node, this should probably be `true`.
///
/// <section class="warning">
/// This functionality is experimental and relies on unstable Zarr extensions which may not be supported by other implementations.
/// </section>
pub fn convert_n5_hierarchy(
    inner_store: ReadableWritableListableStorage,
    path: &NodePath,
    infer_missing_metadata: bool,
) -> Result<(), StorageError> {
    let mut n5_store = N5Store::new(inner_store.clone());
    n5_store.set_infer_missing_metadata(infer_missing_metadata);
    let arc_n5_store = std::sync::Arc::new(n5_store);
    let root = Node::open(&arc_n5_store, path.as_str()).expect("node open");
    for node in std::iter::once(&root).chain(root.children().iter()) {
        let key = meta_key_v3(node.path());
        let val =
            Bytes::from(serde_json::to_vec(node.metadata()).expect("metadata should serialize"));
        inner_store.set(&key, val)?;
    }
    Ok(())
}
