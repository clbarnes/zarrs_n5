use bytes::Buf;
use zarrs::{
    group::GroupMetadataV3,
    metadata::v3::NodeMetadataV3,
    node::NodePath,
    storage::{
        Bytes, ReadableStorageTraits, ReadableWritableListableStorage, StorageError, StoreKey,
        StorePrefix, discover_children,
    },
};

use crate::{N5_METADATA_KEY, N5ArrayMode, N5Metadata, storage::infer_array_mode};

fn implicit_group_attributes() -> serde_json::Map<String, serde_json::Value> {
    let mut attributes = serde_json::Map::new();
    attributes.insert("_implicit".to_string(), serde_json::Value::Bool(true));
    attributes
}

/// Return the metadata key given a node path for a specified metadata file name (e.g. zarr.json, .zarray, .zgroup, .zaatrs).
///
/// Copied from [zarrs::node::key].
#[must_use]
fn meta_key_any(path: &NodePath, metadata_file_name: &str) -> StoreKey {
    let path = path.as_str();
    if path.eq("/") {
        unsafe { StoreKey::new_unchecked(metadata_file_name.to_string()) }
    } else {
        let path = path.strip_prefix('/').unwrap_or(path);
        unsafe { StoreKey::new_unchecked(format!("{path}/{metadata_file_name}")) }
    }
}

fn meta_key_n5(path: &NodePath) -> StoreKey {
    meta_key_any(path, N5_METADATA_KEY)
}

fn default_metadata_bytes() -> Bytes {
    let group_attrs = implicit_group_attributes();
    let inferred_group_meta =
        NodeMetadataV3::Group(GroupMetadataV3::default().with_attributes(group_attrs));
    Bytes::from_owner(
        serde_json::to_vec(&inferred_group_meta)
            .expect("should be able to serialize inferred group metadata"),
    )
}

/// Convert an N5 node to Zarr v3 by reading the N5 metadata for each node and writing the corresponding Zarr metadata.
///
/// Then in future it can be opened by regular Zarr APIs without needing the N5Store wrapper.
///
/// Does not verify that the given path is an N5 hierarchy root.
/// `infer_missing_metadata` controls whether missing N5 metadata documents will be treated as empty groups (true) or an error (false).
/// If you are reasonably certain that the given path is an N5 node, this should probably be `true`.
///
/// N5 data types are partially described in the array metadata, but individual blocks describe their "mode" (default, varlength, or object).
/// If you want to treat all arrays as a specific mode, specify it in `array_mode`.
/// Otherwise, the mode will be inferred for each array by trying to find a block and reading its header.
///
/// `recursive` will descend into child groups to convert the entire hierarchy.
///
/// <section class="warning">
/// This functionality is experimental and relies on unstable Zarr extensions which may not be supported by other implementations.
/// </section>
pub fn convert_n5(
    inner_store: ReadableWritableListableStorage,
    path: &NodePath,
    infer_missing_metadata: bool,
    array_mode: Option<N5ArrayMode>,
    recursive: bool,
) -> Result<(), StorageError> {
    // if inferring missing metadata, what bytes are we writing for missing metadata
    let default_group_bytes = infer_missing_metadata.then(default_metadata_bytes);

    let root_meta = meta_key_n5(path);
    let root_prefix = root_meta.parent();

    // depth-first traversal of hierarchy
    let mut to_visit = vec![root_prefix];
    while let Some(prefix) = to_visit.pop() {
        let n5_key = prefix_to_n5_attrs(&prefix);
        let zarr_key = prefix_to_zarr_v3_meta(&prefix);

        let Some(b) = inner_store.get(&n5_key)? else {
            // N5 metadata missing
            let Some(def) = &default_group_bytes else {
                // caller did not want implicit groups
                return Err(StorageError::MissingMetadata(prefix));
            };
            // write default metadata and descend to children
            inner_store.set(&zarr_key, def.clone())?;
            if recursive {
                to_visit.extend(discover_children(&inner_store, &prefix)?);
            }
            continue;
        };

        let n5_meta: N5Metadata = serde_json::from_reader(b.reader()).map_err(|err| {
            StorageError::Other(format!("failed to parse N5 metadata at {n5_key}: {err}"))
        })?;
        match n5_meta {
            N5Metadata::Array(arrmeta) => {
                // use mode if specified, otherwise infer from blocks
                let mode = if let Some(m) = array_mode {
                    m
                } else {
                    infer_array_mode(&inner_store, &prefix)?.unwrap_or_else(|| {
                        log::warn!("Could not infer array mode for {}; using default", prefix);
                        N5ArrayMode::default()
                    })
                };

                let zmeta = NodeMetadataV3::Array(
                    arrmeta
                        .try_into_zarr(mode)
                        .map_err(|e| StorageError::InvalidMetadata(n5_key, e.to_string()))?,
                );
                // write zarr metadata, do not descend further
                inner_store.set(
                    &zarr_key,
                    Bytes::from(
                        serde_json::to_vec(&zmeta)
                            .expect("should be able to serialize converted array metadata"),
                    ),
                )?;
            }
            N5Metadata::Group(grpmeta) => {
                // convert group metadata and descend to children
                let zmeta = NodeMetadataV3::Group(grpmeta.into());
                inner_store.set(
                    &zarr_key,
                    Bytes::from(
                        serde_json::to_vec(&zmeta)
                            .expect("should be able to serialize converted group metadata"),
                    ),
                )?;
                if recursive {
                    to_visit.extend(discover_children(&inner_store, &prefix)?);
                }
            }
        }
    }
    Ok(())
}

fn prefix_to_key(prefix: &StorePrefix, name: &str) -> StoreKey {
    unsafe { StoreKey::new_unchecked(format!("{}{name}", prefix.as_str())) }
}

fn prefix_to_n5_attrs(prefix: &StorePrefix) -> StoreKey {
    prefix_to_key(prefix, N5_METADATA_KEY)
}

fn prefix_to_zarr_v3_meta(prefix: &StorePrefix) -> StoreKey {
    prefix_to_key(prefix, "zarr.json")
}
