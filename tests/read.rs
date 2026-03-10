use npyz::NpyFile;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zarrs::filesystem::FilesystemStore;
use zarrs::metadata::v3::NodeMetadataV3;
use zarrs::storage::WritableStorageTraits;
use zarrs::storage::store::MemoryStore;
use zarrs::storage::{ReadableListableStorage, ReadableStorageTraits, StoreKey};

fn data_dir() -> PathBuf {
    env_logger::try_init().ok();
    Path::new(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Get shape and values from the raw.npy file.
fn read_raw() -> (Vec<u64>, Vec<f32>) {
    let data = include_bytes!("../data/raw.npy");
    let f = NpyFile::new(data.as_slice()).expect("should be valid");
    let shape = f.shape().to_vec();
    let data = f
        .into_vec::<f32>()
        .expect("should be able to read data to vec");
    (shape, data)
}

fn read_fs_to_memory(path: impl AsRef<Path>) -> MemoryStore {
    let store = MemoryStore::default();
    let root = path.as_ref();
    let mut dirs_to_visit = vec![PathBuf::from(".")];
    while let Some(rel_p) = dirs_to_visit.pop() {
        let p = root.join(&rel_p);
        for entry in std::fs::read_dir(&p).expect("dir should be readable") {
            let entry = entry.expect("should be able to read directory entry");
            let ftype = entry.file_type().expect("should be able to get file type");
            if ftype.is_file() {
                let path = entry.path();
                let rel_path = path
                    .strip_prefix(root)
                    .expect("should be able to strip prefix");
                let key_str = rel_path.to_str().expect("path should be utf-8");
                let key = StoreKey::new(key_str).expect("key should be valid");
                let data = std::fs::read(path).expect("should be able to read file");
                store
                    .set(&key, data.into())
                    .expect("should be able to set store");
            } else if ftype.is_dir() {
                dirs_to_visit.push(entry.path());
            } else {
                panic!("unexpected file type");
            }
        }
    }
    store
}

fn inner_memory_store(name: &str) -> MemoryStore {
    let path = data_dir().join(format!("{name}.n5"));
    read_fs_to_memory(path)
}

fn inner_store() -> FilesystemStore {
    let dpath = data_dir();
    FilesystemStore::new(dpath).expect("should be able to create store")
}

fn n5_store() -> ReadableListableStorage {
    Arc::new(zarrs_n5::N5Store::new(inner_store()))
}

fn read_n5(name: &str) -> (Vec<u64>, Vec<f32>) {
    let array_name = format!("/{name}.n5");
    let store = n5_store();
    let array = zarrs::array::Array::open(store.clone(), &array_name).expect("open array");
    serde_json::to_string_pretty(array.metadata())
        .map(|s| log::trace!("metadata for {name}.n5:\n{s}"))
        .expect("metadata should be serializable");
    let shape = array.shape().to_vec();
    let data: Vec<f32> = array
        .retrieve_array_subset(&array.subset_all())
        .expect("retrieve all data");
    (shape, data)
}

fn check_read(name: &str) {
    let (raw_shape, raw_data) = read_raw();
    let (n5_shape, n5_data) = read_n5(name);
    assert_eq!(raw_shape, n5_shape);
    assert_eq!(raw_data, n5_data);
}

#[test]
fn test_single_chunk() {
    check_read("single_chunk");
}

#[test]
fn test_even_chunk() {
    check_read("even_chunk");
}

#[test]
fn test_bz2() {
    check_read("bz2");
}

#[test]
fn test_gzip() {
    check_read("gzip");
}

#[test]
fn test_zstd() {
    check_read("zstd");
}

#[test]
fn test_blosc() {
    check_read("blosc");
}

#[test]
fn test_uneven_padded() {
    check_read("uneven_chunk_padded");
}

#[test]
fn test_uneven_truncated() {
    check_read("uneven_chunk_truncated");
}

#[test]
fn test_convert_node() {
    let store = Arc::new(inner_memory_store("single_chunk"));
    zarrs_n5::convert_n5_node(store.clone(), &"/".try_into().unwrap(), false)
        .expect("should be able to convert node");

    let n5_attrs_bytes = store
        .get(&"attributes.json".try_into().unwrap())
        .expect("should be able to get attributes.json")
        .expect("attributes.json should exist");
    let n5_metadata: zarrs_n5::N5Metadata =
        serde_json::from_slice(&n5_attrs_bytes).expect("should be able to parse attributes.json");
    let expected_zarr_metadata: NodeMetadataV3 = n5_metadata
        .try_into()
        .expect("should be able to convert N5 metadata to Zarr metadata");

    let zarr_metadata_bytes = store
        .get(&"zarr.json".try_into().unwrap())
        .expect("should be able to get zarr.json")
        .expect("zarr.json should exist");
    let zarr_metadata: NodeMetadataV3 =
        serde_json::from_slice(&zarr_metadata_bytes).expect("should be able to parse zarr.json");
    assert_eq!(zarr_metadata, expected_zarr_metadata);
}
