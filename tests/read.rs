use npyz::NpyFile;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zarrs::filesystem::FilesystemStore;
use zarrs::storage::ReadableListableStorage;

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

fn inner_store() -> FilesystemStore {
    let dpath = data_dir();
    FilesystemStore::new(dpath).expect("should be able to create store")
}

fn n5_store() -> ReadableListableStorage {
    Arc::new(zarrs_n5::storage::N5Store::new(inner_store()))
}

fn read_n5(name: &str) -> (Vec<u64>, Vec<f32>) {
    let array_name = format!("/{name}.n5");
    let store = n5_store();
    let array = zarrs::array::Array::open(store.clone(), &array_name).expect("open array");
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
