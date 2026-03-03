# zarrs_n5

[N5](https://github.com/saalfeldlab/n5) support for the [zarrs](https://github.com/zarrs) ecosystem.

## Limitations

- Read-only
- No partial chunk reading
- "default" chunk mode (i.e. not varlen or object)
- Compression support:
  - N5 core
    - [x] gzip
    - [x] bzip2
    - [ ] lz4
    - [ ] xz
  - N5 extensions
    - [x] zstd <https://github.com/JaneliaSciComp/n5-zstandard>
    - [x] blosc <https://github.com/saalfeldlab/n5-blosc>
    - [ ] jpeg is implemented in java but [not well documented](https://github.com/saalfeldlab/n5-jpeg/issues/1)
      - PRs welcome but I'm unlikely to prioritise this unless a [Zarr JPEG codec were stabilised](https://github.com/zarr-developers/zarr-extensions/issues/15)
- The handling of edge chunks is quite inefficient
- Zarr groups must have metadata documents, but N5 groups may not.
  This may lead to unexpected behaviour when discovering hierarchy structure.
  This library allows inferring a group with empty attributes when a metadata document is missing.
- N5 hierarchies have a root (metadata contains the `"n5"` key), but Zarr hierarchies do not; in practice this doesn't matter much

## Prior art

- [constantinpape/z5](https://github.com/constantinpape/z5): C++/Python reader over a common subset of Zarr and N5
- [aschampion/rust-n5](https://github.com/aschampion/rust-n5): rust implementation of N5
- [zarr-developers/zarr-python](https://github.com/zarr-developers/zarr-python/blob/v2.18.7/zarr/n5.py): Zarr v2's N5 support
- [zarr-developers/n5py](https://github.com/zarr-developers/n5py): python plugins for reading N5 through Zarr v3
