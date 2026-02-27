# zarrs_n5

[N5](https://github.com/saalfeldlab/n5) support for the [zarrs](https://github.com/zarrs) ecosystem.

## Limitations

- Read-only
- No partial chunk reading
- "default" chunk mode (i.e. not varlen or object)
- Currently only supports gzip and bzip2 compression
  - lz4 and xz are described in the core spec
  - blosc is implemented in java but [not well documented](https://github.com/saalfeldlab/n5-blosc/issues/13)
  - zstd is implemented in java but [not well documented](https://github.com/JaneliaSciComp/n5-zstandard/issues/6)
  - jpeg is implemented in java but [not well documented](https://github.com/saalfeldlab/n5-jpeg/issues/1)
- Currently only tests against chunks which perfectly fit the array
  - _should_ work otherwise but I need to find some test data, because zarr v2's N5 implementation pads the end chunks like zarr does

## Prior art

- [constantinpape/z5](https://github.com/constantinpape/z5): C++/Python reader over a common subset of Zarr and N5
- [aschampion/rust-n5](https://github.com/aschampion/rust-n5): rust implementation of N5
- [zarr-developers/zarr-python](https://github.com/zarr-developers/zarr-python/blob/v2.18.7/zarr/n5.py): Zarr v2's N5 support
- [zarr-developers/n5py](https://github.com/zarr-developers/n5py): python plugins for reading N5 through Zarr v3
