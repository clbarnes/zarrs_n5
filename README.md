# zarrs_n5

[N5](https://github.com/saalfeldlab/n5) support for the [zarrs](https://github.com/zarrs) ecosystem.

## Limitations

- Read-only
- Sync-only
- No partial chunk reading
- "default" chunk mode (i.e. not varlen or object)
- Only supports gzip and bzip2 compression

## Prior art

- [constantinpape/z5](https://github.com/constantinpape/z5): C++/Python reader over a common subset of Zarr and N5
- [aschampion/rust-n5](https://github.com/aschampion/rust-n5): rust implementation of N5
- [zarr-developers/n5py](https://github.com/zarr-developers/n5py): python plugins for reading N5 through Zarr v3

Zarr v2 also had some native N5 support.
