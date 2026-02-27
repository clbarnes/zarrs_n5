#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "numpy",
#     "zarr>=2.18,<3.0",
# ]
# ///
from pathlib import Path
import shutil
import warnings
import numpy as np

# https://zarr.readthedocs.io/en/v2.18.5/
import zarr

warnings.simplefilter("ignore", category=FutureWarning)
here = Path(__file__).parent.resolve()


def create_im(shape=(256, 128), dtype=np.float32):
    def fn(i, j):
        return i**2 + j**2

    arr = np.fromfunction(fn, shape=shape, dtype=dtype)
    rooted = np.sqrt(arr)
    normed = rooted / rooted.max()
    return normed


class N5Writer:
    def __init__(self, im: np.ndarray, force=False, root: Path = here) -> None:
        self.im = im
        self.force = force
        self.root = root

    def write(
        self,
        name: str,
        chunks: tuple[int, ...] | None = None,
        compressor=None,
        desc: str | None = None,
    ):
        if chunks is None:
            chunks = self.im.shape

        dpath = self.root / f"{name}.n5"
        if dpath.exists():  # noqa: F821
            if self.force:
                shutil.rmtree(dpath)
            else:
                raise FileExistsError(f"{dpath} already exists")

        store = zarr.N5Store(str(dpath))
        z = zarr.array(self.im, chunks=chunks, store=store, compressor=compressor)
        if desc is not None:
            z.attrs["description"] = desc

    def write_npy(self):
        dpath = self.root / "raw.npy"
        if dpath.exists():
            if self.force:
                dpath.unlink()
            else:
                raise FileExistsError(f"{dpath} already exists")

        np.save(dpath, self.im)

    def write_all(self):
        self.write_npy()
        self.single_chunk()
        self.even_chunk()
        self.gzip()
        self.bz2()

    def single_chunk(self):
        self.write("single_chunk", desc="Single-chunk array")

    def even_chunk(self):
        self.write(
            "even_chunk",
            chunks=tuple(s // 2 for s in self.im.shape),
            desc="Evenly-chunked array",
        )

    def gzip_uneven(self):
        self.write(
            "gzip_uneven",
            chunks=tuple(s // 2 + s // 4 for s in self.im.shape),
            compressor=zarr.GZip(level=6),
            desc="GZip-compressed, unevenly-chunked array",
        )

    def uneven_chunk(self):
        # NOTE zarr pads the last chunks, so this isn't a good test
        self.write(
            "uneven_chunk",
            chunks=tuple(s // 2 + s // 4 for s in self.im.shape),
            desc="Unevenly-chunked array",
        )

    def gzip(self):
        self.write(
            "gzip",
            compressor=zarr.GZip(level=6),
            desc="GZip-compressed array",
        )

    def bz2(self):
        self.write(
            "bz2",
            compressor=zarr.BZ2(level=6),
            desc="BZ2-compressed array",
        )


def main():
    im = create_im()
    writer = N5Writer(im, True)
    writer.write_all()


if __name__ == "__main__":
    main()
