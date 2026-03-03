#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "numpy",
#     "tensorstore",
# ]
# ///
from pathlib import Path
import shutil
import numpy as np
import tensorstore as ts

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
        compressor: str | None = None,
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

        metadata = {
            "dimensions": self.im.shape,
            "dataType": str(self.im.dtype),
        }
        if chunks is None:
            metadata["blockSize"] = self.im.shape
        else:
            metadata["blockSize"] = chunks

        if compressor is None:
            metadata["compression"] = {"type": "raw"}
        else:
            metadata["compression"] = {"type": compressor}
            if compressor == "blosc":
                metadata["compression"]["cname"] = "blosclz"
                metadata["compression"]["clevel"] = 6  # type: ignore
                metadata["compression"]["shuffle"] = 0  # type: ignore

        dataset = ts.open(
            {
                "driver": "n5",
                "kvstore": {
                    "driver": "file",
                    "path": str(dpath),
                },
                "metadata": metadata,
                "create": True,
            }
        ).result()
        dataset.write(self.im).result()

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
        self.zstd()
        self.gzip()
        self.bz2()
        self.blosc()
        self.uneven_chunk_padded()

    def single_chunk(self):
        self.write("single_chunk", desc="Single-chunk array")

    def zstd(self):
        self.write(
            "zstd",
            compressor="zstd",
            desc="ZStd-compressed array",
        )

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
            compressor="gzip",
            desc="GZip-compressed, unevenly-chunked array",
        )

    def uneven_chunk_padded(self):
        self.write(
            "uneven_chunk_padded",
            chunks=tuple(s // 2 + s // 4 for s in self.im.shape),
            desc="Unevenly-chunked array",
        )

    def gzip(self):
        self.write(
            "gzip",
            compressor="gzip",
            desc="GZip-compressed array",
        )

    def blosc(self):
        self.write(
            "blosc",
            compressor="blosc",
            desc="Blosc-compressed array",
        )

    def bz2(self):
        self.write(
            "bz2",
            compressor="bzip2",
            desc="BZ2-compressed array",
        )


def main():
    im = create_im()
    writer = N5Writer(im, True)
    writer.write_all()


if __name__ == "__main__":
    main()
