use std::borrow::Cow;
use std::num::NonZeroU64;
use std::ops::{Deref, Range};
use zarrs::array::codec::api::{
    ArrayBytes, ArrayBytesOffsets, ArrayBytesVariableLength, CodecError,
};
use zarrs::array::{DataType, FillValue};

mod default;
pub use default::{N5DefaultCodec, N5DefaultCodecConfiguration};

// TODO
// ?lz4
// ?xz

struct ShapeRectifier<'a> {
    array_bytes: ArrayBytes<'a>,
    shape: &'a [NonZeroU64],
    data_type: &'a DataType,
    fill_value: &'a FillValue,
    desired_shape: &'a [NonZeroU64],
}

impl<'a> ShapeRectifier<'a> {
    fn new_unchecked(
        array_bytes: ArrayBytes<'a>,
        shape: &'a [NonZeroU64],
        data_type: &'a DataType,
        fill_value: &'a FillValue,
        desired_shape: &'a [NonZeroU64],
    ) -> Self {
        Self {
            array_bytes,
            shape,
            data_type,
            fill_value,
            desired_shape,
        }
    }

    fn rectify(self) -> Result<ArrayBytes<'static>, CodecError> {
        if self.shape == self.desired_shape {
            return Ok(self.array_bytes.into_owned());
        }
        match &self.array_bytes {
            ArrayBytes::Fixed(cow) => {
                let Some(width) = self.data_type.fixed_size() else {
                    return Err(CodecError::Other(format!(
                        "array bytes have a fixed width but {} does not",
                        self.data_type
                    )));
                };
                self.handle_fixed(cow, width)
            }
            ArrayBytes::Variable(abvl) => {
                // This is unreachable for now but is along the right lines if implemented in future.
                let offsets = abvl.offsets().deref();
                let bytes = abvl.bytes().deref();
                self.handle_variable(bytes, offsets)
            }
            ArrayBytes::Optional(_) => {
                unreachable!("optional data should not be present in N5 blocks")
            }
        }
    }

    fn handle_fixed(
        &self,
        bytes: &[u8],
        type_width: usize,
    ) -> Result<ArrayBytes<'static>, CodecError> {
        let desired_numel: usize = self
            .desired_shape
            .iter()
            .map(|n| n.get() as usize)
            .product();
        let mut out = Vec::with_capacity(desired_numel * type_width);
        let mut out_idx_iter = IdxIter::new(self.desired_shape);
        let raveller = Raveller::new(self.shape);
        while out_idx_iter.incr() {
            let current = out_idx_iter.current().expect("just checked incr");
            let Some(idx) = raveller.linearize(current) else {
                out.extend_from_slice(self.fill_value.as_ne_bytes());
                continue;
            };
            let start = idx * type_width;
            let end = start + type_width;
            out.extend_from_slice(&bytes[start..end]);
        }
        Ok(ArrayBytes::Fixed(Cow::Owned(out)))
    }

    /// This is unreachable for now but is along the right lines if implemented in future.
    fn handle_variable(
        &self,
        bytes: &[u8],
        offsets: &[usize],
    ) -> Result<ArrayBytes<'static>, CodecError> {
        let desired_numel: usize = self
            .desired_shape
            .iter()
            .map(|n| n.get() as usize)
            .product();
        let mut out_offsets = Vec::with_capacity(desired_numel + 1);
        out_offsets.push(0usize);

        let mut out = Vec::new();
        let mut out_idx_iter = IdxIter::new(self.desired_shape);
        let raveller = Raveller::new(self.shape);
        while out_idx_iter.incr() {
            let current = out_idx_iter.current().expect("just checked incr");
            let Some(idx) = raveller.linearize(current) else {
                out.extend_from_slice(self.fill_value.as_ne_bytes());
                out_offsets.push(out.len());

                continue;
            };

            let start = offsets[idx];
            let end = offsets[idx + 1];
            out.extend_from_slice(&bytes[start..end]);
            out_offsets.push(out.len());
        }
        Ok(ArrayBytes::Variable(ArrayBytesVariableLength::new(
            out,
            ArrayBytesOffsets::new(out_offsets)?,
        )?))
    }
}

struct IdxIter<'a> {
    max_shape: &'a [NonZeroU64],
    current: Vec<usize>,
    started: bool,
    finished: bool,
}

impl<'a> IdxIter<'a> {
    pub fn new(max_shape: &'a [NonZeroU64]) -> Self {
        let current = vec![0; max_shape.len()];
        Self {
            max_shape,
            current,
            started: false,
            finished: false,
        }
    }

    /// Returns whether there is a new item to show.
    pub fn incr(&mut self) -> bool {
        if self.finished {
            return false;
        }

        if !self.started {
            self.started = true;
            return true;
        }

        for idx in (0..self.current.len()).rev() {
            self.current[idx] += 1;
            if (self.current[idx] as u64) < self.max_shape[idx].get() {
                break;
            } else if idx == 0 {
                self.finished = true;
                return false;
            } else {
                self.current[idx] = 0;
            }
        }
        true
    }

    /// Get the current value
    pub fn current(&self) -> Option<&[usize]> {
        if !self.started || self.finished {
            None
        } else {
            Some(&self.current)
        }
    }
}

struct Raveller<'a> {
    shape: &'a [NonZeroU64],
}

impl<'a> Raveller<'a> {
    pub fn new(shape: &'a [NonZeroU64]) -> Self {
        Self { shape }
    }
}

impl<'a> Raveller<'a> {
    /// Panics if `idx.len() != self.shape.len()`
    fn linearize(&self, idx: &[usize]) -> Option<usize> {
        assert!(idx.len() == self.shape.len());
        let mut out = 0;
        let mut stride = 1;
        for (i, s) in idx.iter().zip(self.shape.iter()).rev() {
            if *i as u64 >= s.get() {
                return None;
            }
            out += i * stride;
            stride *= s.get() as usize;
        }
        Some(out)
    }

    #[allow(unused)]
    /// Get the linear indices of the given row.
    /// The row is specified by all but the last dimension (i.e. C contiguous).
    ///
    /// Panics if `idx.len() != self.shape.len() - 1`.
    fn linearize_row(&self, idx: &[usize]) -> Option<Range<usize>> {
        assert!(idx.len() == self.shape.len() - 1);
        let mut start = 0;
        let end = self.shape.len() - 1;
        let last_shape = self.shape[end].get() as usize;
        let mut stride = last_shape;
        for (i, s) in idx.iter().zip(self.shape[..end].iter()).rev() {
            if *i as u64 >= s.get() {
                return None;
            }
            start += i * stride;
            stride *= s.get() as usize;
        }
        Some(start..start + last_shape)
    }

    #[allow(unused)]
    fn row_len(&self) -> usize {
        self.shape.last().unwrap().get() as usize
    }
}
