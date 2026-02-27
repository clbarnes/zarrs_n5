#[derive(Debug, Clone)]
pub struct N5ChunkHeader {
    pub(crate) mode: N5ChunkMode,
    /// Column-major, probably?
    pub(crate) shape: Vec<u32>,
}

#[derive(Debug, Clone, Copy)]
#[repr(u16)]
pub(crate) enum N5ChunkMode {
    Default = 0,
    #[allow(unused)]
    VarLen {
        num_el: u32,
    } = 1,
    Object = 2,
}

impl N5ChunkHeader {
    pub(crate) fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        let mut offset: usize = 0;

        let mode_num = u16::from_be_bytes(
            bytes[offset..offset + 2]
                .try_into()
                .map_err(crate::Error::wrap)?,
        );
        offset += 2;
        let ndim = u16::from_be_bytes(
            bytes[offset..offset + 2]
                .try_into()
                .map_err(crate::Error::wrap)?,
        );
        offset += 2;
        let mut shape = Vec::with_capacity(ndim as usize);
        for _ in 0..ndim {
            shape.push(u32::from_be_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(crate::Error::wrap)?,
            ));
            offset += 4;
        }

        let mode = match mode_num {
            0 => N5ChunkMode::Default,
            1 => {
                let num_el = u32::from_be_bytes(
                    bytes[offset..offset + 4]
                        .try_into()
                        .map_err(crate::Error::wrap)?,
                );
                N5ChunkMode::VarLen { num_el }
            }
            2 => N5ChunkMode::Object,
            n => return Err(crate::Error::general(format!("invalid N5 chunk mode {n}"))),
        };
        Ok(N5ChunkHeader { mode, shape })
    }

    pub(crate) fn data_offset(&self) -> usize {
        size_of::<u16>()  // mode discriminator
            + size_of::<u16>() // ndim
            + self.shape.len() * size_of::<u32>()  // shape
            + match self.mode {
                N5ChunkMode::VarLen { .. } => size_of::<u32>(),
                _ => 0,
            }
    }
}
