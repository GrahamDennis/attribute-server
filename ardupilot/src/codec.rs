use bytes::{Buf, BufMut, BytesMut};
use mavio::protocol::{MavLinkVersion, MavSTX, MaybeVersioned};
use mavio::{Frame, Receiver, Sender};
use std::io::Cursor;
use std::marker::PhantomData;
use tokio_util::codec::{Decoder, Encoder};

pub struct MavlinkCodec<V: MaybeVersioned> {
    phantom_data: PhantomData<V>,
}

impl<V: MaybeVersioned> MavlinkCodec<V> {
    pub fn new() -> MavlinkCodec<V> {
        MavlinkCodec {
            phantom_data: PhantomData,
        }
    }
}

fn find_frame_start<V: MaybeVersioned>(src: &BytesMut) -> Option<(usize, MavLinkVersion)> {
    for (idx, &byte) in src.iter().enumerate() {
        if V::is_magic_byte(byte) {
            return Some((idx, Option::<MavLinkVersion>::from(MavSTX::from(byte))?));
        }
    }

    None
}

impl<V: MaybeVersioned> Decoder for MavlinkCodec<V> {
    type Item = mavio::Frame<V>;
    // FIXME: change error type
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some((frame_start, _mavlink_version)) = find_frame_start::<V>(src) else {
            return Ok(None);
        };
        if frame_start > 0 {
            src.advance(frame_start);
        }
        let cursor = Cursor::new(&*src);
        let mut receiver = Receiver::new::<V>(cursor);
        match receiver.recv() {
            Ok(frame) => {
                let header = frame.header();
                src.advance(header.size() + header.body_length());
                Ok(Some(frame))
            }
            Err(mavio::error::Error::Io(io_error))
                if io_error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                Ok(None)
            }
            Err(error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        }
    }
}

impl<V: MaybeVersioned> Encoder<Frame<V>> for MavlinkCodec<V> {
    // FIXME: change error type
    type Error = std::io::Error;

    fn encode(&mut self, frame: Frame<V>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let mut sender = Sender::new(dst.writer());

        match sender.send(&frame) {
            Ok(_) => Ok(()),
            Err(error) => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                error.to_string(),
            )),
        }
    }
}
