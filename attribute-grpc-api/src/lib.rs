pub mod pb {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("file_descriptor_set");

    tonic::include_proto!("me.grahamdennis.attribute");
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn can_encode_ping_request() {
        let ping = pb::PingRequest {};
        let encoded = ping.encode_to_vec();
        assert_eq!(encoded, vec![]);
    }

    #[test]
    fn can_decode_ping_request() {
        let bytes: &'static [u8] = &[];
        let ping = pb::PingRequest::decode(bytes).unwrap();
        assert_eq!(ping, pb::PingRequest {});
    }
}
