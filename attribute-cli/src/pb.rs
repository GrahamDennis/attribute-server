pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("file_descriptor_set.attribute");

tonic::include_proto!("me.grahamdennis.attribute");

pub mod mavlink {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("file_descriptor_set.mavlink");

    tonic::include_proto!("me.grahamdennis.attribute.mavlink");
}
