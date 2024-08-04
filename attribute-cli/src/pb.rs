use crate::pb;

pub const FILE_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("file_descriptor_set.attribute");

tonic::include_proto!("me.grahamdennis.attribute");

impl EntityRow {
    fn attribute_value(&self, idx: usize) -> Option<&attribute_value::AttributeValue> {
        self.values
            .get(idx)?
            .value
            .as_ref()?
            .attribute_value
            .as_ref()
    }

    pub fn string_value(&self, idx: usize) -> Option<&String> {
        match self.attribute_value(idx)? {
            attribute_value::AttributeValue::StringValue(value) => Some(value),
            _ => None,
        }
    }

    pub fn entity_id_value(&self, idx: usize) -> Option<&String> {
        match self.attribute_value(idx)? {
            attribute_value::AttributeValue::EntityIdValue(entity_id) => Some(entity_id),
            _ => None,
        }
    }

    pub fn bytes_value(&self, idx: usize) -> Option<&Vec<u8>> {
        match self.attribute_value(idx)? {
            attribute_value::AttributeValue::BytesValue(value) => Some(value),
            _ => None,
        }
    }
}

pub mod mavlink {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("file_descriptor_set.mavlink");

    tonic::include_proto!("me.grahamdennis.attribute.mavlink");
}
