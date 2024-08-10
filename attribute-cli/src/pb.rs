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

impl EntityLocator {
    pub fn from_symbol(symbol: impl ToString) -> Self {
        Self {
            locator: Some(entity_locator::Locator::Symbol(symbol.to_string())),
        }
    }
    pub fn from_entity_id(entity_id: impl ToString) -> Self {
        Self {
            locator: Some(entity_locator::Locator::EntityId(entity_id.to_string())),
        }
    }
}

impl AttributeValue {
    pub fn from_string(value: impl ToString) -> Self {
        Self {
            attribute_value: Some(attribute_value::AttributeValue::StringValue(
                value.to_string(),
            )),
        }
    }

    pub fn from_entity_id(value: impl ToString) -> Self {
        Self {
            attribute_value: Some(attribute_value::AttributeValue::EntityIdValue(
                value.to_string(),
            )),
        }
    }
}

pub mod mavlink {
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("file_descriptor_set.mavlink");

    tonic::include_proto!("me.grahamdennis.attribute.mavlink");
}
