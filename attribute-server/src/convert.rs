use attribute_grpc_api::grpc;
use attribute_store::store::{AttributeValue, Entity, EntityId, EntityLocator, Symbol};
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use prost::Message;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConversionError {
    #[error("field missing")]
    FieldMissing,
    #[error("error converting field `{field}`")]
    ErrorConvertingField {
        field: &'static str,
        source: Box<ConversionError>,
    },
    #[error("error decoding entity id")]
    InvalidEntityId(#[source] anyhow::Error),
    #[error("invalid symbol")]
    InvalidSymbol(#[source] anyhow::Error),
}

type ConversionResult<T> = Result<T, ConversionError>;

pub trait TryFromProto<T>: Sized {
    fn try_from_proto(value: T) -> ConversionResult<Self>;
}

pub trait IntoProto<T>: Sized {
    fn into_proto(self: Self) -> T;
}

impl TryFromProto<grpc::GetEntityRequest> for EntityLocator {
    fn try_from_proto(value: grpc::GetEntityRequest) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .entity_locator
            .ok_or(FieldMissing)
            .and_then(|loc| EntityLocator::try_from_proto(loc))
            .map_err(|err| ErrorConvertingField {
                field: "entity_locator",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::EntityLocator> for EntityLocator {
    fn try_from_proto(value: grpc::EntityLocator) -> ConversionResult<Self> {
        use ConversionError::*;

        value
            .locator
            .ok_or(FieldMissing)
            .and_then(|loc| EntityLocator::try_from_proto(loc))
            .map_err(|err| ErrorConvertingField {
                field: "locator",
                source: err.into(),
            })
    }
}

impl TryFromProto<grpc::entity_locator::Locator> for EntityLocator {
    fn try_from_proto(value: grpc::entity_locator::Locator) -> ConversionResult<Self> {
        use grpc::entity_locator::Locator;

        match value {
            Locator::EntityId(entity_id) => {
                EntityId::try_from_proto(entity_id).map(EntityLocator::EntityId)
            }
            Locator::Symbol(symbol) => Symbol::try_from_proto(symbol).map(EntityLocator::Symbol),
        }
    }
}

impl TryFromProto<String> for EntityId {
    fn try_from_proto(value: String) -> ConversionResult<Self> {
        use ConversionError::*;

        let decoded_bytes = URL_SAFE
            .decode(&value)
            .map_err(|err| InvalidEntityId(err.into()))?;
        let internal_entity_id = grpc::InternalEntityId::decode(&*decoded_bytes)
            .map_err(|err| InvalidEntityId(err.into()))?;
        let entity_id: EntityId = internal_entity_id.database_id.into();
        Ok(entity_id)
    }
}

impl TryFromProto<String> for Symbol {
    fn try_from_proto(value: String) -> ConversionResult<Self> {
        use ConversionError::*;

        Symbol::try_from(value).map_err(|err| InvalidSymbol(err.into()))
    }
}

impl IntoProto<grpc::Entity> for Entity {
    fn into_proto(self: Self) -> grpc::Entity {
        grpc::Entity {
            entity_id: self.entity_id.into_proto(),
            attributes: self.attributes.clone().into_proto(),
            attributes_map: self.attributes.into_proto(),
        }
    }
}

impl IntoProto<String> for EntityId {
    fn into_proto(self: Self) -> String {
        let EntityId(database_id) = self;
        let internal_entity_id = grpc::InternalEntityId { database_id };
        URL_SAFE.encode(internal_entity_id.encode_to_vec())
    }
}

impl IntoProto<Vec<grpc::Attribute>> for HashMap<Symbol, AttributeValue> {
    fn into_proto(self: Self) -> Vec<grpc::Attribute> {
        self.into_iter()
            .map(|(symbol, attribute_value)| grpc::Attribute {
                attribute_symbol: symbol.into(),
                attribute_value: Some(attribute_value.into_proto()),
            })
            .collect()
    }
}

impl IntoProto<HashMap<String, grpc::AttributeValue>> for HashMap<Symbol, AttributeValue> {
    fn into_proto(self: Self) -> HashMap<String, grpc::AttributeValue> {
        self.into_iter()
            .map(|(symbol, attribute_value)| (symbol.into(), attribute_value.into_proto()))
            .collect()
    }
}

impl IntoProto<grpc::AttributeValue> for AttributeValue {
    fn into_proto(self: Self) -> grpc::AttributeValue {
        grpc::AttributeValue {
            attribute_value: Some(self.into_proto()),
        }
    }
}

impl IntoProto<grpc::attribute_value::AttributeValue> for AttributeValue {
    fn into_proto(self: Self) -> grpc::attribute_value::AttributeValue {
        match self {
            AttributeValue::String(string_value) => {
                grpc::attribute_value::AttributeValue::StringValue(string_value)
            }
            AttributeValue::EntityId(entity_id) => {
                grpc::attribute_value::AttributeValue::EntityIdValue(entity_id.into_proto())
            }
            AttributeValue::Bytes(bytes) => {
                grpc::attribute_value::AttributeValue::BytesValue(bytes)
            }
        }
    }
}
