use crate::mavlink::AttributeTypes;
use crate::pb;
use crate::pb::attribute_store_client::AttributeStoreClient;
use crate::pb::{AttributeValue, EntityLocator, UpdateEntityRequest};
use prost_reflect::ReflectMessage;
use tonic::transport::Channel;

pub trait TypedAttribute {
    fn attribute_name() -> &'static str;
    fn as_bytes(&self) -> Vec<u8>;
}

impl AttributeStoreClient<Channel> {
    pub async fn update_protobuf_attribute_type<T: TypedAttribute + ReflectMessage + Default>(
        &mut self,
        file_descriptor_entity_id: &str,
    ) -> Result<tonic::Response<pb::UpdateEntityResponse>, tonic::Status> {
        let create_global_position_request = UpdateEntityRequest {
            entity_locator: Some(EntityLocator::from_symbol(T::attribute_name())),
            attributes_to_update: vec![
                pb::AttributeToUpdate {
                    attribute_type: "@symbolName".to_string(),
                    attribute_value: Some(AttributeValue::from_string(T::attribute_name())),
                },
                pb::AttributeToUpdate {
                    attribute_type: AttributeTypes::MessageName.as_str().to_string(),
                    attribute_value: Some(AttributeValue::from_string(
                        T::default().descriptor().full_name(),
                    )),
                },
                pb::AttributeToUpdate {
                    attribute_type: AttributeTypes::FileDescriptorSetRef.as_str().to_string(),
                    attribute_value: Some(AttributeValue::from_entity_id(
                        file_descriptor_entity_id,
                    )),
                },
            ],
        };
        self.update_entity(create_global_position_request).await
    }

    pub async fn simple_update_entity<T: TypedAttribute>(
        &mut self,
        symbol_id: &str,
        // FIXME: This should take a tuple of N different TypedAttributes
        value: T,
    ) -> Result<tonic::Response<pb::UpdateEntityResponse>, tonic::Status> {
        let attribute_name = T::attribute_name();
        self.update_entity(pb::UpdateEntityRequest {
            entity_locator: Some(EntityLocator::from_symbol(attribute_name)),
            attributes_to_update: vec![
                pb::AttributeToUpdate {
                    attribute_type: "@symbolName".to_string(),
                    attribute_value: Some(AttributeValue::from_string(symbol_id)),
                },
                pb::AttributeToUpdate {
                    attribute_type: T::attribute_name().to_string(),
                    attribute_value: Some(AttributeValue::from_bytes(value.as_bytes())),
                },
            ],
        })
        .await
    }
}
