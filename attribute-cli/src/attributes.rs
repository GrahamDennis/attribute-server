use crate::mavlink::AttributeTypes;
use crate::pb;
use crate::pb::attribute_store_client::AttributeStoreClient;
use crate::pb::{AttributeType, AttributeValue, CreateAttributeTypeRequest, EntityLocator, UpdateEntityRequest, ValueType};
use prost_reflect::{DescriptorPool, MessageDescriptor, ReflectMessage};
use tonic::transport::Channel;

pub trait TypedAttribute {
    fn attribute_name() -> &'static str;
    fn as_bytes(&self) -> Vec<u8>;
}

impl AttributeStoreClient<Channel> {
    pub async fn upload_protobuf_message_specs(&mut self, file_descriptor_set_bytes: &[u8]) -> anyhow::Result<()> {
        let descriptor_pool =
            DescriptorPool::decode(file_descriptor_set_bytes)?;
        let attribute_type_extension = descriptor_pool.get_extension_by_name("me.grahamdennis.attribute.attribute_type_options").unwrap();

        let message_descriptor_is_attribute_type = |message_descriptor: MessageDescriptor| -> anyhow::Result<Option<MessageDescriptor>> {
            let options = message_descriptor.options();
            let extension = options.get_extension(&attribute_type_extension);
            let Some(extension) = extension.as_message() else {
                return Ok(None);
            };
            let attribute_type_options = extension.transcode_to::<pb::AttributeTypeOptions>()?;
            Ok(if attribute_type_options.create_attribute_type {
                Some(message_descriptor)
            } else {
                None
            })
        };

        for file_descriptor in descriptor_pool.files() {
            let attribute_message_descriptors: Vec<_> = file_descriptor.messages()
                .map(message_descriptor_is_attribute_type)
                .filter_map(|result| result.transpose())
                .collect::<Result<_, _>>()?;
            
            if attribute_message_descriptors.is_empty() {
                continue;
            }
            let message_full_names: Vec<_> = attribute_message_descriptors.iter().map(MessageDescriptor::full_name).collect();
            tracing::info!(file_descriptor=file_descriptor.package_name(), messages=?message_full_names, "Uploading file descriptor");
            let create_fdset_request = UpdateEntityRequest {
                entity_locator: Some(EntityLocator::from_symbol(
                    file_descriptor.package_name(),
                )),
                attributes_to_update: vec![
                    pb::AttributeToUpdate {
                        attribute_type: "@symbolName".to_string(),
                        attribute_value: Some(AttributeValue::from_string(
                            file_descriptor.package_name(),
                        )),
                    },
                    pb::AttributeToUpdate {
                        attribute_type: AttributeTypes::FileDescriptorSet.as_str().to_string(),
                        attribute_value: Some(AttributeValue::from_bytes(
                            file_descriptor_set_bytes.to_vec(),
                        )),
                    },
                ],
            };
            let fdset_response = self
                .update_entity(create_fdset_request)
                .await?
                .into_inner();
            let fdset_entity = fdset_response
                .entity
                .ok_or(anyhow::format_err!("Failed to create mavlink fdset entity"))?;
            

            let filtered_messages: Vec<_> = file_descriptor.messages()
                .map(message_descriptor_is_attribute_type)
                .filter_map(|result| result.transpose())
                .collect::<Result<_, _>>()?;
            
            for message_descriptor in filtered_messages {
                self.update_protobuf_attribute_type_v2(&fdset_entity.entity_id, &message_descriptor).await?;
            }
        }
        
        Ok(())
    }

    pub async fn update_protobuf_attribute_type_v2(
        &mut self,
        file_descriptor_entity_id: &str,
        message_descriptor: &MessageDescriptor,
    ) -> Result<tonic::Response<pb::UpdateEntityResponse>, tonic::Status> {
        let symbol_name = message_descriptor.full_name();
        
        let create_attribute_type_request =         CreateAttributeTypeRequest {
            attribute_type: Some(AttributeType {
                symbol: symbol_name.to_string(),
                value_type: ValueType::Bytes.into(),
            }),
        };
        let create_attribute_result = self.create_attribute_type(create_attribute_type_request).await;
        match create_attribute_result {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::AlreadyExists => {
                tracing::debug!("skipping attribute because it already exists");
            }
            Err(status) => {
                return Err(status)?;
            }
        }

        let update_entity_request = UpdateEntityRequest {
            entity_locator: Some(EntityLocator::from_symbol(symbol_name)),
            attributes_to_update: vec![
                pb::AttributeToUpdate {
                    attribute_type: "@symbolName".to_string(),
                    attribute_value: Some(AttributeValue::from_string(symbol_name)),
                },
                pb::AttributeToUpdate {
                    attribute_type: AttributeTypes::MessageName.as_str().to_string(),
                    attribute_value: Some(AttributeValue::from_string(
                        symbol_name,
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
        self.update_entity(update_entity_request).await
    }

    pub async fn update_protobuf_attribute_type<T: TypedAttribute + ReflectMessage + Default>(
        &mut self,
        file_descriptor_entity_id: &str,
    ) -> Result<tonic::Response<pb::UpdateEntityResponse>, tonic::Status> {
        let update_entity_request = UpdateEntityRequest {
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
        self.update_entity(update_entity_request).await
    }

    pub async fn simple_update_entity<T: TypedAttribute>(
        &mut self,
        symbol_id: &str,
        // FIXME: This should take a tuple of N different TypedAttributes
        value: T,
    ) -> Result<tonic::Response<pb::UpdateEntityResponse>, tonic::Status> {
        self.update_entity(pb::UpdateEntityRequest {
            entity_locator: Some(EntityLocator::from_symbol(symbol_id)),
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
