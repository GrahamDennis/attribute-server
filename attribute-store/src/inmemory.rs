use crate::store::{
    AttributeStore, AttributeStoreError, AttributeTypes, AttributeValue, BootstrapSymbol,
    CreateAttributeTypeRequest, Entity, EntityId, EntityLocator, EntityQuery, EntityRow, Symbol,
    UpdateEntityRequest, ValueType,
};
use async_trait::async_trait;
use garde::Unvalidated;
use parking_lot::{Mutex, MutexGuard};
use std::collections::HashMap;
use tracing::Level;

#[derive(Debug)]
pub struct InMemoryAttributeStore {
    attribute_types: Mutex<AttributeTypes>,
    entities: Mutex<Vec<Entity>>,
}

impl InMemoryAttributeStore {
    pub fn new() -> Self {
        let entities: Vec<Entity> = Self::bootstrap_entities();

        for (idx, entity) in entities.iter().enumerate() {
            let EntityId(database_id) = entity.entity_id;
            assert_eq!(usize::try_from(database_id).unwrap(), idx);
        }

        let value_type_symbol: Symbol = BootstrapSymbol::ValueType.into();
        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();

        let attribute_types = entities
            .iter()
            .filter(|entity| entity.attributes.get(&value_type_symbol).is_some())
            .map(|entity| {
                match (
                    entity.attributes.get(&symbol_name_symbol),
                    entity.attributes.get(&value_type_symbol),
                ) {
                    (
                        Some(AttributeValue::String(symbol_name)),
                        Some(AttributeValue::EntityId(value_type_entity_id)),
                    ) => (
                        Symbol::try_from(symbol_name.clone()).ok(),
                        ValueType::try_from(*value_type_entity_id).ok(),
                    ),
                    _ => (None, None),
                }
            })
            .flat_map(|entry| match entry {
                (Some(key), Some(value)) => Some((key, value)),
                _ => None,
            })
            .collect();
        InMemoryAttributeStore {
            attribute_types: Mutex::new(attribute_types),
            entities: Mutex::new(entities),
        }
    }

    #[inline]
    fn all_locks(&self) -> (MutexGuard<AttributeTypes>, MutexGuard<Vec<Entity>>) {
        (self.attribute_types.lock(), self.entities.lock())
    }

    fn bootstrap_entities() -> Vec<Entity> {
        vec![
            BootstrapSymbol::EntityId.into(),
            BootstrapSymbol::SymbolName.into(),
            BootstrapSymbol::ValueType.into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::Text).into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::EntityReference).into(),
            BootstrapSymbol::ValueTypeEnum(ValueType::Bytes).into(),
        ]
    }
}

fn insert_new_entity_with_attributes(
    entities: &mut Vec<Entity>,
    attributes: HashMap<Symbol, AttributeValue>,
) -> Result<Entity, AttributeStoreError> {
    use AttributeStoreError::*;

    let database_id = entities.len();
    let entity = Entity {
        entity_id: EntityId(i64::try_from(database_id).map_err(|err| Other {
            message: format!(
                "Failed to convert database id `{database_id}` to EntityId due to error `{err:?}`"
            ),
            source: err.into(),
        })?),
        attributes,
    };

    entities.push(entity.clone());

    Ok(entity)
}

#[async_trait]
impl AttributeStore for InMemoryAttributeStore {
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn create_attribute_type(
        &self,
        create_attribute_type_request: &CreateAttributeTypeRequest,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreError::*;

        log::trace!("Received create_attribute_type request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
        let (mut locked_attribute_types, mut locked_entities) = self.all_locks();

        // validate
        let validated_request = Unvalidated::new(create_attribute_type_request)
            .validate_with(&locked_attribute_types)?;
        let CreateAttributeTypeRequest { attribute_type } = validated_request.into_inner();

        let database_id = locked_entities.len();
        let entity = Entity {
            entity_id: EntityId(i64::try_from(database_id)
                .map_err(|err| Other {
                    message: format!("Failed to convert database id `{database_id}` to EntityId due to error `{err:?}`"),
                    source: err.into()
                }
                )?
            ),
            attributes: HashMap::from([
                (symbol_name_symbol, AttributeValue::String(attribute_type.symbol.to_string())),
                (BootstrapSymbol::ValueType.into(), AttributeValue::EntityId(attribute_type.value_type.into()))
            ])
        };

        locked_entities.push(entity.clone());
        locked_attribute_types.insert(attribute_type.symbol.clone(), attribute_type.value_type);

        Ok(entity)
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn get_entity(
        &self,
        entity_locator: &EntityLocator,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreError::*;

        log::trace!("Received get_entity request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
        let locked_entities = self.entities.lock();
        let entity = match entity_locator {
            EntityLocator::EntityId(entity_id) => locked_entities.get(usize::try_from(*entity_id)?),
            EntityLocator::Symbol(symbol) => {
                let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                locked_entities.iter().find(|entity| {
                    entity
                        .attributes
                        .get(&symbol_name_symbol)
                        .is_some_and(|attribute_value| {
                            attribute_value.eq(&expected_attribute_value)
                        })
                })
            }
        }
        .ok_or_else(|| EntityNotFound(entity_locator.clone()))?;

        Ok(entity.clone())
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError> {
        log::trace!("Received query_entities request");

        let (locked_attribute_types, locked_entities) = self.all_locks();

        // validate
        let validated_entity_query =
            Unvalidated::new(entity_query).validate_with(&locked_attribute_types)?;
        let EntityQuery {
            root,
            attribute_types,
        } = validated_entity_query.into_inner();

        let entity_rows = locked_entities
            .iter()
            .filter(root.to_predicate())
            .map(|entity| entity.to_entity_row(attribute_types))
            .collect();

        Ok(entity_rows)
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn update_entity(
        &self,
        update_entity_request: &UpdateEntityRequest,
    ) -> Result<Entity, AttributeStoreError> {
        log::trace!("Received query_entities request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
        let (locked_attribute_types, mut locked_entities) = self.all_locks();

        // Validate
        let validated_update_entity_request =
            Unvalidated::from(update_entity_request).validate_with(&locked_attribute_types)?;
        let UpdateEntityRequest {
            entity_locator,
            attributes_to_update,
        } = validated_update_entity_request.into_inner();

        // Update entity
        let existing_entity = match entity_locator {
            EntityLocator::EntityId(entity_id) => {
                locked_entities.get_mut(usize::try_from(*entity_id)?)
            }
            EntityLocator::Symbol(symbol) => {
                let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                locked_entities.iter_mut().find(|entity| {
                    entity
                        .attributes
                        .get(&symbol_name_symbol)
                        .is_some_and(|attribute_value| {
                            attribute_value.eq(&expected_attribute_value)
                        })
                })
            }
        };

        let updated_entity = match existing_entity {
            None => insert_new_entity_with_attributes(
                &mut locked_entities,
                update_entity_request
                    .attributes_to_update
                    .iter()
                    .filter_map(|attribute_to_update| {
                        attribute_to_update
                            .value
                            .clone()
                            .map(|value| (attribute_to_update.symbol.clone(), value))
                    })
                    .collect(),
            )?,
            Some(entity) => {
                for attribute_to_update in attributes_to_update {
                    match &attribute_to_update.value {
                        None => entity.attributes.remove(&attribute_to_update.symbol),
                        Some(attribute_value) => entity
                            .attributes
                            .insert(attribute_to_update.symbol.clone(), attribute_value.clone()),
                    };
                }
                entity.clone()
            }
        };

        Ok(updated_entity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EntityQueryNode, MatchAllQueryNode};

    #[tokio::test]
    async fn can_fetch_by_entity_id() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::EntityId(BootstrapSymbol::EntityId.into()))
            .await
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[tokio::test]
    async fn can_fetch_by_symbol() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::Symbol(BootstrapSymbol::EntityId.into()))
            .await
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[tokio::test]
    async fn can_query_all() {
        let store = InMemoryAttributeStore::new();
        let entities = store
            .query_entities(&EntityQuery {
                attribute_types: vec![
                    BootstrapSymbol::EntityId.into(),
                    BootstrapSymbol::SymbolName.into(),
                ],
                root: EntityQueryNode::MatchAll(MatchAllQueryNode),
            })
            .await
            .unwrap();
        assert_eq!(
            entities,
            InMemoryAttributeStore::bootstrap_entities()
                .into_iter()
                .map(|entity| EntityRow {
                    values: vec![
                        Some(AttributeValue::EntityId(entity.entity_id)),
                        entity
                            .attributes
                            .get(&BootstrapSymbol::SymbolName.into())
                            .cloned()
                    ]
                })
                .collect::<Vec<_>>()
        );
    }
}
