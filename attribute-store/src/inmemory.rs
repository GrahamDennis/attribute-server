use crate::store::{
    AttributeStore, AttributeStoreError, AttributeStoreErrorKind, AttributeToUpdate,
    AttributeTypes, AttributeValue, BootstrapSymbol, CreateAttributeTypeRequest, Entity, EntityId,
    EntityLocator, EntityQuery, EntityRow, EntityVersion, Symbol, UpdateEntityRequest, ValueType,
    WatchEntitiesEvent,
};
use garde::Unvalidated;
use std::collections::HashMap;
use tokio::sync::broadcast;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::Level;

#[derive(Debug)]
pub struct InMemoryAttributeStore {
    attribute_types: AttributeTypes,
    entities: Vec<Entity>,
    watch_entities_channel: Sender<WatchEntitiesEvent>,
    entity_version_sequence: std::ops::RangeFrom<i64>,
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
        let (tx, _) = broadcast::channel(16);
        InMemoryAttributeStore {
            attribute_types,
            entities,
            watch_entities_channel: tx,
            entity_version_sequence: 1..,
        }
    }

    fn entity_version(&mut self) -> EntityVersion {
        EntityVersion(self.entity_version_sequence.next().unwrap())
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

    fn insert_new_entity_with_attributes(
        &mut self,
        attributes: HashMap<Symbol, AttributeValue>,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreErrorKind::*;

        let database_id = self.entities.len();
        let entity = Entity {
            entity_id: EntityId(i64::try_from(database_id).map_err(|err| Other {
                message: format!(
                    "Failed to convert database id `{database_id}` to EntityId due to error `{err:?}`"
                ),
                source: err.into(),
            })?),
            entity_version: self.entity_version(),
            attributes,
        };

        self.entities.push(entity.clone());

        let _ = self.watch_entities_channel.send(WatchEntitiesEvent {
            before: None,
            after: Some(entity.clone()),
        });

        Ok(entity)
    }

    fn update_existing_entity(
        entity: &mut Entity,
        attributes_to_update: &[AttributeToUpdate],
        watch_entities_channel: &Sender<WatchEntitiesEvent>,
        entity_version_sequence: &mut std::ops::RangeFrom<i64>,
    ) -> Result<Entity, AttributeStoreError> {
        let before = entity.clone();
        for attribute_to_update in attributes_to_update {
            match &attribute_to_update.value {
                None => entity.attributes.remove(&attribute_to_update.symbol),
                Some(attribute_value) => entity
                    .attributes
                    .insert(attribute_to_update.symbol.clone(), attribute_value.clone()),
            };
        }
        if before != *entity {
            entity.entity_version = EntityVersion(entity_version_sequence.next().unwrap());
            let _ = watch_entities_channel.send(WatchEntitiesEvent {
                before: Some(before),
                after: Some(entity.clone()),
            });
        }

        Ok(entity.clone())
    }
}

impl AttributeStore for InMemoryAttributeStore {
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    fn create_attribute_type(
        &mut self,
        create_attribute_type_request: &CreateAttributeTypeRequest,
    ) -> Result<Entity, AttributeStoreError> {
        log::trace!("Received create_attribute_type request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();

        // validate
        let validated_request =
            Unvalidated::new(create_attribute_type_request).validate_with(&self.attribute_types)?;
        let CreateAttributeTypeRequest { attribute_type } = validated_request.into_inner();

        let entity = self.insert_new_entity_with_attributes(HashMap::from([
            (
                symbol_name_symbol,
                AttributeValue::String(attribute_type.symbol.to_string()),
            ),
            (
                BootstrapSymbol::ValueType.into(),
                AttributeValue::EntityId(attribute_type.value_type.into()),
            ),
        ]))?;

        self.attribute_types
            .insert(attribute_type.symbol.clone(), attribute_type.value_type);

        Ok(entity)
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    fn get_entity(&self, entity_locator: &EntityLocator) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreErrorKind::*;

        log::trace!("Received get_entity request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
        let entity = match entity_locator {
            EntityLocator::EntityId(entity_id) => self.entities.get(usize::try_from(*entity_id)?),
            EntityLocator::Symbol(symbol) => {
                let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                self.entities.iter().find(|entity| {
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
    fn query_entities(
        &self,
        entity_query: &EntityQuery,
    ) -> Result<Vec<EntityRow>, AttributeStoreError> {
        log::trace!("Received query_entities request");

        // validate
        let validated_entity_query =
            Unvalidated::new(entity_query).validate_with(&self.attribute_types)?;
        let EntityQuery {
            root,
            attribute_types,
        } = validated_entity_query.into_inner();

        let entity_rows = self
            .entities
            .iter()
            .filter(|entity| root.matches(entity))
            .map(|entity| entity.to_entity_row(attribute_types))
            .collect();

        Ok(entity_rows)
    }

    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    fn update_entity(
        &mut self,
        update_entity_request: &UpdateEntityRequest,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreErrorKind::*;
        log::trace!("Received query_entities request");

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();

        // Validate
        let validated_update_entity_request =
            Unvalidated::from(update_entity_request).validate_with(&self.attribute_types)?;
        let UpdateEntityRequest {
            entity_locator,
            attributes_to_update,
        } = validated_update_entity_request.into_inner();

        // Update entity
        let existing_entity =
            match entity_locator {
                EntityLocator::EntityId(entity_id) => {
                    let Some(entity) = self.entities.get_mut(usize::try_from(*entity_id)?) else {
                        return Err(EntityNotFound(entity_locator.clone()))?;
                    };
                    Some(entity)
                }
                EntityLocator::Symbol(symbol) => {
                    let expected_attribute_value = AttributeValue::String(symbol.clone().into());
                    let entity =
                        self.entities.iter_mut().find(|entity| {
                            entity.attributes.get(&symbol_name_symbol).is_some_and(
                                |attribute_value| attribute_value.eq(&expected_attribute_value),
                            )
                        });
                    if entity.is_none() {
                        let expected_symbol_attribute = AttributeToUpdate {
                            symbol: symbol_name_symbol,
                            value: Some(expected_attribute_value),
                        };
                        if !attributes_to_update.contains(&expected_symbol_attribute) {
                            return Err(UpdateNotIdempotent {
                                missing_attribute_to_update: expected_symbol_attribute,
                                entity_locator: entity_locator.clone(),
                            })?;
                        }
                    }
                    entity
                }
            };

        match existing_entity {
            None =>
            // FIXME: Validate that the new entity matches the provided locator
            {
                self.insert_new_entity_with_attributes(
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
                )
            }
            Some(entity) => Self::update_existing_entity(
                entity,
                attributes_to_update,
                &self.watch_entities_channel,
                &mut self.entity_version_sequence,
            ),
        }
    }

    #[tracing::instrument(skip(self))]
    fn watch_entities_receiver(&self) -> Receiver<WatchEntitiesEvent> {
        self.watch_entities_channel.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{EntityQueryNode, MatchAllQueryNode};

    #[test]
    fn can_fetch_by_entity_id() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::EntityId(BootstrapSymbol::EntityId.into()))
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[test]
    fn can_fetch_by_symbol() {
        let store = InMemoryAttributeStore::new();
        let entity_id_entity = store
            .get_entity(&EntityLocator::Symbol(BootstrapSymbol::EntityId.into()))
            .unwrap();
        assert_eq!(entity_id_entity, BootstrapSymbol::EntityId.into());
    }

    #[test]
    fn can_query_all() {
        let store = InMemoryAttributeStore::new();
        let entities = store
            .query_entities(&EntityQuery {
                attribute_types: vec![
                    BootstrapSymbol::EntityId.into(),
                    BootstrapSymbol::SymbolName.into(),
                ],
                root: EntityQueryNode::MatchAll(MatchAllQueryNode),
            })
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
