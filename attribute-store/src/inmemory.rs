use crate::store::{
    AttributeStore, AttributeStoreError, AttributeType, AttributeValue, BootstrapSymbol, Entity,
    EntityId, EntityLocator, EntityQuery, EntityRow, Symbol, ValueType,
};
use async_trait::async_trait;
use parking_lot::{Mutex, MutexGuard};
use std::collections::{HashMap, HashSet};
use tracing::Level;

#[derive(Debug)]
pub struct InMemoryAttributeStore {
    attribute_types: Mutex<HashSet<Symbol>>,
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
            .flat_map(|entity| match entity.attributes.get(&symbol_name_symbol) {
                Some(AttributeValue::String(symbol_name)) => {
                    Symbol::try_from(symbol_name.as_ref()).ok()
                }
                _ => None,
            })
            .collect();
        InMemoryAttributeStore {
            attribute_types: Mutex::new(attribute_types),
            entities: Mutex::new(entities),
        }
    }

    #[inline]
    fn all_locks<'a>(&'a self) -> (MutexGuard<'a, HashSet<Symbol>>, MutexGuard<'a, Vec<Entity>>) {
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

#[async_trait]
impl AttributeStore for InMemoryAttributeStore {
    #[tracing::instrument(skip(self), ret(level = Level::TRACE), err(level = Level::WARN))]
    async fn create_attribute_type(
        &self,
        attribute_type: &AttributeType,
    ) -> Result<Entity, AttributeStoreError> {
        use AttributeStoreError::*;

        log::trace!("Received create_attribute_type request");

        let (mut locked_attribute_types, mut locked_entities) = self.all_locks();

        let symbol_name_symbol: Symbol = BootstrapSymbol::SymbolName.into();
        let attribute_type_symbol_name: &str = &attribute_type.symbol;
        if let Some(matching_entity) = locked_entities.iter().find(|entity| {
            entity
                .attributes
                .get(&symbol_name_symbol)
                .is_some_and(|value| match value {
                    AttributeValue::String(symbol_name) => {
                        symbol_name.eq(attribute_type_symbol_name)
                    }
                    _ => false,
                })
        }) {
            return Err(AttributeTypeConflictError(matching_entity.clone()));
        }

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
                (symbol_name_symbol, AttributeValue::String(attribute_type_symbol_name.to_string())),
                (BootstrapSymbol::ValueType.into(), AttributeValue::EntityId(attribute_type.value_type.into()))
            ])
        };

        locked_entities.push(entity.clone());
        locked_attribute_types.insert(attribute_type.symbol.clone());

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
        use AttributeStoreError::*;

        log::trace!("Received query_entities request");

        let (locked_attribute_types, locked_entities) = self.all_locks();

        let invalid_requested_attribute_types: Vec<_> = entity_query
            .attribute_types
            .iter()
            .filter(|attribute_type| !locked_attribute_types.contains(attribute_type))
            .cloned()
            .collect();

        if !invalid_requested_attribute_types.is_empty() {
            return Err(UnregisteredAttributeTypes(
                invalid_requested_attribute_types,
            ));
        }

        let entity_rows = locked_entities
            .iter()
            .filter(entity_query.root.to_predicate())
            .map(|entity| entity.to_entity_row(&entity_query.attribute_types))
            .collect();

        Ok(entity_rows)
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
