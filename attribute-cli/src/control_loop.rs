use crate::json::to_json;
use crate::pb::entity_query_node::Query;
use crate::pb::{EntityQueryNode, HasAttributeTypesNode, WatchEntitiesRequest};
use crate::{Cli, StatusError};

// See the Bevy query system for a nice way of structuring reading queries.
// Bevy defers updates via 'commands', which is more or less what we need to do here as well.
async fn control_loop_iteration(query: &[()]) -> anyhow::Result<()> {
    Ok(())
}

pub async fn control_loop(cli: &Cli) -> anyhow::Result<()> {
    let request: WatchEntitiesRequest = WatchEntitiesRequest {
        query: Some(EntityQueryNode {
            query: Some(Query::HasAttributeTypes(HasAttributeTypesNode {
                // Insert attribute types here
                attribute_types: vec![],
            })),
        }),
        send_initial_events: true,
    };

    let mut attribute_store_client = crate::create_attribute_store_client(&cli.endpoint).await?;
    let response = attribute_store_client
        .watch_entities(request)
        .await
        .map_err(StatusError::from)?;
    let mut stream = response.into_inner();

    loop {
        tokio::select! {
            message = stream.message() => {
                if let Some(event) = message? {
                    println!("{}", to_json(&event)?);
                } else {
                    break;
                }
            }
        }
    }

    Ok(())
}
