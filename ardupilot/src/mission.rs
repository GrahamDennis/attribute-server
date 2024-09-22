use crate::connection::{Client, NodeId};
use anyhow::format_err;
use async_trait::async_trait;
use mavio::dialects::common::enums::MavMissionType;
use mavio::dialects::common::messages::{
    MissionAck, MissionCount, MissionItemInt, MissionRequestInt, MissionRequestList,
};
use mavio::protocol::Versioned;
use mavio::Frame;

#[async_trait]
pub trait MissionProtocol {
    async fn request_list(
        &mut self,
        request_list: MissionRequestList,
    ) -> anyhow::Result<Result<MissionCount, MissionAck>>;
    async fn request_int(
        &mut self,
        request_int: MissionRequestInt,
    ) -> anyhow::Result<Result<MissionItemInt, MissionAck>>;

    async fn fetch_mission(
        &mut self,
        target_node_id: NodeId,
    ) -> anyhow::Result<Vec<MissionItemInt>>;
}

trait MissionProtocolInternal<V: Versioned> {
    fn extract_mission_ack<T>(self) -> impl Fn(&Frame<V>) -> Option<Result<T, MissionAck>>;
}

impl<V: Versioned> MissionProtocolInternal<V> for NodeId {
    fn extract_mission_ack<T>(self) -> impl Fn(&Frame<V>) -> Option<Result<T, MissionAck>> {
        move |frame: &Frame<V>| {
            if frame.message_id() == MissionAck::message_id() {
                if let Ok(mission_ack) = MissionAck::try_from(frame.payload()) {
                    if mission_ack.target_system == self.system_id
                        && mission_ack.target_component == self.component_id
                    {
                        return Some(Err(mission_ack));
                    }
                }
            }
            None
        }
    }
}

#[async_trait]
impl<V: Versioned> MissionProtocol for Client<V> {
    async fn request_list(
        &mut self,
        request_list: MissionRequestList,
    ) -> anyhow::Result<Result<MissionCount, MissionAck>> {
        let node_id = self.node_id;
        let mission_ack_extractor = node_id.extract_mission_ack();
        let response_extractor = |frame: &Frame<V>| {
            mission_ack_extractor(frame).or_else(|| {
                if frame.message_id() == MissionCount::message_id() {
                    if let Ok(mission_count) = MissionCount::try_from(frame.payload()) {
                        if mission_count.target_system == node_id.system_id
                            && mission_count.target_component == node_id.component_id
                        {
                            return Some(Ok(mission_count));
                        }
                    }
                }
                None
            })
        };
        self.send_and_await_response_with_extractor(request_list, response_extractor)
            .await
    }

    async fn request_int(
        &mut self,
        request_int: MissionRequestInt,
    ) -> anyhow::Result<Result<MissionItemInt, MissionAck>> {
        let node_id = self.node_id;
        let mission_ack_extractor = node_id.extract_mission_ack();
        let response_extractor = |frame: &Frame<V>| {
            mission_ack_extractor(frame).or_else(|| {
                if frame.message_id() == MissionItemInt::message_id() {
                    if let Ok(mission_item_int) = MissionItemInt::try_from(frame.payload()) {
                        if mission_item_int.target_system == node_id.system_id
                            && mission_item_int.target_component == node_id.component_id
                        {
                            return Some(Ok(mission_item_int));
                        }
                    }
                }
                None
            })
        };
        self.send_and_await_response_with_extractor(request_int, response_extractor)
            .await
    }

    async fn fetch_mission(
        &mut self,
        target_node_id: NodeId,
    ) -> anyhow::Result<Vec<MissionItemInt>> {
        let mission_type = MavMissionType::Mission;
        let mission_count = self
            .request_list(MissionRequestList {
                target_system: target_node_id.system_id,
                target_component: target_node_id.component_id,
                mission_type,
            })
            .await?
            .map_err(|err| format_err!("{err:?}"))?;

        let mut mission_items = Vec::with_capacity(mission_count.count as usize);

        for idx in 0..mission_count.count {
            let mission_item = self
                .request_int(MissionRequestInt {
                    target_system: target_node_id.system_id,
                    target_component: target_node_id.component_id,
                    seq: idx,
                    mission_type,
                })
                .await?
                .map_err(|err| format_err!("{err:?}"))?;

            mission_items.push(MissionItemInt {
                target_system: target_node_id.system_id,
                target_component: target_node_id.component_id,
                current: 0,
                ..mission_item
            });
        }

        Ok(mission_items)
    }
}
