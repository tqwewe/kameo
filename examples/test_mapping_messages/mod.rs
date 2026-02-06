use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};
use std::collections::HashMap;

#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TestDataMessage {
    pub id: u32,
    pub data: HashMap<String, Vec<u8>>,
    pub values: Vec<f64>,
}
