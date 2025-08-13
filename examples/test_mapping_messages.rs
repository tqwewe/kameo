//! Shared message definitions for mapping syntax test

use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize, Serialize};
use std::collections::HashMap;

// Test message similar to PreBacktestMessage
#[derive(RemoteMessage, Debug, Clone, Archive, Serialize, Deserialize)]
pub struct TestDataMessage {
    pub id: u32,
    pub data: HashMap<String, Vec<u8>>,
    pub values: Vec<f64>,
}