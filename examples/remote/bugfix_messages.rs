//! Shared message definitions for bugfix test - SINGLE SOURCE OF TRUTH
//! 
//! This reproduces the real trading system message flow:
//! 1. PreBacktest: Executor -> TAManager
//! 2. BacktestIteration: Executor -> TAManager  
//! 3. BacktestSummary: Executor -> TAManager (THIS IS THE ONE FAILING)
//!
//! CRITICAL: This file is the ONLY place these message types are defined.
//! Both client and server import from this single source to ensure identical type hashes.

use kameo::RemoteMessage;
use rkyv::{Archive, Deserialize as RDeserialize, Serialize as RSerialize};

/// PreBacktest message - simulates real PreBacktestMessage
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct PreBacktestMessage {
    pub strategy: String,
    pub symbol: String,
    pub timeframe: String,
}

/// BacktestIteration message - simulates real BacktestIterationMessage  
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct BacktestIterationMessage {
    pub iteration: u32,
    pub progress_pct: f64,
    pub current_pnl: f64,
}

/// BacktestSummary message - THIS IS THE ONE FAILING IN REAL SYSTEM
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct BacktestSummaryMessage {
    pub backtest_id: String,
    pub total_pnl: f64,
    pub total_trades: u32,
    pub win_rate: f64,
    pub summary_data: Vec<u8>, // Large data like real system (122836 bytes)
}

/// Response from TAManager back to Executor
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TAManagerResponse {
    pub message_id: u32,
    pub response_data: String,
    pub timestamp: String,
}

/// Test completion marker
#[derive(RemoteMessage, Debug, Clone, Archive, RSerialize, RDeserialize)]
pub struct TestComplete {
    pub total_messages_sent: u32,
    pub test_duration_ms: u64,
}

/// Message for setting executor reference for bidirectional communication
#[derive(Clone)]
pub struct SetExecutorRef {
    pub executor_ref: kameo::remote::DistributedActorRef,
}