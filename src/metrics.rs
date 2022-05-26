use lazy_static::lazy_static;
use prometheus::{
    register_int_counter, register_int_counter_vec, register_int_gauge, register_int_gauge_vec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
};

/// Collection of different application metrics
pub struct Metrics {
    pub cached_entries: IntGaugeVec,
    pub corrupted_programs: IntCounter,
    pub evictions: IntCounterVec,
    pub active_subscriptions: IntGaugeVec,
    pub inflight_subscriptions: IntGaugeVec,
    pub active_ws_connections: IntGauge,
    pub ws_reconnects: IntCounterVec,
    pub ws_data_received: IntCounterVec,
    pub cache_size: IntGaugeVec,
}

pub(crate) const ACCOUNTS: &[&str] = &["accounts"];
pub(crate) const PROGRAMS: &[&str] = &["programs"];

lazy_static! {
    /// Globally accessable metrics container
    pub static ref METRICS: Metrics = {
        let cached_entries = register_int_gauge_vec!(
            "cached_entries",
            "Number of entries in cache per entry type",
            &["type"]
        )
        .unwrap();

        let active_subscriptions = register_int_gauge_vec!(
            "active_subscriptions",
            "Number of active websocket subscriptions per websocket worker",
            &["id"]
        ).unwrap();

        let inflight_subscriptions = register_int_gauge_vec!(
            "inflight_subscriptions",
            "Number of unconfirmed websocket subscriptions per websocket worker",
            &["id"]
        ).unwrap();

        let active_ws_connections = register_int_gauge!(
            "active_ws_connections",
            "Number of active websocket connections/workers",
        ).unwrap();

        let ws_data_received = register_int_counter_vec!(
            "ws_data_received",
            "Bytes received from websocket connection per worker",
            &["id"]
        ).unwrap();

        let ws_reconnects = register_int_counter_vec!(
            "ws_reconnects",
            "Number of times each websocket worker was forced to reconnect",
            &["id"]
        ).unwrap();

        let cache_size = register_int_gauge_vec!(
            "cache_size",
            "Memory amount occupied by cache",
            &["type"]
        ).unwrap();

        let evictions = register_int_counter_vec!(
            "evictions",
            "Number of records which were evicted due to full cache",
            &["type"]
        ).unwrap();

        let corrupted_programs = register_int_counter!(
            "corrupted_programs",
            "Number of programs which were corrupted in cache after one or more of their accounts were deleted"
        ).unwrap();

        Metrics {
            cached_entries,
            active_subscriptions,
            inflight_subscriptions,
            active_ws_connections,
            ws_reconnects,
            ws_data_received,
            evictions,
            corrupted_programs,
            cache_size,
        }
    };
}
