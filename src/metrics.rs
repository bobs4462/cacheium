use lazy_static::lazy_static;
use prometheus::{
    register_int_counter_vec, register_int_gauge, register_int_gauge_vec, IntCounterVec, IntGauge,
    IntGaugeVec,
};

/// Collection of different application metrics
pub struct Metrics {
    pub cached_entries: IntGaugeVec,
    pub active_subscriptions: IntGaugeVec,
    pub active_ws_connections: IntGauge,
    pub ws_data_received: IntCounterVec,
    pub cache_size: IntGauge,
}

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
            &["id", "subscription"]
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

        let cache_size = register_int_gauge!(
            "cache_size",
            "Memory amount occupied by cache",
        ).unwrap();


        Metrics {
            cached_entries,
            active_subscriptions,
            active_ws_connections,
            ws_data_received,
            cache_size,
        }
    };
}
