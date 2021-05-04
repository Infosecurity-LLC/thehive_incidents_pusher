import logging

from appmetrics import metrics
from appmetrics.histogram import SlidingTimeWindowReservoir
from flask import Flask

logger = logging.getLogger('thehive_incidents_pusher')


def register_app_metrics():
    metrics.new_counter("received_kafka_messages")
    metrics.new_counter("created_thehive_alerts")
    metrics.new_counter("created_thehive_cases")
    metrics.new_counter("successfully_processed_messages")
    metrics.new_counter("enriched_by_hbase_alerts")
    metrics.new_counter("loaded_hbase_normalized_events")
    metrics.new_counter("loaded_hbase_raw_events")
    metrics.new_counter("thehive_api_errors")
    metrics.new_counter("hbase_errors")

    if not metrics.REGISTRY.get("full_processing_time"):
        metrics.new_histogram("full_processing_time", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("hbase_loading_time"):
        metrics.new_histogram("hbase_loading_time", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("send_alert"):
        metrics.new_histogram("send_alert", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("create_case"):
        metrics.new_histogram("create_case", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("merge_alerts_in_case"):
        metrics.new_histogram("merge_alerts_in_case", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("set_final_tag"):
        metrics.new_histogram("set_final_tag", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("thehive_alert_preparing"):
        metrics.new_histogram("thehive_alert_preparing", SlidingTimeWindowReservoir())
    if not metrics.REGISTRY.get("thehive_case_preparing"):
        metrics.new_histogram("thehive_case_preparing", SlidingTimeWindowReservoir())

    metrics.tag("received_kafka_messages", "default")
    metrics.tag("created_thehive_alerts", "default")
    metrics.tag("created_thehive_cases", "default")
    metrics.tag("successfully_processed_messages", "default")
    metrics.tag("enriched_by_hbase_alerts", "default")
    metrics.tag("loaded_hbase_normalized_events", "default")
    metrics.tag("loaded_hbase_raw_events", "default")
    metrics.tag("thehive_api_errors", "default")
    metrics.tag("hbase_errors", "default")
    metrics.tag("full_processing_time", "default")
    metrics.tag("hbase_loading_time", "default")
    metrics.tag("full_processing_time", "profiling")
    metrics.tag("hbase_loading_time", "profiling")
    metrics.tag("send_alert", "profiling")
    metrics.tag("create_case", "profiling")
    metrics.tag("merge_alerts_in_case", "profiling")
    metrics.tag("set_final_tag", "profiling")
    metrics.tag("thehive_alert_preparing", "profiling")
    metrics.tag("thehive_case_preparing", "profiling")
    logger.info("Register some metrics for app: %s", str(metrics.REGISTRY))


def run_metrics_webserver(host: str = '0.0.0.0', port: int = 5000):
    app = Flask(__name__)
    from appmetrics.wsgi import AppMetricsMiddleware
    app.wsgi_app = AppMetricsMiddleware(app.wsgi_app, "app_metrics")
    app.run(host, port, debug=False)
