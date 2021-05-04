import logging
from typing import Dict, NoReturn, List

from appmetrics import metrics
from common_proto.incident_pb2 import Incident
from common_proto.normalized_event_pb2 import SocEvent
from common_proto.raw_event_pb2 import RawEvent
from google.protobuf.json_format import ParseDict, ParseError
from requests import HTTPError
from retry import retry
from thehive4py.exceptions import TheHiveException
from thehive4py.models import Alert, Case

from modules.custom_thehive_api import CustomTheHiveApi
from modules.db import hbase_pool
from modules.hbase_event_loader import HbaseEventsLoader
from modules.soc_event_parser import SocEventParser

logger = logging.getLogger('thehive_incidents_pusher')


class TheHivePusher:
    def __init__(self, thehive_settings: Dict, hbase_event_loader_settings: Dict):
        logger.info("Create THive API client with settings: %s", str(thehive_settings))
        self.api = CustomTheHiveApi(**thehive_settings)
        self.hbase_event_loader = HbaseEventsLoader(
            hbase_pool,
            hbase_event_loader_settings['namespace'],
            hbase_event_loader_settings['raw_table_name'],
            hbase_event_loader_settings['normalized_table_name']
        )

    @retry((TheHiveException, HTTPError), tries=5, delay=2)
    @metrics.with_histogram("send_alert", reservoir_type='sliding_time_window')
    def send_alert(self, alert: Alert) -> Dict:
        try:
            response = self.api.create_alert(alert)
            response.raise_for_status()
        except (TheHiveException, HTTPError) as exc:
            metrics.notify('thehive_api_errors', 1)
            logger.error("TheHive create alert error: %s", str(exc))
            raise exc
        return response.json()

    @retry((TheHiveException, HTTPError), tries=5, delay=2)
    def create_case_from_alert(self, alert_id: str) -> NoReturn:
        try:
            self.api.promote_alert_to_case(alert_id)
        except (TheHiveException, HTTPError) as exc:
            metrics.notify('thehive_api_errors', 1)
            logger.error("TheHive create case from alert error: %s", str(exc))
            raise exc

    @retry((TheHiveException, HTTPError), tries=10, delay=6)
    @metrics.with_histogram("create_case", reservoir_type='sliding_time_window')
    def create_case(self, case: Case) -> Dict:
        try:
            response = self.api.create_case(case)
            response.raise_for_status()
        except (TheHiveException, HTTPError) as exc:
            metrics.notify('thehive_api_errors', 1)
            logger.error("TheHive create case error: %s", str(exc))
            raise exc
        return response.json()

    @retry((TheHiveException, HTTPError), tries=5, delay=2)
    @metrics.with_histogram("merge_alerts_in_case", reservoir_type='sliding_time_window')
    def merge_alerts_in_case(self, case_id: str, alert_ids: List[str]) -> Dict:
        try:
            response = self.api.merge_alerts_into_case(case_id, alert_ids)
            response.raise_for_status()
        except (TheHiveException, HTTPError) as exc:
            metrics.notify('thehive_api_errors', 1)
            logger.error("TheHive create case from alert error: %s", str(exc))
            raise exc
        return response.json()

    @retry((TheHiveException, HTTPError), tries=5, delay=2)
    @metrics.with_histogram("set_final_tag", reservoir_type='sliding_time_window')
    def set_final_tag(self, case: Case) -> Dict:
        case.tags.append('FINAL')
        try:
            response = self.api.update_case(case, fields=['tags'])
            response.raise_for_status()
        except (TheHiveException, HTTPError) as exc:
            metrics.notify('thehive_api_errors', 1)
            logger.error("TheHive set tag final error: %s", str(exc))
            raise exc
        return response.json()

    @metrics.with_histogram("full_processing_time", reservoir_type='sliding_time_window')
    def push(self, message: Dict):
        with metrics.timer("thehive_case_preparing", reservoir_type='sliding_time_window'):
            try:
                ea_incident = ParseDict(message, Incident(), ignore_unknown_fields=True)
            except ParseError as err:
                logger.warning("Message %s is not valid. Raised %s", str(message), str(err))
                return
            case = SocEventParser.prepare_thehive_case(ea_incident)
            raw_events = self.load_raw_events(ea_incident)
            case.customFields.update({'raw': {'string': ';\n'.join(raw_events), 'order': len(case.customFields)}})
        r = self.create_case(case)
        case.id = r['id']
        metrics.notify('created_thehive_cases', 1)
        logger.info("Successfully create case from event into THive: %s", str(r))

        normalized_events = self.load_normalized_events(ea_incident)

        alert_ids = []
        for event in normalized_events:
            alert = self._prepare_alert_from_event(event)
            logger.info("Try to send alert to theHive: %s", str(alert))
            try:
                r = self.send_alert(alert)
            except HTTPError as err:
                if err.response.status_code == 400:
                    continue
                raise err
            logger.info("Successfully push alert to THive: %s", str(r))
            metrics.notify('created_thehive_alerts', 1)
            alert_ids.append(r['id'])

        if alert_ids:
            self.merge_alerts_in_case(case.id, alert_ids)
        self.set_final_tag(case)

        logger.info("Successfully process ea message")
        metrics.notify("successfully_processed_messages", 1)

    def load_normalized_events(self, incident: Incident) -> List[SocEvent]:
        logger.info("Try to get normalized events from HBase")
        with metrics.timer("hbase_loading_time", reservoir_type='sliding_time_window'):
            try:
                normalized_events = self.hbase_event_loader.get_normalized_events(
                    [item.value for item in incident.correlationEvent.correlation.eventIds]
                )
                logger.info("Receive normalized events: %s", len(normalized_events))
                metrics.notify("loaded_hbase_normalized_events", len(normalized_events))
            except Exception as err:
                metrics.notify('hbase_errors', 1)
                logger.warning("Some unknown exception have been raised by HBaseEventLoader: %s", str(err))
                normalized_events = []
                pass
        return normalized_events

    def load_raw_events(self, incident: Incident) -> List[RawEvent]:
        logger.info("Try to get raw events from HBase")
        with metrics.timer("hbase_loading_time", reservoir_type='sliding_time_window'):
            try:
                raw_events = self.hbase_event_loader.get_raw_events(
                    [item for item in incident.correlationEvent.data.rawIds]
                )
                logger.info("Receive raw events: %s", len(raw_events))
                metrics.notify("loaded_hbase_raw_events", len(raw_events))
            except Exception as err:
                metrics.notify('hbase_errors', 1)
                logger.warning("Some unknown exception have been raised by HBaseEventLoader: %s", str(err))
                raw_events = []
                pass
        return raw_events

    def _prepare_alert_from_event(self, event: SocEvent) -> Alert:
        logger.info("Parse message with SocEventParser: %s", str(event.id))
        with metrics.timer("thehive_alert_preparing", reservoir_type='sliding_time_window'):
            alert = SocEventParser.prepare_thehive_alert(event)

        logger.info("Complement event data with raw from HBase")
        with metrics.timer("hbase_loading_time", reservoir_type='sliding_time_window'):
            try:
                raw_logs = self.hbase_event_loader.get_raw_events([item for item in event.data.rawIds])
                logger.info("Receive raw logs: %s", str(raw_logs))
                alert.customFields.update(
                    {'raw': {'string': ';\n'.join(raw_logs), 'order': len(alert.customFields)}}
                )
                metrics.notify("enriched_by_hbase_alerts", 1)
            except Exception as err:
                # TODO: specify type of exceptions that should be caught
                metrics.notify('hbase_errors', 1)
                logger.warning("Some exception have been raised by HBaseEnricher: %s", str(err))
                pass
        return alert
