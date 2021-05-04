import logging
import socket
from typing import List

import happybase
from appmetrics import metrics
from common_proto.normalized_event_pb2 import SocEvent
from common_proto.raw_event_pb2 import RawEvent
from happybase import NoConnectionsAvailable
from retry import retry
from thriftpy2.protocol.exc import TException

logger = logging.getLogger('thehive_incidents_pusher')


class HbaseEventsLoader:
    def __init__(self, hbase_pool: happybase.ConnectionPool, namespace: str, raw_table_name: str,
                 normalized_table_name: str):
        self.hbase_pool = hbase_pool
        self.namespace = namespace
        self.raw_table_name = raw_table_name
        self.normalized_table_name = normalized_table_name

    def _full_table_name(self, table_name: str) -> str:
        return f'{self.namespace}:{table_name}'

    @property
    def full_raw_table_name(self) -> str:
        return self._full_table_name(self.raw_table_name)

    @property
    def full_normalized_table_name(self) -> str:
        return self._full_table_name(self.normalized_table_name)

    def get_raw_events(self, event_ids: List[str]) -> List[str]:
        try:
            result = self._get_events_from_hbase(event_ids, self.full_raw_table_name)
        except (NoConnectionsAvailable, TException, socket.timeout) as err:
            result = []
            pass
        return [self._raw_event_deserializer(item) for item in result]

    def get_normalized_events(self, event_ids: List[str]) -> List[SocEvent]:
        try:
            result = self._get_events_from_hbase(event_ids, self.full_normalized_table_name)
        except (NoConnectionsAvailable, TException, socket.timeout) as err:
            result = []
            pass
        return [self._normalized_event_deserializer(item) for item in result]

    @retry((NoConnectionsAvailable, TException, socket.timeout), tries=3, delay=1)
    def _get_events_from_hbase(self, event_ids: List[str], full_table_name: str) -> List[bytes]:
        if not event_ids:
            return []
        try:
            with self.hbase_pool.connection() as conn:
                table = conn.table(full_table_name)
                table.scan()
                result = table.rows([event_id.encode() for event_id in event_ids], columns=[b'n:e'])
        except (NoConnectionsAvailable, TException, socket.timeout) as err:
            logger.warning("HBase request raised an error: %s", str(err))
            metrics.notify("hbase_errors", 1)
            raise err
        return [data[b'n:e'] for _, data in result]

    @staticmethod
    def _raw_event_deserializer(protobuf_decoded_str: bytes) -> str:
        event = RawEvent().FromString(protobuf_decoded_str)
        return event.raw

    @staticmethod
    def _normalized_event_deserializer(protobuf_decoded_str: bytes) -> SocEvent:
        event = SocEvent().FromString(protobuf_decoded_str)
        return event
