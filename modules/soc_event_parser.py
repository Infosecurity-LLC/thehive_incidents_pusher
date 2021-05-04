from datetime import datetime
from typing import Dict, List

from common_proto.incident_pb2 import Incident
from common_proto.normalized_event_pb2 import SocEvent
from google.protobuf.reflection import GeneratedProtocolMessageType
from thehive4py.models import Alert, AlertArtifact, CustomFieldHelper, Case

from modules.protobuf_message_flattener import ProtobufMessageFlattener


class CustomFieldsBuilder(object):
    @staticmethod
    def build(fields: Dict) -> Dict:
        custom_field_helper = CustomFieldHelper()
        for key, field_value in fields.items():
            if ('time' in key.lower() or 'date' in key.lower()) and isinstance(field_value, int):
                custom_field_helper.add_date(key, field_value * 1000)
            elif isinstance(field_value, datetime):
                custom_field_helper.add_number(key, int(field_value.timestamp()) * 1000)
            elif isinstance(field_value, bool):
                custom_field_helper.add_boolean(key, field_value)
            elif isinstance(field_value, int):
                custom_field_helper.add_number(key, field_value)
            elif isinstance(field_value, str):
                custom_field_helper.add_string(key, field_value)
            else:
                try:
                    custom_field_helper.add_string(key, field_value)
                except (TypeError, ValueError):
                    pass
        return custom_field_helper.build()


class SocEventParser:
    @classmethod
    def prepare_thehive_alert(cls, event: SocEvent) -> Alert:
        return Alert(
            title=cls._get_title(event),
            type=cls._get_alert_type(event),
            source=cls._get_alert_source(event),
            sourceRef=cls._get_alert_source_ref(event),
            description=cls._get_description(event),
            customFields=cls.prepare_custom_fields(event),
            # below only not required attributes
            date=cls._get_datetime(event),
            severity=cls._get_severity(event),
            caseTemplate=cls._get_case_template_name_for_alert(event),
            tags=cls._prepare_tags(event),
            # items that will be used in case investigation
            artifacts=cls._prepare_artifacts(event)
        )

    @classmethod
    def prepare_thehive_case(cls, incident: Incident) -> Case:
        return Case(
            title=cls._get_case_title(incident),
            description=cls._get_case_description(incident),
            severity=cls._get_case_severity(incident),
            tags=cls._prepare_case_tags(incident),
            startDate=cls._get_case_datetime(incident),
            metrics=cls._prepare_metrics(incident),
            customFields=cls.prepare_custom_fields(incident),
            template=cls._get_case_template_name(incident)
        )

    @staticmethod
    def _get_title(event: SocEvent) -> str:
        return event.id

    @staticmethod
    def _get_case_title(incident: Incident) -> str:
        return f'{incident.usecaseId}_{incident.id}'

    @staticmethod
    def _get_alert_type(event: SocEvent) -> str:
        descriptor = event.eventSource.DESCRIPTOR.fields_by_name.get('category')
        category_name = descriptor.enum_type.values_by_number[event.eventSource.category].name
        return category_name

    @staticmethod
    def _get_alert_source(event: SocEvent) -> str:
        # У нас весь источник события описывается с помощью eventSource
        source = event.eventSource
        return ':'.join((source.vendor, source.title, source.subsys.value))

    @staticmethod
    def _get_alert_source_ref(event: SocEvent) -> str:
        return event.id

    @staticmethod
    def _get_datetime(event: SocEvent) -> int:
        return event.eventTime * 1000

    @staticmethod
    def _get_case_datetime(incident: Incident) -> int:
        return incident.detectedTime * 1000

    @staticmethod
    def _get_description(event: SocEvent) -> str:
        # IMHO, description should be received from references
        # FIXME freeze me pls or I'll chase you with axe
        # return "Here's Johnny!"
        return event.eventSource.id

    @staticmethod
    def _get_case_description(incident: Incident) -> str:
        return incident.correlationEvent.eventSource.id

    @staticmethod
    def _get_severity(event: SocEvent) -> int:
        # kostyl', cause thehive severity could be only in {1,2,3}
        return event.interaction.importance - 1 if event.interaction.importance > 1 else 1

    @staticmethod
    def _get_case_severity(incident: Incident) -> int:
        # kostyl', cause thehive severity could be only in {1,2,3}
        return incident.severityLevel - 1 if incident.severityLevel > 1 else 1

    @staticmethod
    def _get_case_template_name_for_alert(event: SocEvent):
        # It depends on usecase and maybe organization
        # TODO: remove hardcoded value (kostylization sucks)
        # template_name = f'{event.correlation.name}_template'
        return 'Alert_template_full'

    @staticmethod
    def _get_case_template_name(event: SocEvent):
        # It depends on usecase and maybe organization
        # TODO: remove hardcoded value (kostylization sucks)
        # template_name = f'{event.correlation.name}_template'
        return 'Case_template_full'

    @classmethod
    def _prepare_tags(cls, event: SocEvent) -> List[str]:
        importance_descriptor = event.interaction.DESCRIPTOR.fields_by_name.get('importance')
        importance = importance_descriptor.enum_type.values_by_number[event.interaction.importance].name
        return [
            cls._get_alert_type(event),
            event.eventSource.vendor,
            event.eventSource.title,
            importance
        ]

    @classmethod
    def _prepare_case_tags(cls, incident: Incident) -> List[str]:
        severity_descriptor = incident.DESCRIPTOR.fields_by_name.get('severityLevel')
        severity = severity_descriptor.enum_type.values_by_number[incident.severityLevel].name
        return [
            incident.correlationEvent.collector.organization,
            incident.correlationRuleName,
            incident.usecaseId,
            severity
        ]

    @staticmethod
    def prepare_custom_fields(obj: GeneratedProtocolMessageType) -> Dict:
        # Any information that's important but not included not in case neither in alert
        # List of custom fields depends on usecase, IMHO
        fields = ProtobufMessageFlattener.flatten_object(obj)
        return CustomFieldsBuilder.build(fields)

    @classmethod
    def _prepare_artifacts(cls, event: SocEvent) -> List[AlertArtifact]:
        # TODO: implement artifacts parsing
        ...
        return []

    @staticmethod
    def _prepare_metrics(event: SocEvent) -> Dict:
        # TODO: implement metrics parsing
        return {}
