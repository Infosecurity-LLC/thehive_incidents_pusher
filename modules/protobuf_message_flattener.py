from typing import List, Dict, Any

from google.protobuf.reflection import GeneratedProtocolMessageType
from google.protobuf.wrappers_pb2 import DESCRIPTOR as WRAPPERS_FILE_DESCRIPTOR


class ProtobufMessageFlattener:
    @staticmethod
    def _parse_enum_name(descriptor, value):
        return descriptor.enum_type.values_by_number[value].name

    @staticmethod
    def _full_path(path_stack: List[str]) -> str:
        # It's necessary due to references to custom fields in TheHive templates
        def capitalize_first_char(s: str) -> str:
            return s[0].upper() + s[1:]
        path_without_underscore = []
        for item in path_stack:
            path_without_underscore.extend(item.split('_'))
        return ''.join([
            path_without_underscore[0],
            *(capitalize_first_char(item) for item in path_without_underscore[1:])
        ])

    @staticmethod
    def flatten_object(obj: GeneratedProtocolMessageType, path_stack: List[str] = None,
                       result: Dict = None) -> Dict[str, Any]:
        if path_stack is None:
            path_stack = []
        if result is None:
            result = {}

        for descriptor in obj.DESCRIPTOR.fields:
            value = getattr(obj, descriptor.name)
            # Skip empty object
            if descriptor.label == descriptor.LABEL_OPTIONAL and descriptor.type == descriptor.TYPE_MESSAGE \
                    and not value.ByteSize():
                continue
            # open new level of recursion
            path_stack.append(descriptor.name)
            # object parsing
            if descriptor.type == descriptor.TYPE_MESSAGE:
                # parse object list
                if descriptor.label == descriptor.LABEL_REPEATED:
                    if descriptor.message_type.file != WRAPPERS_FILE_DESCRIPTOR:
                        for index, list_item in enumerate(value):
                            path_stack.append(f"[{index}]")
                            ProtobufMessageFlattener.flatten_object(list_item, path_stack, result)
                            path_stack.pop()
                    else:
                        values = [item.value for item in value]
                        result.update({ProtobufMessageFlattener._full_path(path_stack): '; '.join(values)})
                # parse wrapped optional value
                elif descriptor.label == descriptor.LABEL_OPTIONAL and \
                        descriptor.message_type.file == WRAPPERS_FILE_DESCRIPTOR:
                    result.update({ProtobufMessageFlattener._full_path(path_stack): value.value})
                else:
                    ProtobufMessageFlattener.flatten_object(value, path_stack, result)
            # enum parsing
            elif descriptor.type == descriptor.TYPE_ENUM:
                if descriptor.label == descriptor.LABEL_REPEATED:
                    # there may be many of enums
                    values = [ProtobufMessageFlattener._parse_enum_name(descriptor, item) for item in value]
                    result.update({ProtobufMessageFlattener._full_path(path_stack): '; '.join(values)})
                else:
                    result.update({
                        ProtobufMessageFlattener._full_path(path_stack):
                            ProtobufMessageFlattener._parse_enum_name(descriptor, value)
                    })
            # scalar value parsing
            else:
                if descriptor.label == descriptor.LABEL_REPEATED:
                    # there may be many of values
                    values = [item for item in value]
                    result.update({ProtobufMessageFlattener._full_path(path_stack): '; '.join(values)})
                else:
                    result.update({ProtobufMessageFlattener._full_path(path_stack): value})
            # leave recursion lvl \o/
            path_stack.pop()
        return result
