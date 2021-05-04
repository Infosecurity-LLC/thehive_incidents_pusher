from datetime import datetime, date
from decimal import Decimal
from json import JSONEncoder

from bson.objectid import ObjectId


class CorrectJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime) or isinstance(o, date):
            return o.isoformat()
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, Decimal):
            return o.__float__()
        return super(CorrectJSONEncoder, self).default(o)
