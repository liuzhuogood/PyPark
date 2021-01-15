import datetime
import json
from decimal import Decimal


class JsonTo(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            if isinstance(obj, Decimal):
                return float(obj)
            if isinstance(obj, (str, int, float, bool, dict)):
                return json.JSONEncoder.default(self, obj)
            return obj.__dict__
