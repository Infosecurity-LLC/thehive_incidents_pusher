import json
from typing import List

import requests
from thehive4py.api import TheHiveApi, TheHiveException


class CustomTheHiveApi(TheHiveApi):
    def merge_alerts_into_case(self, case_id: str, alert_ids: List[str]) -> requests.Response:
        req = self.url + "/api/alert/merge/_bulk"

        try:
            data = json.dumps({"caseId": case_id, "alertIds": alert_ids})
            return requests.post(req, headers={'Content-Type': 'application/json'}, data=data, proxies=self.proxies,
                                 auth=self.auth, verify=self.cert)
        except requests.exceptions.RequestException as e:
            raise TheHiveException("Merge alerts into case error: {}".format(e))

    def get_custom_fields(self) -> requests.Response:
        req = self.url + '/api/list/custom_fields'
        try:
            return requests.get(req, headers={'Content-Type': 'application/json'}, proxies=self.proxies,
                                auth=self.auth, verify=self.cert)
        except requests.exceptions.RequestException as e:
            raise TheHiveException("Getting custom fields error: {}".format(e))
