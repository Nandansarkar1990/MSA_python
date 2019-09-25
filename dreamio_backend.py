import requests
import logging
import json


log = logging.getLogger(__name__)


class DremIOClient(object):
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.headers = {"accept": "application/json",
                        "content-type": "application/json"}
        self.token = self.get_dremio_token(username, password)



    def get_url(self, uri):
        return self.base_url + uri

    def request_get(self, uri):
        url = self.get_url(uri)
        r = requests.get(url=url, headers=self.headers)
        return r

    def request_post(self, uri, data, add_token = True):
        if add_token:
            self.headers["Authorization"] = self.token
        url = self.get_url(uri)
        data = json.dumps(data, indent=2)
        print("Posting %s "% url, data, self.headers)
        r = requests.post(url=url, headers=self.headers, data=data)
        return r

    def request_get(self, uri, add_token=True):
        url = self.get_url(uri)
        if add_token:
            self.headers["Authorization"] = self.token
        r = requests.get(url=url, headers=self.headers)
        return r

    def get_dremio_token(self, username, password):
        resp = self.request_post("/apiv2/login", data={"userName": username,
                                                       "password": password},
                                 add_token=False)
        return "_dremio"+resp.json().get("token", None)

    def create_job(self, query):
        resp = self.request_post("/api/v3/sql", data={"sql": query},
                                 add_token=True)
        if resp.status_code == 200:
            job = resp.json().get("id", None)
        else:
            raise Exception("Failed to create job error code " + str(resp.status_code))
        return job

    def fetch_job_data(self, job_id):
        resp = self.request_get("/api/v3/job/%s/results" % job_id)
        print("Fetch Data", resp)
        return resp.json()
