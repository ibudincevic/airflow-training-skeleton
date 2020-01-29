from airflow.hooks.base_hook import BaseHook
import requests

class LaunchHook(BaseHook):

    def __init__(self, conn_id):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = super().get_connection(self._conn_id).host  # create connection instance here 

        return self._conn

    def do_stuff(self, arg1, arg2, **kwargs):
        session = self.get_conn()

        query = f"{session}?startdate={ds}&enddate={tomorrow_ds}"
        # result_path = f"/tmp/rocket_launches/ds={ds}"
        # pathlib.Path(result_path).mkdir(parents=True, exist_ok=True)
        response = requests.get(query)
        print(f"response was {response}")

        # session.do_stuff()
