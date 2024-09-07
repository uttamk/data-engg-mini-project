from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def hello_world():
    @task()
    def list_files():
        gcs_hook = GCSHook()
        print(gcs_hook.list("movies_load_bucket_sneha"))

    list_files()


hello_world()
