# Dagster 1.0+ style
from dagster import Definitions
from .jobs import hello_job

defs = Definitions(
    jobs=[hello_job],
    # resources=[],
    # schedules=[],
    # sensors=[],
)