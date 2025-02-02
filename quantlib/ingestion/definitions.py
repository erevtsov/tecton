# Dagster 1.0+ style
import dagster as dg

from .jobs import hello_job

defs = dg.Definitions(
    jobs=[hello_job],
    assets=[],
    # resources=[],
    # schedules=[],
    # sensors=[],
)
