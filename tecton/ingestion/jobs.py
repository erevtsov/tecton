from dagster import job
from .ops import hello_op


@job
def hello_job():
    hello_op()
