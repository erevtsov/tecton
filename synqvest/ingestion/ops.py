from dagster import op


@op
def hello_op():
    return 'Hello from Dagster!'
