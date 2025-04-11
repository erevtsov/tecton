import dagster as dg

from tecton.data.futures.assets import (
    futures_continuous_data,
    futures_discrete_data,
)

# Define the Definitions object
defs = dg.Definitions(
    assets=[
        futures_discrete_data,
        futures_continuous_data,
    ],
    # resources={'storage_backend': STORAGE_BACKEND},
)
