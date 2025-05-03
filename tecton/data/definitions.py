import dagster as dg

from tecton.data.futures.assets import (
    futures_continuous_data,
    futures_discrete_data,
)
from tecton.data.models.trend.assets import (
    factors,
)

# Define the Definitions object
defs = dg.Definitions(
    assets=[
        futures_discrete_data,
        futures_continuous_data,
        factors,
    ],
    # resources={'storage_backend': STORAGE_BACKEND},
)
