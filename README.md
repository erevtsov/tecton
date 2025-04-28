# tecton
Personal project to build a quant investment platform while testing out some newer tools/libraries.
- dagster for data ingesting, pipelines, job scheduling
- ibis with duckdb backend for data querying
    - store data in parquet


#### Abstractions
- Mantle: core data access
- Markets: a collection of tradeable futures markets
    - Market: entries of Markets
        - some metadata is lazy-loaded
    - Contract: the discrete futures contract
- FactorModel: factor model instance (SignalModel)
    - ModelDefinition:
        - contains necessary information to build the model
    - types:
        - AlphaModel
        - RiskModel
- Strategy:
- Simulation:
- Portfolio:

#### Vision for Supported Asset Classes/Strategies
- Futures+Forwards
    - Trend
        - price trend
        - fundamental trend
    - Carry
- Crypto
    - Fundamental L/S
- Equities
    - Fundamental L/S

### Notes on env setup:

#### Install uv

```
curl -LsSf https://astral.sh/uv/install.sh | sh
brew install ta-lib
source ~/.local/bin/env
uv sync
```

#### .bashrc
```
source .venv/bin/activate
```

#### ta-lib
https://github.com/TA-Lib/ta-lib-python

### Target Data Calls

```
from tecton.dal.mantle import Mantle

Mantle.select(Mantle.tables.futures, start, end, elements=[])

```

### TODOs
### laptop setup
- copy raw data files
- implement S3 and local storage options


#### Futures Data
- add data checks for partitions
- return data sorted...

#### Signals
- store model definition in yaml
- build factor-forecast signal pipeline

#### Portfolio Construction
- calc/store vol
- try using garch model for vol
- calculate market and asset correlations

#### Features
- lookback capability


