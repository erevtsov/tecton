# tecton

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
from tecton.dal.loader import Scout

Scout.fetch(Scout.tables.universe, start, end, elements=[])

```

### TODOs
### laptop setup
- copy raw data files
- implement S3 and local storage options


#### Futures Data
- fix the weird price bug in 2012
- fix issues w/ data scaling
- create name map for the assets in universe
- add data checks for partitions
- return data sorted...

#### Signals
- store model definition in yaml
- build micro-agg-forecast signal pipeline

#### Portfolio Construction

#### Features
- lookback capability


