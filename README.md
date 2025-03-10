# tecton

### Notes on env setup:

#### Install uv

`curl -LsSf https://astral.sh/uv/install.sh | sh`

#### sync dependencies
`uv sync`

#### .bashrc
```
source ~/.local/bin/env
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

#### Infrastructure
- how to maintain dagster state after the server is shut down?

#### Futures Data
- fix the weird price bug in 2012
- fix ts_ref missing intermittedly
- create name map for the assets in universe
- filter files to scan based on start/end dates
- add data checks for partitions
- return data sorted...

#### Features
- lookback capability


