# python-hddb

A Python client for [Datasketch](https://datasketch.co/) and [MotherDuck](https://motherduck.com/).

## Installation

```bash
pip install python-hddb
```

## Usage

```python
from python_hddb import HdDB

db_client = HdDB()
df = pd.DataFrame(
    data={"username": ["ddazal", "lcalderon", "pipeleon"], "age": [30, 28, 29]}
)
db_client.create_database(dataframes=[df], names=["users"])
```