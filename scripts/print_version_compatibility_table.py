import json
import os

import pandas as pd
import tabulate


VERSION_COMPATIBILITY_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "octue",
    "metadata",
    "version_compatibilities.json",
)

with open(VERSION_COMPATIBILITY_DATA_PATH) as f:
    VERSION_COMPATIBILITY_DATA = json.load(f)

print(tabulate.tabulate(pd.DataFrame.from_records(VERSION_COMPATIBILITY_DATA), headers="keys", tablefmt="grid"))
