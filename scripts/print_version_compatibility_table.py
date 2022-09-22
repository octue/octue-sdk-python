import collections
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

# Order the data so the highest versions are shown first in the columns and rows.
sorted_data = collections.OrderedDict(sorted(VERSION_COMPATIBILITY_DATA.items(), reverse=True))

for column, row in sorted_data.items():
    sorted_data[column] = collections.OrderedDict(sorted(row.items(), reverse=True))

# Transpose the dataframe so parents are the rows and children the columns.
df = pd.DataFrame.from_dict(sorted_data).transpose()

print(tabulate.tabulate(df, headers="keys", tablefmt="grid"))
