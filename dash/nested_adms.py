import json
import os
import sys

import pandas as pd

print(os.getcwd())
sys.path.append("utils")
from read_db_table import get_climatology_table

data = get_climatology_table()

adm0_list = data.admin0name.unique()
adm1_list = data.admin1name.unique()

adm1_options_dict = dict()
for adm0 in adm0_list:
    adm1_options_dict[adm0] = list(data[['admin1name']][data.admin0name == adm0]
                              .squeeze()
                              .unique())
    
with open("../dash/adm1_options_dict.json", "w") as f:
    json.dump(adm1_options_dict, f)

adm2_options_dict = dict()
for adm1 in adm1_list:
    adm2_options_dict[adm1] = list(data[['admin2name']][data.admin1name == adm1]
                              .squeeze()
                              .unique())


with open("../dash/adm2_options_dict.json", "w") as f:
    json.dump(adm2_options_dict, f)