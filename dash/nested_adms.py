import pandas as pd


# For now, using local dataset to create options list
df = pd.read_csv("data/ACCESS1-0_rcp45/time_series/ACCESS1-0_rcp45_yearly.csv", encoding='utf-8-sig')
data = df[['admin0Name', 'admin1Name', 'admin2Name']]

adm0_list = data.admin0Name.unique()
adm1_list = data.admin1Name.unique()

adm1_options_dict = dict()
for adm0 in adm0_list:
    adm1_options_dict[adm0] = list(data[['admin1Name']][data.admin0Name == adm0]
                              .squeeze()
                              .unique())
    
with open("../dash/adm1_options_dict.json", "w") as f:
    json.dump(adm1_options_dict, f)

adm2_options_dict = dict()
for adm1 in adm1_list:
    adm2_options_dict[adm1] = list(data[['admin2Name']][data.admin1Name == adm1]
                              .squeeze()
                              .unique())

# TODO: Connect to db and pull names in case there are updateswith open("../dash/adm2_options_dict.json", "w") as f:
    json.dump(adm2_options_dict, f)