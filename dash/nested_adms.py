import pandas as pd

# Example structure from documentation: https://dash.plotly.com/basic-callbacks
# all_options = {
#     'America': ['New York City', 'San Francisco', 'Cincinnati'],
#     'Canada': [u'Montréal', 'Toronto', 'Ottawa']
# }

# For now, using local dataset to create options list
df = pd.read_csv("data/ACCESS1-0_rcp45/time_series/ACCESS1-0_rcp45_yearly.csv")
data = df[['admin0Name', 'admin1Name', 'admin2Name']]
adm1_list = data.admin1Name.unique()

                              .squeeze()
                              .unique())

adm2_options_dict = dict()
for adm1 in adm1_list:
    adm2_options_dict[adm1] = list(data[['admin2Name']][data.admin1Name == adm1]
                              .squeeze()
                              .unique())

# TODO: Connect to db and pull names in case there are updates