import os
import re

import pandas as pd

from dash import Dash, dcc, html

zs_path = "data/zonal_statistics/"

li = []

for root, dirs, files in os.walk(zs_path):
    for file in files:
        if file.endswith(".csv"):
            with open(f"{zs_path}/{file}", 'r') as f:
                month = re.search('_\d_2061-2080', file).group(0)
                month = month.replace("_2061-2080", "")
                month = month.replace("_", "")
                data = pd.read_csv(f, index_col=None, header=0)
                data['month'] = int(month)
                li.append(data)

data = pd.concat(li, axis=0, ignore_index=True)

data = data.query("admin2Name == 'Sakania'") #TODO: allow users to filter

app = Dash(__name__)

app.layout = html.Div(

    children=[

        html.H1(children="West Africa Climate Change Analytics"),

        html.P(

            children=(

                "Analyze sub-national climate change projections"

                " by month between 2061-2080"

            ),

        ),

        dcc.Graph(

            figure={

                "data": [

                    {

                        "x": data["month"],

                        "y": data["mean"],

                        "type": "lines",

                    },

                ],

                "layout": {"title": "Average Temperature"},

            },

        ),

        dcc.Graph(

            figure={

                "data": [

                    {

                        "x": data["month"],

                        "y": data["max"],

                        "type": "lines",

                    },

                ],

                "layout": {"title": "Max Projected Temperature"},

            },

        ),

    ]

)

if __name__ == "__main__":
    app.run_server(debug=False)