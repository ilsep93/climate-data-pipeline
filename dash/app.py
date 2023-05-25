import json
import os
import re
import sys

import pandas as pd
import plotly.express as px

from dash import Dash, Input, Output, dcc, html

sys.path.append("utils")
from read_db_table import get_climatology_table

with open("dash/adm1_options_dict.json") as adm1_options_dict:
    adm1_options_dict = json.load(adm1_options_dict)

with open("dash/adm2_options_dict.json") as adm2_options_dict:
    adm2_options_dict = json.load(adm2_options_dict)

data = get_climatology_table()
data['month'] = pd.to_datetime(data['month'], format='%m').dt.month_name()

external_stylesheets = [
    {
        "href": (
            "https://fonts.googleapis.com/css2?"
            "family=Lato:wght@400;700&display=swap"
        ),
        "rel": "stylesheet",
    },
]

app = Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div(
    children=[
        html.Div(
            children=[
                html.H1(
                    children="Subnational Climate Change in West Africa", className="header-title"
                ),
                html.P(
                    children=(
                        "Projected CHELSA climatologies for 2061-2080"
                    ),
                    className="header-description",
                ),
            ],
            className="header",
        ),

        html.Div(
            children=[
                html.Div(
                    children=[
                        html.Div(children="Country", className="menu-title"),
                        dcc.Dropdown(
                            id="adm0-dropdown",
                            options=[
                                {"label": adm0, "value": adm0}
                                for adm0 in adm1_options_dict.keys()
                            ],
                            value="Democratic Republic of Congo",
                            searchable=True,
                            clearable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="ADM1", className="menu-title"),
                        dcc.Dropdown(
                            id="adm1-dropdown",
                            options=[
                                {
                                    "label": "",
                                    "value": "",
                                }
                            ],
                            value="Haut-Katanga",
                            clearable=False,
                            searchable=True,
                            className="dropdown",
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(children="ADM2", className="menu-title"),
                        dcc.Dropdown(
                            id="adm2-dropdown",
                            options=[
                                {"label": "", "value": ""}
                            ],
                            value="",
                            clearable=False,
                            searchable=True,
                            className="dropdown",
                        ),
                    ]
                ),
            ],
            className="menu",
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="average-temp",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="max-temp",
                        config={"displayModeBar": False},
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    ]
)

# Set dropdown options based on selected adm0
@app.callback(
    Output('adm1-dropdown', 'options'),
    Input('adm0-dropdown', 'value'))
def set_adm1_options(selected_country):
    return [{'label': i, 'value': i} for i in adm1_options_dict[selected_country]]

@app.callback(
    Output('adm2-dropdown', 'options'),
    Input('adm1-dropdown', 'value'))
def set_adm2_options(selected_adm1):
    return [{'label': i, 'value': i} for i in adm2_options_dict[selected_adm1]]

# Set default to first option in dropdown list
@app.callback(
    Output('adm1-dropdown', 'value'),
    Input('adm1-dropdown', 'options'))
def set_adm1_value(available_options):
    return available_options[0]['value']

@app.callback(
    Output('adm2-dropdown', 'value'),
    Input('adm2-dropdown', 'options'))
def set_adm2_value(available_options):
    return available_options[0]['value']

@app.callback(
    Output("average-temp", "figure"),
    Output("max-temp", "figure"),
    Input("adm0-dropdown", "value"),
    Input("adm1-dropdown", "value"),
    Input("adm2-dropdown", "value")
)
def update_charts(adm0, adm1, adm2):
    THEME = "simple_white"

    filtered_data = data.query(
        "admin0name== @adm0 and admin1name == @adm1 and admin2name == @adm2"
    )
    filtered_data['Climatology'] = filtered_data['climatology']
    average_temp_fig = px.line(
        filtered_data,
        x="month",
        y="mean",
        color="Climatology",
        template=THEME,
        )
    
    max_temp_fig = px.line(
        filtered_data, 
        x="month",
        y="max",
        color='Climatology',
        template=THEME,
        )
    
    return average_temp_fig, max_temp_fig

if __name__ == "__main__":
    app.run_server(debug=True)