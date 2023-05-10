import os
import re

import pandas as pd
from sqlalchemy.orm import DeclarativeBase

from dash import Dash, Input, Output, dcc, html


# Read tables from Postgres
class Base(DeclarativeBase):
    pass

# Read vector locally


# Attribute join

# Create map, where user can select an ADM2 and see different climatologies for that area


zs_path = "data/ACCESS1-0_rcp45/time_series/ACCESS1-0_rcp45_yearly.csv"

data = pd.read_csv(zs_path)

#data = data.query("admin2Name == 'Sakania'")

adm0_options = data["admin0Name"].sort_values().unique()
adm1_options = data["admin1Name"].sort_values().unique()
adm2_options = data["admin2Name"].sort_values().unique()


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
                        html.Div(children="adm0", className="menu-title"),
                        dcc.Dropdown(
                            id="adm0-filter",
                            options=[
                                {"label": adm0, "value": adm0}
                                for adm0 in adm0_options
                            ],
                            value="Democratic Republic of Congo",
                            clearable=False,
                            className="dropdown",
                        ),
                    ]
                ),
                html.Div(
                    children=[
                        html.Div(children="adm1", className="menu-title"),
                        dcc.Dropdown(
                            id="adm1-filter",
                            options=[
                                {
                                    "label": adm1.title(),
                                    "value": adm1,
                                }
                                for adm1 in adm1_options
                            ],
                            value="Haut-Katanga",
                            clearable=False,
                            searchable=False,
                            className="dropdown",
                        ),
                    ],
                ),
                html.Div(
                    children=[
                        html.Div(children="adm2", className="menu-title"),
                        dcc.Dropdown(
                            id="adm2-filter",
                            options=[
                                {"label": adm2, "value": adm2}
                                for adm2 in adm2_options
                            ],
                            value="Sakania",
                            clearable=False,
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

@app.callback(
    Output("average-temp", "figure"),
    Output("max-temp", "figure"),
    Input("adm0-filter", "value"),
    Input("adm1-filter", "value"),
    Input("adm2-filter", "value")
)
def update_charts(adm0, adm1, adm2):
    filtered_data = data.query(
        " admin0Name== @adm0 and admin1Name == @adm1 and admin2Name == @adm2"
    )
    average_temp_figure = {
        "data": [
            {
                "x": filtered_data["month"],
                "y": filtered_data["mean"],
                "type": "lines",
                "hovertemplate": "%{y:.2f}°C<extra></extra>",
            },
        ],
        "layout": {
            "title": {
                "text": "Average Temperature °C",
                "x": 0.05,
                "xanchor": "left",
            },
            "xaxis": {"fixedrange": True},
            "yaxis": {"ticksuffix": "°C", "fixedrange": True},
            "colorway": ["#17B897"],
        },
    }

    max_temperature_figure = {
        "data": [
            {
                "x": filtered_data["month"],
                "y": filtered_data["max"],
                "type": "lines",
                "hovertemplate": "%{y:.2f}°C<extra></extra>",
            },
        ],
        "layout": {
            "title": {"text": "Maximum Temperature °C", "x": 0.05, "xanchor": "left"},
            "xaxis": {"fixedrange": True},
            "yaxis": {"fixedrange": True},
            "colorway": ["#E12D39"],
        },
    }
    return average_temp_figure, max_temperature_figure

if __name__ == "__main__":
    app.run_server(debug=True)