import os
import re

import pandas as pd
from sqlalchemy.orm import DeclarativeBase

from dash import Dash, dcc, html


# Read tables from Postgres
class Base(DeclarativeBase):
    pass

# Read vector locally


# Attribute join

# Create map, where user can select an ADM2 and see different climatologies for that area


zs_path = "data/ACCESS1-0_rcp45/time_series/ACCESS1-0_rcp45_yearly.csv"

data = pd.read_csv(zs_path)

data = data.query("admin2Name == 'Sakania'") #TODO: allow users to filter

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

        html.H1(children="West Africa Climate Change Analytics", className="header-title"),

        html.P(

            children=(
                "Subnational Climate Change Projections"
                " by month between 2061-2080"
            ),
            className="header-description"
        ),
        html.Div(
            children=[
                html.Div(
                    children=dcc.Graph(
                        id="price-chart",
                        config={"displayModeBar": False},
                        figure={
                            "data": [
                                {
                                    "x": data["month"],
                                    "y": data["max"],
                                    "type": "lines",
                                    "hovertemplate": (
                                        "%{y:.2f}°C<extra></extra>"
                                    ),
                                },
                            ],
                            "layout": {
                                "title": {
                                    "text": "Maximum Projected Temperature °C",
                                    "x": 0.05,
                                    "xanchor": "left",
                                },
                                "xaxis": {"fixedrange": True},
                                "yaxis": {
                                    "ticksuffix": "°C",
                                    "fixedrange": True,
                                },
                                "colorway": ["#17b897"],
                            },
                        },
                    ),
                    className="card",
                ),
                html.Div(
                    children=dcc.Graph(
                        id="volume-chart",
                        config={"displayModeBar": False},
                        figure={
                            "data": [
                                {
                                    "x": data["month"],
                                    "y": data["mean"],
                                    "type": "lines",
                                    "hovertemplate": (
                                        "%{y:.2f}°C<extra></extra>"
                                    ),
                                },
                            ],
                            "layout": {
                                "title": {
                                    "text": "Average Projected Temperature °C",
                                    "x": 0.05,
                                    "xanchor": "left",
                                },
                                "xaxis": {"fixedrange": True},
                                "yaxis": {
                                    "ticksuffix": "°C",
                                    "fixedrange": True,
                                },
                                "colorway": ["#E12D39"],
                            },
                        },
                    ),
                    className="card",
                ),
            ],
            className="wrapper",
        ),
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)