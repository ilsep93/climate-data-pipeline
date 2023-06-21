# Subnational Climate Change in West Africa

I am interested in geospatial data, climate change, and inventive data visualizations that can gather the results of different experiments and climatology projections.

For this purpose, this project creates a dashboard showing climate change projections in West Africa at the subnational level (second administrative division). I originally created this project as my capstone for part of the Data.Talks Data Engineering Zoomcamp, but have continued beyond the course for continued technical development.

# Demo

https://github.com/ilsep93/climate-data-pipeline/assets/54957973/572591c2-e0ea-4a9e-87e8-639ec7f453cd

# Overview

As part of this project, I processed raster data using Python libraries like `rasterio`, `fiona`, and `geopandas` and uploaded data to a PostgreSQL database (currently hosted as a Docker container). I then used `dbt` to create an aggregate SQL table that could be used for analysis.

For visualization, I used Dash by Plotly based on a Flask framework to allow users to interact with different climatology projections.

# Data

## Raster

CHELSA (Climatologies at high resolution for the earth’s land surface areas) is a very high resolution (30 arc sec, ~1km) global downscaled climate data set currently hosted by the Swiss Federal Institute for Forest, Snow and Landscape Research WSL.

I use climate data simulations carried out under CMIPS5.

Full citation:

 Karger, D.N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R.W., Zimmermann, N.E., Linder, P., Kessler, M. (2017): Climatologies at high resolution for the Earth land surface areas. Scientific Data. 4 170122. https://doi.org/10.1038/sdata.2017.122

 ## Vector

My source for shapefiles for [West and Central Africa](https://data.humdata.org/dataset/west-and-central-africa-administrative-boundaries-levels) was the Humanitarian Data Exchange (HDX).

I used administrative levels 1 and 2 for my analysis.

# Infrastructure as Code

I use Terraform to manage the Google Cloud resources used in this project, including Google Cloud Storage (GCS) and Big Query (BQ).

I used some introductory Terraform to create, update, and destroy resources used in this project.

# Design Patterns and Clean Architecture

Lessons Learned:

* Factory design pattern to create climatology products; concrete implementation is separate from client code that creates products. There is the option to add additional scenarios and products as they become available by expanding (rather than modifying) existing code.

# TODO

## Database

- [X] Brainstorm appropriate database design from raw to processed data. This will be based on Dash requirements for time series data
- [X] Refactor raster ingestion to create a single table for all months of a given product, and allow for multiple climatologies. There will be one table per climatology.
 - [X] Use dbt to create union table of all climatologies (mostly for practice, since dbt is not strictly necessary)
 - [ ] Allow pipeline to download additional types of climate data. Pipeline currently supports temperature only.
 - [ ] Add mock database connection for unit testing

## Dashboard

- [X] Create a skeleton Dash dashboard to visualize climate projections
  - [X] User selects ADM based on dropdown menu
  - [X] Nested ADM selection (ie. user can only select adm2s that are in a given adm1)
  - [X] Line graph shows how mean and max climate for the ADM is projected to change in 2060-2081, by month. Different line for each climatology. Each line will be labeled and have a different color. User can compare projections across climatologies
  - [X] Create a map for a country with ADM2 boundaries, with a darker color showing a higher temperature. Climatology and month are hardcoded.
  - [ ] Add details about the different climatologies and their assumptions for context
  - [ ] Add new tab to generate map based on month and climatology that is specified by users
