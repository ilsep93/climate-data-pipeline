# CHELSA CMIP Future Climate Pipeline

## Overview

CHELSA (Climatologies at high resolution for the earth’s land surface areas) is a very high resolution (30 arc sec, ~1km) global downscaled climate data set currently hosted by the Swiss Federal Institute for Forest, Snow and Landscape Research WSL.

This project provides a pipeline to process CHELSA models of the Coupled Model Intercomparison Project (CMIP). Different CMIP scenarios hold various assumptions about how future economic and social developments will impact climate change. 

Given a product, scenario, and month, the pipeline downloads the necessary rasters and generates statistics for an area of interest. See TODO section for other project goals (including API access through an endpoint and providing support for statistics given a user-provided bounding box).

## Products

This pipeline supports the following products: temperature (mean daily air temperature), minimum temperature (mean daily minimum air temperature), maximum temperature (mean daily air maximum temperature), precipitation rate (units of (kg*m−2*day−1)/100)

## Model Selection

CHELSA Model selection was based on model interdependence in
ensambles. The four models from which data were taken are: CESM1-BGC run by National Center for Atmospheric Research (NCAR); CMCC-CM run by the Centro Euro-Mediterraneo per I Cambiamenti Climatici (CMCC); MIROC5 run by the University of Tokyo; and ESMMR25 run by Max Planck Institute for Meteorology (MPI-M); and ACCESS1-3, by the Commonwealth Scientific and Industrial Research Organization (CSIRO) and Bureau of Meteorology (BOM), Australia (Karger et al, 2020).

## Phases

Both CMIP5 and CMIP6 are supported through this pipeline. CMIP6 features a new start year for future scenarios (2015 for CMIP6 compared to 2006 for CMIP5), as well as updated climate models and scenarios (O’Neill, 2016).

# Processing Overview

The pipeline takes data from a CHELSA AWS S3 bucket, downloads rasters (by product, scenario, time period, and month), masks the raster given a geojson, calculates zonal statistics (mean, max, min), and uploads yearly table to Postgres database. Dbt creates scenario aggregates by product. Finally, a Plotly Dash app visualizes different scenarios for users.

The filename of each CHELSA data product follows a similar structure including the respective model used, the variable short name, the respective time variables, and the accumulation (or mean) period in the following basic format (Karger et al, 2020):

CHELSA_[short_name]_[timeperiod]_[Version].tif

For CMIP6 data:
CHELSA_[short_name]_[timeperiod]_[model] _[ssp] _[Version].tif

[Hold for pipeline DAG]

### Plotly Dash Demo

https://github.com/ilsep93/climate-data-pipeline/assets/54957973/572591c2-e0ea-4a9e-87e8-639ec7f453cd

# Usage

To use the pipeline, edit `config.json` with the following parameters:

    "root_dir": Location to save downloaded data
    "geom_path" : Path to the geometry object that will be used to extract zonal statistics
    "zonal_stats_aggregates": The types of aggregate statistics for zonal statistics. Options are "mean", "median", "min", and "max".
    "raw_raster_dir": Name of the directory where raw rasters will be downloaded
    "cropped_raster_dir": Name of the directory where cropped rasters will be saved
    "zonal_stats_dir": Name of the directory where zonal statistics will be saved
    "yearly_aggregate_dir": Name of the directory where the yearly aggregate with all monthly projections will be saved
    "product": Climatology product. Options are controlled by enumerated class.
    "scenario": Scenario to be processed. May vary by product.
    "month": Month to be processed, accepted as an integer

# Database Design

* Each product is a table with a SQLAlchemy Object Relational Mapping (ORM) representation
* Scenarios and months are appended to their product's table.
* Each month of zonal statistics is uploaded as it is ready (instead of waiting until the yearly aggregate is available).
  * A plus of this approach is that users do not have to wait for all months to be available in order to query data. A minus is that there will be more uploads (one per month rather than one per year).
  * The pipeline will check if a given scenario and month is already present in the database.
  * An assumption is that each row of a shapefile is uploaded for a given month (no partial uploads).

# Skills Practiced:

* `pydantic` validation of JSON configurations (August 2023)
* Alembic revisions to make updates to tables specified using SQLAlchemy (August 2023)
* Creating config objects where users can directly specify desired directories, rather than having to make direct code-level changes (August 2023)
* Creating Object-Relational Mapping (ORM) tables with `sqlalchemy`, including creating mixin tables with pre-defined columns that can be shared across tables (July 2023)
* Separate business logic from implementation details, and delay the the specific implementation details for as long as possible. Inspired by reading "Clean Architecture: A Craftsman's Guide to Software Structure and Design" (July 2023).
* Factory design pattern to create climatology products; concrete implementation is separate from client code that creates products. There is the option to add additional scenarios and products as they become available by expanding (rather than modifying) existing code (June 2023).
* Utilizing iterfaces to abstract inputs and outputs, reducing coupling within and between modules (June 2023)
* Plotly Dash framework, including a Flask server and dcc.Graph to create dashboard front-end (May 2023)
* dbt to create views and materilized tables, using Jinja code to iterate through tables (March 2023)
* Python packages to manipulate raster data, including `geopandas` and `rasterio` (March 2023)
* Prefect to orchestrate pipeline (February 2023)
* Docker to create network of pgadmin and a Postgres database (Feburary 2023)

# TODOs

- [X] Set attributes for pathways
- [] Log database runs in Postgres
- [] Github Actions with pytest
- [] Create Makefile for pipeline shortcuts
- [] Unit test for raster functions
- [] Provide reference period for climate projections
- [] Build an API to send formatted data based on a requested prod


# References

 Karger, D.N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R.W., Zimmermann, N.E., Linder, P., Kessler, M. (2017): Climatologies at high resolution for the Earth land surface areas. Scientific Data. 4 170122. https://doi.org/10.1038/sdata.2017.122

 Karger, D.N., Schmatz, D., Detttling, D., Zimmermann, N.E. (2020) High resolution monthly precipitation and temperature timeseries for the period 2006-2100. Scientific Data. https://doi.org/10.1038/s41597-020-00587-y

  O'Neill, B. C., Tebaldi, C., van Vuuren, D. P., Eyring, V., Friedlingstein, P., Hurtt, G., Knutti, R., Kriegler, E., Lamarque, J.-F., Lowe, J., Meehl, G. A., Moss, R., Riahi, K., and Sanderson, B. M.: The Scenario Model Intercomparison Project (ScenarioMIP) for CMIP6, Geosci. Model Dev., 9, 3461–3482, https://doi.org/10.5194/gmd-9-3461-2016, 2016.

  Shapefile source for [West and Central Africa] (https://data.humdata.org/dataset/west-and-central-africa-administrative-boundaries-levels) through the Humanitarian Data Exchange (HDX).




