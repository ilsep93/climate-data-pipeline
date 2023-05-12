# Data Pipeline for High Resolution Raster Data

This project is my capstone for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 2023 cohort.

# Data

## Raster

CHELSA (Climatologies at high resolution for the earth’s land surface areas) is a very high resolution (30 arc sec, ~1km) global downscaled climate data set currently hosted by the Swiss Federal Institute for Forest, Snow and Landscape Research WSL.

I use climate data simulations carried out under CMIPS5.

Full citation:

 Karger, D.N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R.W., Zimmermann, N.E., Linder, P., Kessler, M. (2017): Climatologies at high resolution for the Earth land surface areas. Scientific Data. 4 170122. https://doi.org/10.1038/sdata.2017.122

 ## Vector

Shapefiles for [West and Central Africa](https://data.humdata.org/dataset/west-and-central-africa-administrative-boundaries-levels) were sourced from the Humanitarian Data Exchange (HDX).

I used administrative levels 1 and 2 for my analysis.

# Infrastructure as Code

I use Terraform to manage the Google Cloud resources used in this project, including Google Cloud Storage (GCS) and Big Query (BQ).

Terraform is used to create, update, and destroy resources used in this project.

# Spatial Analysis

This pipeline provides subnational estimates of climate projections. 

To achieve this goal, I calculate zonal statistics at the second administrative level using high resolution geospatial climatologies for West African countries:
* Benin
* Burkina Faso
* Cote d'Ivoire
* Cabo Verde
* Camerooon
* Central African Republic
* Chad
* Democratic Republic of the Congo
* Equatorial Guinea
* Gabon
* Gambia
* Ghana
* Guinea
* Guinea Bissau
* Liberia
* Mali
* Mauritania
* Niger
* Nigeria
* Republic of Congo
* Sao Tome and Principe
* Senegal
* Sierra Leone
* Togo

# PostgreSQL

Zonal statistics can then be ingested to Postgres, either locally or through a Docker container (see docker folder).

# TODO

## Database

- [X] Brainstorm appropriate database design from raw to processed data. This will be based on Dash requirements for time series data, and what is easiest to ingest
  - [X] Refactor raster ingestion to create a single table for all months of a given product, and allow for multiple climatologies. There will be one table per climatology.
  - [X] Use dbt to create union table of all climatologies (mostly for practice, since dbt is not strictly necessary)

## Dashboard

- [X] Create a skeleton Dash dashboard to visualize climate projections
  - [X] User selects ADM based on dropdown menu
  - [ ] Nested ADM selection (ie. user can only select adm2s that are in a given adm1)
  - [ ] Line graph shows how mean and max climate for the ADM is projected to change in 2060-2081, by month. Different line for each climatology. Each line will be labeled and have a different color. User can compare projections across climatologies
  - [ ] Create a map for all of West Africa ADM2s, with a darker color showing a greater temperature January and December
