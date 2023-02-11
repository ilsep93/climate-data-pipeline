# Data Pipeline for High Resolution Raster Data

This project is my capstone for the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) 2023 cohort.

# Data

CHELSA (Climatologies at high resolution for the earth’s land surface areas) is a very high resolution (30 arc sec, ~1km) global downscaled climate data set currently hosted by the Swiss Federal Institute for Forest, Snow and Landscape Research WSL.

I use climate data simulations carried out under CMIPS5.

Full citation:

 Karger, D.N., Conrad, O., Böhner, J., Kawohl, T., Kreft, H., Soria-Auza, R.W., Zimmermann, N.E., Linder, P., Kessler, M. (2017): Climatologies at high resolution for the Earth land surface areas. Scientific Data. 4 170122. https://doi.org/10.1038/sdata.2017.122

# Infrastructure as Code

I use Terraform to manage the Google Cloud resources used in this project, including Google Cloud Storage (GCS) and Big Query (BQ).

Terraform is used to create, update, and destroy resources used in this project.

# Workflow Orchestration

I use Prefect to orchestrate a downloads of CHELSA climate projection models. The pipeline is built, run, and monitored using Prefect Cloud tools.

# Spatial Analysis

This pipeline is meant to provide country and subnational estimates of climate projections to data analysts. To achieve this goal, I calculate zonal statistics of high resolution geospatial data models for several geographies and climate models.