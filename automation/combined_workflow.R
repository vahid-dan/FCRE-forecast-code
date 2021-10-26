library(tidyverse)
library(lubridate)
lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "fcre"
configuration_file <- "configure_flare.yml"
update_run_config <- TRUE

source(file.path("automation/check_noaa_present.R"))

noaa_ready <- check_noaa_present(lake_directory, s3_mode = TRUE, forecast_site = forecast_site, configuration_file = configuration_file)

if(noaa_ready){

  source(file.path("01_generate_targets.R"))

  setwd(lake_directory)

  source(file.path("02_run_inflow_forecast.R"))

  setwd(lake_directory)

  source(file.path("03_run_flarer_forecast.R"))

  setwd(lake_directory)

  source(file.path("04_visualize.R"))
}
