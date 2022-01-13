library(tidyverse)
library(lubridate)
lake_directory <- here::here()
setwd(lake_directory)
forecast_site <- "fcre"
configure_run_file <- "configure_run.yml"
update_run_config <- TRUE
config_set_name <- "default"

Sys.setenv("AWS_DEFAULT_REGION" = "s3",
           "AWS_S3_ENDPOINT" = "flare-forecast.org")
message("Checking for NOAA forecasts")
noaa_ready <- FLAREr::check_noaa_present(lake_directory,
                                         configure_run_file,
                                         config_set_name)

if(noaa_ready){

  message("Generating targets")
  source(file.path(lake_directory,"workflows", config_set_name, "01_generate_targets.R"))

  setwd(lake_directory)

  message("Generating inflow forecast")
  source(file.path(lake_directory,"workflows", config_set_name, "02_run_inflow_forecast.R"))

  setwd(lake_directory)

  message("Generating forecast")
  source(file.path(lake_directory,"workflows", config_set_name, "03_run_flarer_forecast.R"))

  setwd(lake_directory)

  message("Generating plots")
  source(file.path(lake_directory,"workflows", config_set_name, "04_visualize.R"))
}
