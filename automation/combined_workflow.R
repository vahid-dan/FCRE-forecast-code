lake_directory <- dirname(getwd())
setwd(lake_directory)

source(file.path("automation/check_noaa_present.R"))

#noaa_ready <- check_noaa_present(lake_directory)

noaa_ready <- TRUE

if(noaa_ready){

  source(file.path("01_generate_targets.R"))

  setwd(lake_directory)

  source(file.path("02_run_inflow_forecast.R"))

  setwd(lake_directory)

  source(file.path("03_run_flarer_forecast.R"))

  setwd(lake_directory)

  source(file.path("04_visualize.R"))
}
