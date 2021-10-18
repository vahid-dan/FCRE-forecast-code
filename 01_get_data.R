#Note: lake_directory need to be set prior to running this script
lake_directory <- getwd()

config <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing","observation_processing.yml"))

if(config$pull_from_git){
  setwd(file.path(lake_directory, "data_raw", config$realtime_met_station_location))
  system("git pull")

  setwd(file.path(lake_directory, "data_raw", config$realtime_insitu_location))
  system("git pull")

  setwd(file.path(lake_directory, "data_raw", config$realtime_inflow_data_location))
  system("git pull")

  setwd(file.path(lake_directory, "data_raw", config$manual_data_location))
  system("git pull")
}




