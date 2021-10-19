lake_directory <- here::here()

config <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing","observation_processing.yml"))

if(!dir.exists(file.path(lake_directory, "data_raw", config$realtime_insitu_location))){
  setwd(file.path(lake_directory, "data_raw"))
  system(paste0("git clone --single-branch --branch ",config$realtime_insitu_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config$realtime_insitu_location))
}

if(!dir.exists(file.path(lake_directory, "data_raw", config$realtime_met_station_location))){
  setwd(file.path(lake_directory, "data_raw"))
  system(paste0("git clone --single-branch --branch ",config$realtime_met_station_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config$realtime_met_station_location))
}

if(!dir.exists(file.path(lake_directory, "data_raw", config$realtime_inflow_data_location))){
  setwd(file.path(lake_directory, "data_raw"))
  system(paste0("git clone --single-branch --branch ",config$realtime_inflow_data_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config$realtime_inflow_data_location))
}


if(!dir.exists(file.path(lake_directory, "data_raw", config$manual_data_location))){
  setwd(file.path(lake_directory, "data_raw"))
  system(paste0("git clone --single-branch --branch ",config$manual_data_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config$manual_data_location))
}

if(!dir.exists(file.path(lake_directory, "data_raw", config$manual_data_location, config_obs$met_raw_obs_fname[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/389/5/3d1866fecfb8e17dc902c76436239431", destfile = file.path(lake_directory, "data_raw",config$manual_data_location,"/Met_final_2015_2020.csv"), method="curl")
}

if(!dir.exists(file.path(lake_directory, "data_raw", config$manual_data_location, config_obs$inflow_raw_file1[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e", destfile = file.path(lake_directory, "data_raw",config$manual_data_location,"/inflow_for_EDI_2013_10Jan2021.csv"), method="curl")
}

if(!dir.exists(file.path(lake_directory, "data_raw", config$manual_data_location, config_obs$insitu_obs_fname[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/271/5/c1b1f16b8e3edbbff15444824b65fe8f", destfile = file.path(lake_directory, "data_raw",config$manual_data_location,"/Catwalk_cleanedEDI.csv"), method="curl")
}





