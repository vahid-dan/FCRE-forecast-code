renv::restore()

library(tidyverse)
library(lubridate)

lake_directory <- here::here()

s3_mode <- TRUE

if(file.exists("~/.aws")){
  warning(paste("Detected existing AWS credentials file in ~/.aws,",
                "Consider renaming these so that automated upload will work"))
}

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "rquinnthomas.com")

configuration_file <- "configure_flare.yml"
run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
forecast_site <- run_config$forecast_site

#Note: lake_directory need to be set prior to running this script
lake_directory <- here::here()

config_obs <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing","observation_processing.yml"))
#Note: lake_directory need to be set prior to running this script
config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr",configuration_file))
config$file_path$qaqc_data_directory <- file.path(lake_directory, "data_processed")
config$file_path$data_directory <- file.path(lake_directory, "data_raw")
config_obs$data_location <- config$file_path$data_directory
config$file_path$noaa_directory <- file.path(dirname(lake_directory), "drivers", "noaa")

setwd(file.path(lake_directory, "data_raw"))
if(!dir.exists(file.path(lake_directory, "data_raw", config_obs$realtime_insitu_location))){
  system(paste0("git clone --depth 1 --single-branch --branch ",config_obs$realtime_insitu_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config_obs$realtime_insitu_location))
}else{
  setwd(file.path(lake_directory, "data_raw", config_obs$realtime_insitu_location))
  system("git pull")
}

setwd(file.path(lake_directory, "data_raw"))
if(!dir.exists(file.path(lake_directory, "data_raw", config_obs$realtime_met_station_location))){
  system(paste0("git clone --depth 1 --single-branch --branch ",config_obs$realtime_met_station_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config_obs$realtime_met_station_location))
}else{
  setwd(file.path(lake_directory, "data_raw", config_obs$realtime_met_station_location))
  system("git pull")
}

setwd(file.path(lake_directory, "data_raw"))
if(!dir.exists(file.path(lake_directory, "data_raw", config_obs$realtime_inflow_data_location))){
  system(paste0("git clone --depth 1 --single-branch --branch ",config_obs$realtime_inflow_data_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config_obs$realtime_inflow_data_location))
}else{
  setwd(file.path(lake_directory, "data_raw", config_obs$realtime_inflow_data_location))
  system("git pull")
}

setwd(file.path(lake_directory, "data_raw"))
if(!dir.exists(file.path(lake_directory, "data_raw", config_obs$manual_data_location))){
  system(paste0("git clone --depth 1 --single-branch --branch ",config_obs$manual_data_location, " https://github.com/FLARE-forecast/FCRE-data.git ", config_obs$manual_data_location))
}else{
  setwd(file.path(lake_directory, "data_raw", config_obs$manual_data_location))
  system("git pull")
}

if(!file.exists(file.path(lake_directory, "data_raw", config_obs$met_raw_obs_fname[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/389/5/3d1866fecfb8e17dc902c76436239431", destfile = file.path(lake_directory, "data_raw",config_obs$manual_data_location,"/Met_final_2015_2020.csv"), method="curl")
}

if(!file.exists(file.path(lake_directory, "data_raw", config_obs$inflow_raw_file1[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e", destfile = file.path(lake_directory, "data_raw",config_obs$manual_data_location,"/inflow_for_EDI_2013_10Jan2021.csv"), method="curl")
}

if(!file.exists(file.path(lake_directory, "data_raw", config_obs$insitu_obs_fname[2]))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/271/5/c1b1f16b8e3edbbff15444824b65fe8f", destfile = file.path(lake_directory, "data_raw",config_obs$manual_data_location,"/Catwalk_cleanedEDI.csv"), method="curl")
}

if(!file.exists(file.path(lake_directory, "data_raw", config_obs$secchi_fname))){
  download.file("https://pasta.lternet.edu/package/data/eml/edi/198/8/336d0a27c4ae396a75f4c07c01652985", destfile = file.path(lake_directory, "data_raw",config_obs$manual_data_location,"/Secchi_depth_2013-2020.csv"), method="curl")
}


files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

if(is.null(config_obs$met_file)){
  met_qaqc(realtime_file = file.path(config$file_path$data_directory, config_obs$met_raw_obs_fname[1]),
           qaqc_file = file.path(config$file_path$data_directory, config_obs$met_raw_obs_fname[2]),
           cleaned_met_file_dir = config$file_path$qaqc_data_directory,
           input_file_tz = "EST",
           nldas = file.path(config$file_path$data_directory, config_obs$nldas))
}else{
  file.copy(file.path(config$file_path$data_directory,config_obs$met_file), cleaned_met_file, overwrite = TRUE)
}

cleaned_inflow_file <- paste0(config$file_path$qaqc_data_directory, "/fcre-targets-inflow.csv")

if(is.null(config_obs$inflow1_file)){
  inflow_qaqc(realtime_file = file.path(config$file_path$data_directory, config_obs$inflow_raw_file1[1]),
              qaqc_file = file.path(config$file_path$data_directory, config_obs$inflow_raw_file1[2]),
              nutrients_file = file.path(config$file_path$data_directory, config_obs$nutrients_fname),
              cleaned_inflow_file ,
              input_file_tz = 'EST')
}else{
  file.copy(file.path(config$file_path$data_directory,config_obs$inflow1_file), cleaned_inflow_file, overwrite = TRUE)
}


cleaned_observations_file_long <- paste0(config$file_path$qaqc_data_directory,
                                         "/fcre-targets-insitu.csv")

config_obs$data_location <- config$file_path$data_directory
if(is.null(config_obs$combined_obs_file)){
  in_situ_qaqc(insitu_obs_fname = file.path(config$file_path$data_directory,config_obs$insitu_obs_fname),
               data_location = config$file_path$data_directory,
               maintenance_file = file.path(config$file_path$data_directory,config_obs$maintenance_file),
               ctd_fname = file.path(config$file_path$data_directory,config_obs$ctd_fname),
               nutrients_fname =  file.path(config$file_path$data_directory, config_obs$nutrients_fname),
               secchi_fname = file.path(config$file_path$data_directory, config_obs$secchi_fname),
               cleaned_observations_file_long = cleaned_observations_file_long,
               lake_name_code = forecast_site,
               config = config_obs)
}else{
  file.copy(file.path(config$file_path$data_directory,config_obs$combined_obs_file), cleaned_observations_file_long, overwrite = TRUE)
}

if(s3_mode){
  aws.s3::put_object(file = cleaned_observations_file_long, object = file.path(forecast_site, "fcre-targets-insitu.csv"), bucket = "targets")
  aws.s3::put_object(file = cleaned_inflow_file, object = file.path(forecast_site, "fcre-targets-inflow.csv"), bucket = "targets")
  aws.s3::put_object(file = file.path(config$file_path$qaqc_data_directory, "observed-met_fcre.nc"), object = file.path(forecast_site, "observed-met_fcre.nc"), bucket = "targets")
}


