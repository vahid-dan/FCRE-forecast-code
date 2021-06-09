#Note: lake_directory need to be set prior to running this script

if(!exist(lake_directory)){
  stop("Missing lake_directory variable")
}

config_obs <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing","observation_processing.yml"))
config_flare <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_flare.yml"))

library(tidyverse)
library(lubridate)

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

if(is.null(config_obs$met_file)){
  met_qaqc(realtime_file = file.path(config_obs$data_location, config_obs$met_raw_obs_fname[1]),
           qaqc_file = file.path(config_obs$data_location, config_obs$met_raw_obs_fname[2]),
           cleaned_met_file_dir = config_flare$file_path$qaqc_data_directory,
           input_file_tz = "EST",
           nldas = file.path(config_obs$data_location, config_obs$nldas))
}else{
  file.copy(file.path(config_obs$data_location,config_obs$met_file), cleaned_met_file, overwrite = TRUE)
}

cleaned_inflow_file <- paste0(config_flare$file_path$qaqc_data_directory, "/inflow_postQAQC.csv")

if(is.null(config_obs$inflow1_file)){
  inflow_qaqc(realtime_file = file.path(config_obs$data_location, config_obs$inflow_raw_file1[1]),
              qaqc_file = file.path(config_obs$data_location, config_obs$inflow_raw_file1[2]),
              nutrients_file = file.path(config_obs$data_location, config_obs$nutrients_fname),
              cleaned_inflow_file ,
              input_file_tz = 'EST')
}else{
  file.copy(file.path(config_obs$data_location,config_obs$inflow1_file), cleaned_inflow_file, overwrite = TRUE)
}


cleaned_observations_file_long <- paste0(config_flare$file_path$qaqc_data_directory,
                                         "/observations_postQAQC_long.csv")
if(is.null(config_obs$combined_obs_file)){
  in_situ_qaqc(insitu_obs_fname = file.path(config_obs$data_location,config_obs$insitu_obs_fname),
               data_location = config_obs$data_location,
               maintenance_file = file.path(config_obs$data_location,config_obs$maintenance_file),
               ctd_fname = file.path(config_obs$data_location,config_obs$ctd_fname),
               nutrients_fname =  file.path(config_obs$data_location, config_obs$nutrients_fname),
               secchi_fname = file.path(config_obs$data_location, config_obs$secchi_fname),
               cleaned_observations_file_long = cleaned_observations_file_long,
               lake_name_code = config_flare$location$lake_name_code,
               config = config_obs)
}else{
  file.copy(file.path(config_obs$data_location,config_obs$combined_obs_file), cleaned_observations_file_long, overwrite = TRUE)
}

file.copy(file.path(config_obs$data_location,config_obs$sss_fname), file.path(config_obs$qaqc_data_location,basename(config_obs$sss_fname)))

if(!is.null(config_obs$specified_sss_inflow_file)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_sss_inflow_file), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_sss_inflow_file)))
}
if(!is.null(config_obs$specified_sss_outflow_file)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_sss_outflow_file), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_sss_outflow_file)))
}
if(!is.null(config_obs$specified_metfile)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_metfile), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_metfile)))
}

if(!is.null(config_obs$specified_inflow1)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_inflow1), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_inflow1)))
}

if(!is.null(config_obs$specified_inflow2)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_inflow2), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_inflow2)))
}

if(!is.null(config_obs$specified_outflow1)){
  file.copy(file.path(config_obs$data_location,config_obs$specified_outflow1), file.path(config_flare$file_path$qaqc_data_directory,basename(config_obs$specified_outflow1)))
}

