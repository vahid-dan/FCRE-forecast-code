renv::restore()

library(tidyverse)
library(lubridate)

lake_directory <- here::here()

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

configure_run_file <- "configure_run.yml"
config <- set_configuration(configure_run_file,lake_directory)

get_git_repo(lake_directory,
             directory = config$obs_config$realtime_insitu_location,
             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

get_git_repo(lake_directory,
             directory = config$obs_config$realtime_met_station_location,
             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

get_git_repo(lake_directory,
             directory = config$obs_config$realtime_inflow_data_location,
             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

get_git_repo(lake_directory,
             directory = config$obs_config$manual_data_location,
             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/5/3d1866fecfb8e17dc902c76436239431",
                         file = config$obs_config$met_raw_obs_fname[2],
                         lake_directory)

get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/5/c1b1f16b8e3edbbff15444824b65fe8f",
             file = config$obs_config$insitu_obs_fname[2],
             lake_directory)

get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/8/336d0a27c4ae396a75f4c07c01652985",
             file = config$obs_config$secchi_fname,
             lake_directory)

get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e",
             file = config$obs_config$inflow_raw_file1[2],
             lake_directory)

cleaned_met_file <- met_qaqc(realtime_file = file.path(config$file_path$data_directory, config$obs_config$met_raw_obs_fname[1]),
           qaqc_file = file.path(config$file_path$data_directory, config$obs_config$met_raw_obs_fname[2]),
           cleaned_met_file = file.path(config$file_path$qaqc_data_directory, paste0("observed-met_",config$location$site_id,".nc")),
           input_file_tz = "EST",
           nldas = file.path(config$file_path$data_directory, config$obs_config$nldas))

cleaned_inflow_file <- inflow_qaqc(realtime_file = file.path(config$file_path$data_directory, config$obs_config$inflow_raw_file1[1]),
              qaqc_file = file.path(config$file_path$data_directory, config$obs_config$inflow_raw_file1[2]),
              nutrients_file = file.path(config$file_path$data_directory, config$obs_config$nutrients_fname),
              cleaned_inflow_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id,"-targets-inflow.csv")),
              input_file_tz = 'EST')

cleaned_insitu_file <- in_situ_qaqc(insitu_obs_fname = file.path(config$file_path$data_directory,config$obs_config$insitu_obs_fname),
               data_location = config$file_path$data_directory,
               maintenance_file = file.path(config$file_path$data_directory,config$obs_config$maintenance_file),
               ctd_fname = NA,
               nutrients_fname =  NA,
               secchi_fname = file.path(config$file_path$data_directory, config$obs_config$secchi_fname),
               cleaned_insitu_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id,"-targets-insitu.csv")),
               lake_name_code = config$location$site_id,
               config = config$obs_config)

put_targets(config = config,
            cleaned_insitu_file,
            cleaned_met_file,
            cleaned_inflow_file)

