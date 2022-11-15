library(tidyverse)
library(lubridate)
set.seed(100)

options(future.globals.maxSize= 891289600)

lake_directory <- here::here()


starting_index <- 1
use_s3 <- FALSE

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

sim_names <- "test_s3"
config_set_name <- "glm_aed_oxy"

config_files <- paste0("configure_flare_glm_aed.yml")

num_forecasts <- 52 * 2
#num_forecasts <- 1 #52 * 3 - 3
#num_forecasts <- 1#19 * 7 + 1
days_between_forecasts <- 7
forecast_horizon <- 16 #32
starting_date <- as_date("2020-09-25")
second_date <- as_date("2020-12-01") - days(days_between_forecasts)
#starting_date <- as_date("2018-07-20")
#second_date <- as_date("2018-07-23") #- days(days_between_forecasts)
#second_date <- as_date("2018-09-01") - days(days_between_forecasts)
#second_date <- as_date("2019-01-01") - days(days_between_forecasts)

#second_date <- as_date("2018-08-01") - days(days_between_forecasts)

start_dates <- rep(NA, num_forecasts)
start_dates[1:2] <- c(starting_date, second_date)
for(i in 3:(3 + num_forecasts)){
  start_dates[i] <- as_date(start_dates[i-1]) + days(days_between_forecasts)
}

start_dates <- as_date(start_dates)
forecast_start_dates <- start_dates + days(days_between_forecasts)
forecast_start_dates <- forecast_start_dates[-1]

configure_run_file <- "configure_aed_run.yml"

j = 1
sites <- "fcre"

#function(i, sites, lake_directory, sim_names, config_files, )

message(paste0("Running site: ", sites[j]))

##'
# Set up configurations for the data processing
config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name = config_set_name)
config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)


if(starting_index == 1){
  run_config <- yaml::read_yaml(file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  run_config$configure_flare <- config_files[j]
  run_config$sim_name <- sim_names
  yaml::write_yaml(run_config, file = file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  message("deleting existing restart file")
  if(file.exists(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))){
    unlink(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))
  }
  if(run_config$use_s3){
    FLAREr::delete_restart(sites[j], sim_names, bucket = config$s3$warm_start$bucket, endpoint = config$s3$warm_start$endpoint)
  }
}

#' Clone or pull from data repositories

FLAREr::get_git_repo(lake_directory,
                     directory = config_obs$realtime_insitu_location,
                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

FLAREr::get_git_repo(lake_directory,
                     directory = config_obs$realtime_met_station_location,
                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

FLAREr::get_git_repo(lake_directory,
                     directory = config_obs$realtime_inflow_data_location,
                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

#get_git_repo(lake_directory,
#             directory = config_obs$manual_data_location,
#             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

#' Download files from EDI

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/5/3d1866fecfb8e17dc902c76436239431",
                     file = config_obs$met_raw_obs_fname[2],
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/5/c1b1f16b8e3edbbff15444824b65fe8f",
                     file = config_obs$insitu_obs_fname[2],
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/8/336d0a27c4ae396a75f4c07c01652985",
                     file = config_obs$secchi_fname,
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/200/11/d771f5e9956304424c3bc0a39298a5ce",
                     file = config_obs$ctd_fname,
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/199/8/da174082a3d924e989d3151924f9ef98",
                     file = config_obs$nutrients_fname,
                     lake_directory)


FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e",
                     file = config_obs$inflow_raw_file1[2],
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/542/1/791ec9ca0f1cb9361fa6a03fae8dfc95",
                     file = "silica_master_df.csv",
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/551/5/38d72673295864956cccd6bbba99a1a3",
                     file = "Dissolved_CO2_CH4_Virginia_Reservoirs.csv",
                     lake_directory)


#' Clean up observed meterology

cleaned_met_file <- met_qaqc_csv(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[1]),
                                 qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$met_raw_obs_fname[2]),
                                 cleaned_met_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id,paste0("observed-met_",config_obs$site_id,".csv")),
                                 input_file_tz = "EST",
                                 nldas = NULL,
                                 site_id = config_obs$site_id)

#' Clean up observed inflow

cleaned_inflow_file <- inflow_qaqc_csv(realtime_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[1]),
                                       qaqc_file = file.path(config_obs$file_path$data_directory, config_obs$inflow_raw_file1[2]),
                                       nutrients_file = file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                       silica_file = file.path(config_obs$file_path$data_directory,  config_obs$silica_fname),
                                       co2_ch4 = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                       cleaned_inflow_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-inflow.csv")),
                                       input_file_tz = 'EST',
                                       site_id = config_obs$site_id)

#' Clean up observed insitu measurements

cleaned_insitu_file <- in_situ_qaqc_csv(insitu_obs_fname = file.path(config_obs$file_path$data_directory,config_obs$insitu_obs_fname),
                                        data_location = config_obs$file_path$data_directory,
                                        maintenance_file = file.path(config_obs$file_path$data_directory,config_obs$maintenance_file),
                                        ctd_fname = file.path(config_obs$file_path$data_directory, config_obs$ctd_fname),
                                        nutrients_fname =  file.path(config_obs$file_path$data_directory, config_obs$nutrients_fname),
                                        secchi_fname = file.path(config_obs$file_path$data_directory, config_obs$secchi_fname),
                                        ch4_fname = file.path(config_obs$file_path$data_directory, config_obs$ch4_fname),
                                        cleaned_insitu_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-insitu.csv")),
                                        lake_name_code = config_obs$site_id,
                                        config = config_obs)

FLAREr::put_targets(site_id = config_obs$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file,
                    cleaned_inflow_file,
                    use_s3 = config$run_config$use_s3,
                    config)

##` Download NOAA forecasts`

message("    Downloading NOAA data")

cycle <- "00"

if(starting_index == 1){
  config$run_config$start_datetime <- as.character(paste0(start_dates[1], " 00:00:00"))
  config$run_config$forecast_start_datetime <- as.character(paste0(start_dates[2], " 00:00:00"))
  config$run_config$forecast_horizon <- 0
  config$run_config$restart_file <- NA
  run_config <- config$run_config
  yaml::write_yaml(run_config, file = file.path(config$file_path$configuration_directory, configure_run_file))
}

#for(i in 1:1){
for(i in starting_index:length(forecast_start_dates)){

  #https_file <- "https://raw.githubusercontent.com/cayelan/FCR-GLM-AED-Forecasting/master/FCR_2013_2019GLMHistoricalRun_GLMv3beta/inputs/FCR_SSS_inflow_2013_2021_20211102_allfractions_2DOCpools.csv"
  #if(!file.exists(file.path(config$file_path$execute_directory, basename(https_file)))){
  #download.file(https_file,
  #              file.path(config$file_path$execute_directory, basename(https_file)))
  #}

  file.copy(file.path(config$file_path$data_directory,"FCR_SSS_inflow_2013_2021_20220413_allfractions_2DOCpools.csv"),
            file.path(config$file_path$execute_directory,"FCR_SSS_inflow_2013_2021_20220413_allfractions_2DOCpools.csv"))


  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)
  config <- FLAREr::get_restart_file(config, lake_directory)

  message(paste0("     Running forecast that starts on: ", config$run_config$start_datetime))

  if(config$run_config$forecast_horizon > 0){

    inflow_model_coeff <- NULL
    inflow_model_coeff$future_inflow_flow_coeff <- c(0.0010803, 0.9478724, 0.3478991)
    inflow_model_coeff$future_inflow_flow_error <- 0.00965
    inflow_model_coeff$future_inflow_temp_coeff <- c(0.20291, 0.94214, 0.04278)
    inflow_model_coeff$future_inflow_temp_error <- 0.943

    temp_flow_forecast <- forecast_inflows_outflows_arrow(inflow_obs = file.path(config$file_path$qaqc_data_directory, "fcre-targets-inflow.csv"),
                                                          obs_met_file = file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.csv"),
                                                          inflow_model = config$inflow$forecast_inflow_model,
                                                          inflow_process_uncertainty = FALSE,
                                                          inflow_model_coeff = inflow_model_coeff,
                                                          site_id = config$location$site_id,
                                                          use_s3_met = TRUE,
                                                          use_s3_inflow = use_s3,
                                                          met_bucket = config$s3$drivers$bucket,
                                                          met_endpoint = config$s3$drivers$endpoint,
                                                          inflow_bucket = config$s3$inflow_drivers$bucket,
                                                          inflow_endpoint = config$s3$inflow_drivers$endpoint,
                                                          inflow_local_directory = file.path(lake_directory, "drivers"),
                                                          forecast_start_datetime = config$run_config$forecast_start_datetime,
                                                          forecast_horizon = config$run_config$forecast_horizon)

  }

  #Need to remove the 00 ensemble member because it only goes 16-days in the future

  #pars_config <- NULL #readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$states_config_file), col_types = readr::cols())


  #Download and process observations (already done)

  met_out <- FLAREr::generate_met_files_arrow(obs_met_file = file.path(config$file_path$qaqc_data_directory, paste0("observed-met_",config$location$site_id,".csv")),
                                              out_dir = config$file_path$execute_directory,
                                              start_datetime = config$run_config$start_datetime,
                                              end_datetime = config$run_config$end_datetime,
                                              forecast_start_datetime = config$run_config$forecast_start_datetime,
                                              forecast_horizon =  config$run_config$forecast_horizon,
                                              site_id = config$location$site_id,
                                              use_s3 = TRUE,
                                              bucket = config$s3$drivers$bucket,
                                              endpoint = config$s3$drivers$endpoint,
                                              local_directory = NULL,
                                              use_forecast = TRUE,
                                              use_ler_vars = FALSE)

  if(config$model_settings$model_name == "glm_aed"){
    variables <- c("time", "FLOW", "TEMP", "SALT",
                   'OXY_oxy',
                   'CAR_dic',
                   'CAR_ch4',
                   'SIL_rsi',
                   'NIT_amm',
                   'NIT_nit',
                   'PHS_frp',
                   'OGM_doc',
                   'OGM_docr',
                   'OGM_poc',
                   'OGM_don',
                   'OGM_donr',
                   'OGM_pon',
                   'OGM_dop',
                   'OGM_dopr',
                   'OGM_pop',
                   'PHY_cyano',
                   'PHY_green',
                   'PHY_diatom')
  }else{
    variables <- c("time", "FLOW", "TEMP", "SALT")
  }

  if(config$run_config$forecast_horizon > 0){
    inflow_forecast_dir = file.path(config$inflow$forecast_inflow_model, config$location$site_id, "0", lubridate::as_date(config$run_config$forecast_start_datetime))
  }else{
    inflow_forecast_dir <- NULL
  }

  inflow_outflow_files <- FLAREr::create_inflow_outflow_files_arrow(inflow_forecast_dir = inflow_forecast_dir,
                                                                    inflow_obs = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-inflow.csv")),
                                                                    variables = variables,
                                                                    out_dir = config$file_path$execute_directory,
                                                                    start_datetime = config$run_config$start_datetime,
                                                                    end_datetime = config$run_config$end_datetime,
                                                                    forecast_start_datetime = config$run_config$forecast_start_datetime,
                                                                    forecast_horizon =  config$run_config$forecast_horizon,
                                                                    site_id = config$location$site_id,
                                                                    use_s3 = use_s3,
                                                                    bucket = config$s3$inflow_drivers$bucket,
                                                                    endpoint = config$s3$inflow_drivers$endpoint,
                                                                    local_directory = file.path(lake_directory, "drivers", inflow_forecast_dir),
                                                                    use_forecast = TRUE,
                                                                    use_ler_vars = FALSE)

  if(config$model_settings$model_name == "glm_aed"){
    inflow_outflow_files$inflow_file_name <- cbind(inflow_outflow_files$inflow_file_name, rep(file.path(config$file_path$execute_directory,"FCR_SSS_inflow_2013_2021_20220413_allfractions_2DOCpools.csv"), length(inflow_outflow_files$inflow_file_name)))
  }

  #Create observation matrix
  obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                   obs_config = obs_config,
                                   config)

  obs_secchi_depth <- get_obs_secchi_depth(obs_file = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                           start_datetime = config$run_config$start_datetime,
                                           end_datetime = config$run_config$end_datetime,
                                           forecast_start_datetime = config$run_config$forecast_start_datetime,
                                           forecast_horizon =  config$run_config$forecast_horizon)
  #obs[ ,2:dim(obs)[2], ] <- NA

  states_config <- FLAREr::generate_states_to_obs_mapping(states_config, obs_config)

  model_sd <- FLAREr::initiate_model_error(config, states_config)

  init <- FLAREr::generate_initial_conditions(states_config,
                                              obs_config,
                                              pars_config,
                                              obs,
                                              config,
                                              historical_met_error = met_out$historical_met_error)
  #Run EnKF
  da_forecast_output <- FLAREr::run_da_forecast(states_init = init$states,
                                                pars_init = init$pars,
                                                aux_states_init = init$aux_states_init,
                                                obs = obs,
                                                obs_sd = obs_config$obs_sd,
                                                model_sd = model_sd,
                                                working_directory = config$file_path$execute_directory,
                                                met_file_names = met_out$filenames,
                                                inflow_file_names = inflow_outflow_files$inflow_file_name,
                                                outflow_file_names = inflow_outflow_files$outflow_file_name,
                                                config = config,
                                                pars_config = pars_config,
                                                states_config = states_config,
                                                obs_config = obs_config,
                                                management = NULL,
                                                da_method = config$da_setup$da_method,
                                                par_fit_method = config$da_setup$par_fit_method,
                                                obs_secchi = obs_secchi_depth$obs_secchi,
                                                obs_depth = obs_secchi_depth$obs_depth)

  # Save forecast

  saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                              forecast_output_directory = config$file_path$forecast_output_directory,
                                              use_short_filename = TRUE)

  forecast_df <- FLAREr::write_forecast_arrow(da_forecast_output = da_forecast_output,
                                              use_s3 = use_s3,
                                              bucket = config$s3$forecasts_parquet$bucket,
                                              endpoint = config$s3$forecasts_parquet$endpoint,
                                              local_directory = file.path(lake_directory, "forecasts/parquet"))

  FLAREr::generate_forecast_score_arrow(targets_file = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                        forecast_df = forecast_df,
                                        use_s3 = use_s3,
                                        bucket = config$s3$scores$bucket,
                                        endpoint = config$s3$scores$endpoint,
                                        local_directory = file.path(lake_directory, "scores/parquet"))

  message("Generating plot")
  FLAREr::plotting_general_2(file_name = saved_file,
                             target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")),
                             ncore = 2,
                             obs_csv = FALSE)

  FLAREr::put_forecast(saved_file, eml_file_name = NULL, config)

  FLAREr::update_run_config(config, lake_directory, configure_run_file, saved_file, new_horizon = forecast_horizon, day_advance = days_between_forecasts)
}
