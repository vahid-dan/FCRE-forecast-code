update_run_config <- TRUE

config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_flare.yml"))
run_config <- yaml::read_yaml(config$file_path$run_config)

config$run_config <- run_config
# Set up timings
#Weather Drivers
start_datetime <- lubridate::as_datetime(config$run_config$start_datetime)
if(is.na(config$run_config$forecast_start_datetime)){
  end_datetime <- lubridate::as_datetime(config$run_config$end_datetime)
  forecast_start_datetime <- end_datetime
}else{
  forecast_start_datetime <- lubridate::as_datetime(config$run_config$forecast_start_datetime)
  end_datetime <- forecast_start_datetime + lubridate::days(config$run_config$forecast_horizon)
}
forecast_hour <- lubridate::hour(forecast_start_datetime)
if(forecast_hour < 10){forecast_hour <- paste0("0",forecast_hour)}
noaa_forecast_path <- file.path(config$file_path$noaa_directory, config$met$forecast_met_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)


forecast_files <- list.files(noaa_forecast_path, full.names = TRUE)

if(length(forecast_files) > 0){



  if(!dir.exists(config$file_path$execute_directory)){
    dir.create(config$file_path$execute_directory)
  }

  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$states_config_file), col_types = readr::cols())


  #Download and process observations (already done)

  cleaned_observations_file_long <- file.path(config$file_path$qaqc_data_directory,"observations_postQAQC_long.csv")
  cleaned_inflow_file <- file.path(config$file_path$qaqc_data_directory, "/inflow_postQAQC.csv")
  observed_met_file <- file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.nc")

  met_out <- FLAREr::generate_glm_met_files(obs_met_file = observed_met_file,
                                            out_dir = config$file_path$execute_directory,
                                            forecast_dir = noaa_forecast_path,
                                            config = config)

  met_file_names <- met_out$met_file_names
  historical_met_error <- met_out$historical_met_error

  #Inflow Drivers (already done)

  inflow_forecast_path <- file.path(config$file_path$inflow_directory, config$inflow$forecast_inflow_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)

  inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_forecast_path,
                                                                  inflow_obs = cleaned_inflow_file,
                                                                  working_directory = config$file_path$execute_directory,
                                                                  config = config,
                                                                  state_names = states_config$state_names)

  inflow_file_names <- inflow_outflow_files$inflow_file_name
  outflow_file_names <- inflow_outflow_files$outflow_file_name

  #Create observation matrix
  obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long,
                                   obs_config,
                                   config)

  states_config <- FLAREr::generate_states_to_obs_mapping(states_config, obs_config)

  model_sd <- FLAREr::initiate_model_error(config, states_config)

  init <- FLAREr::generate_initial_conditions(states_config,
                                              obs_config,
                                              pars_config,
                                              obs,
                                              config,
                                              restart_file = config$run_config$restart_file,
                                              historical_met_error = met_out$historical_met_error)
  #Run EnKF
  da_output <- FLAREr::run_da_forecast(states_init = init$states,
                                       pars_init = init$pars,
                                       aux_states_init = init$aux_states_init,
                                       obs = obs,
                                       obs_sd = obs_config$obs_sd,
                                       model_sd = model_sd,
                                       working_directory = config$file_path$execute_directory,
                                       met_file_names = met_out$filenames,
                                       inflow_file_names = inflow_file_names,
                                       outflow_file_names = outflow_file_names,
                                       config = config,
                                       pars_config = pars_config,
                                       states_config = states_config,
                                       obs_config = obs_config,
                                       management = NULL,
                                       da_method = config$da_setup$da_method,
                                       par_fit_method = config$da_setup$par_fit_method)

  # Save forecast
  saved_file <- FLAREr::write_forecast_netcdf(da_output,
                                              forecast_location = config$file_path$forecast_output_directory)

  #Create EML Metadata
  FLAREr::create_flare_metadata(file_name = saved_file,
                                da_output)

  #files <- list.files(config$file_path$execute_directory, full.names = TRUE)
  #unlist(config$$execute_location, recursive = TRUE)

  #sapply(files, unlist, recursive = TRUE)

  #unlist(config$file_path$execute_directory,recursive = TRUE)

  if(update_run_config){
    run_config$start_datetime <- run_config$forecast_start_datetime
    run_config$forecast_start_datetime <- as.character(lubridate::as_datetime(run_config$forecast_start_datetime) + lubridate::days(1))
    if(lubridate::hour(run_config$forecast_start_datetime) == 0){
      run_config$forecast_start_datetime <- paste(run_config$forecast_start_datetime, "00:00:00")
    }
    run_config$restart_file <- saved_file
    yaml::write_yaml(run_config, file = file.path(config$file_path$run_config))
  }
}else{
  if(update_run_config){
    run_config$forecast_start_datetime <- as.character(lubridate::as_date(run_config$forecast_start_datetime) + lubridate::days(1))
    yaml::write_yaml(run_config, file = file.path(config$file_path$run_config,"run_configuration.yml"))
  }
}

