lake_directory <- here::here()
s3_mode <- TRUE

if(file.exists("~/.aws")){
  warning(paste("Detected existing AWS credentials file in ~/.aws,",
                "Consider renaming these so that automated upload will work"))
}

Sys.setenv("AWS_DEFAULT_REGION" = "data",
           "AWS_S3_ENDPOINT" = "rquinnthomas.com")

if(!exists("update_run_config")){
  stop("Missing update_run_config variable")
}

configuration_file <- "configure_flare.yml"

#Note: lake_directory need to be set prior to running this script
config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr",configuration_file))
config$file_path$qaqc_data_directory <- file.path(lake_directory, "data_processed")
config$file_path$data_directory <- file.path(lake_directory, "data_raw")
config$file_path$noaa_directory <- file.path(dirname(lake_directory), "drivers", "noaa")
config$file_path$configuration_directory <- file.path(lake_directory, "configuration")
config$file_path$execute_directory <- file.path(lake_directory, "flare_tempdir")
config$file_path$forecast_output_directory <- file.path(dirname(lake_directory), "forecasts", forecast_site)
if(s3_mode){
  aws.s3::save_object(object = file.path(forecast_site, "configure_run.yml"), bucket = "restart", file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
}
run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
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


if(s3_mode){
  aws.s3::save_object(object = file.path(forecast_site, "fcre-targets-insitu.csv"), bucket = "targets", file = file.path(config$file_path$qaqc_data_directory, "fcre-targets-insitu.csv"))
  aws.s3::save_object(object = file.path(forecast_site, "fcre-targets-inflow.csv"), bucket = "targets", file = file.path(config$file_path$qaqc_data_directory, "fcre-targets-inflow.csv"))
  aws.s3::save_object(object = file.path(forecast_site, "/observed-met_fcre.csv"), bucket = "targets", file = file.path(config$file_path$qaqc_data_directory, "observed-met_fcre.csv"))

  noaa_files = aws.s3::get_bucket(bucket = "drivers", prefix = file.path("noaa", config$met$forecast_met_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour))
  noaa_forecast_path <- file.path(lake_directory,"drivers/noaa", config$met$forecast_met_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)

  for(i in 1:length(noaa_files)){
    aws.s3::save_object(object = noaa_files$object[[i]],bucket = "drivers", file = file.path(lake_directory, "drivers", noaa_files$object[[i]]))
  }


}

forecast_files <- list.files(noaa_forecast_path, full.names = TRUE)

if(length(forecast_files) > 0){

  if(!dir.exists(config$file_path$execute_directory)){
    dir.create(config$file_path$execute_directory)
  }

  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$states_config_file), col_types = readr::cols())


  #Download and process observations (already done)

  cleaned_observations_file_long <- file.path(config$file_path$qaqc_data_directory,"fcre-targets-insitu.csv")
  cleaned_inflow_file <- file.path(config$file_path$qaqc_data_directory, "fcre-targets-inflow.csv")
  observed_met_file <- file.path(config$file_path$qaqc_data_directory,"observed-met_fcre.nc")

  met_out <- FLAREr::generate_glm_met_files(obs_met_file = observed_met_file,
                                            out_dir = config$file_path$execute_directory,
                                            forecast_dir = noaa_forecast_path,
                                            config = config)

  met_file_names <- met_out$met_file_names
  historical_met_error <- met_out$historical_met_error

  #Inflow Drivers (already done)

  if(config$model_settings$model_name == "glm"){

    inflow_forecast_path <- file.path(config$file_path$inflow_directory, config$inflow$forecast_inflow_model,config$location$site_id,lubridate::as_date(forecast_start_datetime),forecast_hour)

    #NEED TO DOWNLOAD FROM BUCKET

    inflow_outflow_files <- FLAREr::create_glm_inflow_outflow_files(inflow_file_dir = inflow_forecast_path,
                                                                    inflow_obs = cleaned_inflow_file,
                                                                    working_directory = config$file_path$execute_directory,
                                                                    config = config,
                                                                    state_names = states_config$state_names)

    inflow_file_names <- inflow_outflow_files$inflow_file_name
    outflow_file_names <- inflow_outflow_files$outflow_file_name

    management <- NULL

  }else if(config$model_settings$model_name == "glm_aed"){

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_weir_inflow_2013_2019_20200828_allfractions_2poolsDOC.csv"),
              file.path(config$file_path$execute_directory, "FCR_weir_inflow_2013_2019_20200624_allfractions_2poolsDOC.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_wetland_inflow_2013_2019_20200828_allfractions_2DOCpools.csv"),
              file.path(config$file_path$execute_directory, "FCR_wetland_inflow_2013_2019_20200713_allfractions_2DOCpools.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_SSS_inflow_2013_2019_20200701_allfractions_2DOCpools.csv"),
              file.path(config$file_path$execute_directory, "FCR_SSS_inflow_2013_2019_20200701_allfractions_2DOCpools.csv"))

    file.copy(file.path(config_obs$data_location, "manual-data/FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"),
              file.path(config$file_path$execute_directory, "FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"))

    file1 <- file.path(config$file_path$execute_directory, "FCR_weir_inflow_2013_2019_20200624_allfractions_2poolsDOC.csv")
    file2 <- file.path(config$file_path$execute_directory, "FCR_wetland_inflow_2013_2019_20200713_allfractions_2DOCpools.csv")
    inflow_file_names <- tibble(file1 = file1,
                                file2 = file2,
                                file3 = "sss_inflow.csv")
    outflow_file_names <- tibble(file_1 = file.path(config$file_path$execute_directory, "FCR_spillway_outflow_SUMMED_WeirWetland_2013_2019_20200615.csv"),
                                 file_2 = "sss_outflow.csv")

    management <- FLAREr::generate_oxygen_management(config = config)
  }

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
  da_forecast_output <- FLAREr::run_da_forecast(states_init = init$states,
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
                                                management,
                                                da_method = config$da_setup$da_method,
                                                par_fit_method = config$da_setup$par_fit_method)

  # Save forecast
  saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                              forecast_output_directory = config$file_path$forecast_output_directory)

  #Create EML Metadata
  eml_file_name <- FLAREr::create_flare_metadata(file_name = saved_file,
                                da_forecast_output = da_forecast_output)

  if(s3_mode){
    aws.s3::put_object(file = saved_file, object = file.path(forecast_site, basename(saved_file)), bucket = "forecasts")
    aws.s3::put_object(file = eml_file_name, object = file.path(forecast_site, basename(eml_file_name)), bucket = "forecasts")
  }


  #Clean up temp files and large objects in memory
  unlist(config$file_path$execute_directory, recursive = TRUE)

  rm(da_forecast_output)
  gc()

  if(update_run_config){
    run_config$start_datetime <- run_config$forecast_start_datetime
    run_config$forecast_start_datetime <- as.character(lubridate::as_datetime(run_config$forecast_start_datetime) + lubridate::days(1))
    if(lubridate::hour(run_config$forecast_start_datetime) == 0){
      run_config$forecast_start_datetime <- paste(run_config$forecast_start_datetime, "00:00:00")
    }
    run_config$restart_file <- saved_file
    yaml::write_yaml(run_config, file = file.path(config$file_path$run_config))
    if(s3_mode){
      aws.s3::put_object(file = saved_file, object = file.path(forecast_site, basename(saved_file)), bucket = "restart")
      #Add metdata
      aws.s3::put_object(file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"), object = file.path(forecast_site, "configure_run.yml"), bucket = "restart")
    }
  }
}else{
  if(update_run_config){
    run_config$forecast_start_datetime <- as.character(lubridate::as_date(run_config$forecast_start_datetime) + lubridate::days(1))
    yaml::write_yaml(run_config, file = file.path(config$file_path$run_config,"run_configuration.yml"))
    if(s3_mode){
      aws.s3::put_object(file = saved_file, object = file.path(forecast_site, basename(saved_file)), bucket = "restart")
      #Add metdata
      aws.s3::put_object(file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"), object = file.path(forecast_site, "configure_run.yml"), bucket = "restart")
    }
  }
}

