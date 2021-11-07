get_run_config <- function(configure_run_file, lake_directory, config, clean_start){

  if(clean_start | !config$run_config$use_s3){
    restart_exists <- file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name, configure_run_file)
    if(!restart_exists){
      file.copy(file.path(lake_directory,"configuration","FLAREr",configure_run_file), file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name, configure_run_file))
    }
  }else if(config$run_config$use_s3){
    restart_exists <- aws.s3::object_exists(object = file.path(config$location$site_id, config$run_config$sim_name, configure_run_file), bucket = "restart")
    if(restart_exists){
      aws.s3::save_object(object = file.path(config$location$site_id, config$run_config$sim_name, configure_run_file), bucket = "restart", file = file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name, configure_run_file))
    }else{
      file.copy(file.path(lake_directory,"configuration","FLAREr",configure_run_file), file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name, configure_run_file))
    }
  }
  run_config <- yaml::read_yaml(file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name, configure_run_file))
  return(run_config)
}

get_git_repo <- function(lake_directory, directory, git_repo){
  setwd(file.path(lake_directory, "data_raw"))
  if(!dir.exists(file.path(lake_directory, "data_raw", directory))){
    system(paste("git clone --depth 1 --single-branch --branch",directory,git_repo, directory, sep = " "))
  }else{
    setwd(file.path(lake_directory, "data_raw", directory))
    system("git pull")
  }
  setwd(lake_directory)
}

get_edi_file <- function(edi_https, file, lake_directory){

  if(!file.exists(file.path(lake_directory, "data_raw", file))){
    if(!dir.exists(dirname(file.path(lake_directory, "data_raw", file)))){
      dir.create(dirname(file.path(lake_directory, "data_raw", file)))
    }
    download.file(edi_https,
                  destfile = file.path(lake_directory, "data_raw", file),
                  method="curl")
  }
}


put_targets <- function(config, cleaned_insitu_file, cleaned_met_file, cleaned_inflow_file = NA){

  if(config$run_config$use_s3){
    if(!is.na(cleaned_insitu_file)){
      aws.s3::put_object(file = cleaned_insitu_file, object = file.path(config$location$site_id, basename(cleaned_insitu_file)), bucket = "targets")
    }
    if(!is.na(cleaned_inflow_file)){
      aws.s3::put_object(file = cleaned_inflow_file, object = file.path(config$location$site_id, basename(cleaned_inflow_file)), bucket = "targets")
    }
    if(!is.na(cleaned_met_file)){
      aws.s3::put_object(file = cleaned_met_file, object = file.path(config$location$site_id, basename(cleaned_met_file)), bucket = "targets")
    }
  }
}

get_targets <- function(lake_directory, config){
  if(config$run_config$use_s3){
    download_s3_objects(lake_directory, bucket = "targets", prefix = config$location$site_id)
  }
}

get_driver_forecast_path <- function(config, forecast_model){
  if(config$run_config$forecast_horizon > 0){
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
    forecast_path <- file.path(forecast_model,
                               config$location$site_id,
                               lubridate::as_date(forecast_start_datetime),forecast_hour)
  }else{
    forecast_path <- NULL
  }
  return(forecast_path)
}

get_driver_forecast <- function(lake_directory, forecast_path){

  download_s3_objects(lake_directory,
                      bucket = "drivers",
                      prefix = forecast_path)
}

set_configuration <- function(configure_run_file, lake_directory, clean_start = FALSE){
  run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr",configure_run_file))
  config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr",run_config$configure_flare))
  config$run_config <- run_config
  config_obs <- yaml::read_yaml(file.path(lake_directory,"configuration","observation_processing", config$run_config$configure_obs))
  config$file_path$qaqc_data_directory <- file.path(lake_directory, "targets")
  config$file_path$data_directory <- file.path(lake_directory, "data_raw")
  config$file_path$noaa_directory <- file.path(lake_directory, "drivers")
  config$file_path$configuration_directory <- file.path(lake_directory, "configuration")
  config$file_path$inflow_directory <- file.path(lake_directory, "drivers")
  config$file_path$analysis_directory <- file.path(lake_directory, "analysis")
  config$file_path$run_config
  config$file_path$forecast_output_directory <- file.path(lake_directory, "forecasts", config$location$site_id)
  config$file_path$restart_directory <- file.path(lake_directory, "restart", config$location$site_id, config$run_config$sim_name)
  config_obs$data_location <- config$file_path$data_directory
  config$file_path$execute_directory <- file.path(lake_directory, "flare_tempdir", config$location$site_id, run_config$sim_name)
  if(!dir.exists(config$file_path$qaqc_data_directory)){
    dir.create(config$file_path$qaqc_data_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$forecast_output_directory)){
    dir.create(config$file_path$forecast_output_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$restart_directory)){
    dir.create(config$file_path$restart_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$analysis_directory)){
    dir.create(config$file_path$analysis_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$data_directory)){
    dir.create(config$file_path$data_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$noaa_directory)){
    dir.create(config$file_path$noaa_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$inflow_directory)){
    dir.create(config$file_path$inflow_directory, recursive = TRUE)
  }

  if(!dir.exists(config$file_path$execute_directory)){
    dir.create(config$file_path$execute_directory, recursive = TRUE)
  }

  run_config <- get_run_config(configure_run_file, lake_directory, config, clean_start = clean_start)
  config$run_config <- run_config
  config$obs_config <- config_obs

  return(config)
}

get_restart_file <- function(config, lake_directory){
  if(!is.na(config$run_config$restart_file)){
    restart_file <- basename(config$run_config$restart_file)
    if(config$run_config$use_s3){
      aws.s3::save_object(object = file.path(config$location$site_id, restart_file),
                          bucket = "forecasts",
                          file = file.path(lake_directory, "forecasts", restart_file))
    }
    config$run_config$restart_file <- file.path(lake_directory, "forecasts", restart_file)
  }
  return(config)
}

update_run_config <- function(config, lake_directory, configure_run_file, saved_file, new_horizon, day_advance = 1){
  config$run_config$start_datetime <- config$run_config$forecast_start_datetime
  if(config$run_config$forecast_horizon == 0){
    config$run_config$forecast_horizon <- new_horizon
  }
  config$run_config$forecast_start_datetime <- as.character(lubridate::as_datetime(config$run_config$forecast_start_datetime) + lubridate::days(day_advance))
  if(lubridate::hour(config$run_config$forecast_start_datetime) == 0){
    config$run_config$forecast_start_datetime <- paste(config$run_config$forecast_start_datetime, "00:00:00")
  }
  config$run_config$restart_file <- basename(saved_file)
  yaml::write_yaml(config$run_config, file = file.path(lake_directory,"restart",config$location$site_id,config$run_config$sim_name,configure_run_file))
  if(config$run_config$use_s3){
    aws.s3::put_object(file = file.path(lake_directory,"restart",config$location$site_id,config$run_config$sim_name, configure_run_file), object = file.path(config$location$site_id,config$run_config$sim_name, configure_run_file), bucket = "restart")
  }
}

put_forecast <- function(saved_file, eml_file_name, config){
  if(config$run_config$use_s3){
    success <- aws.s3::put_object(file = saved_file, object = file.path(config$location$site_id, basename(saved_file)), bucket = "forecasts")
    if(success){
      unlink(saved_file)
    }
    success <- aws.s3::put_object(file = eml_file_name, object = file.path(config$location$site_id, basename(eml_file_name)), bucket = "forecasts")
    if(success){
      unlink(eml_file_name)
    }
  }
}

download_s3_objects <- function(lake_directory, bucket, prefix){

  files <- aws.s3::get_bucket(bucket = bucket, prefix = prefix)
  keys <- vapply(files, `[[`, "", "Key", USE.NAMES = FALSE)
  empty <- grepl("/$", keys)
  keys <- keys[!empty]
  if(length(keys) > 0){
    for(i in 1:length(keys)){
      aws.s3::save_object(object = keys[i],bucket = bucket, file = file.path(lake_directory, bucket, keys[i]))
    }
  }
}

delete_restart <- function(site, sim_name){
  files <- aws.s3::get_bucket(bucket = "restart", prefix = file.path(site, sim_name))
  keys <- vapply(files, `[[`, "", "Key", USE.NAMES = FALSE)
  empty <- grepl("/$", keys)
  keys <- keys[!empty]
  if(length(keys > 0)){
    for(i in 1:length(keys)){
      aws.s3::delete_object(object = keys[i], bucket = "restart")
    }
  }
}





