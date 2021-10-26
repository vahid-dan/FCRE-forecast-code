library(tidyverse)
library(lubridate)

lake_directory <- here::here()
forecast_site <- "fcre"

s3_mode <- TRUE

configuration_file <- "configure_flare.yml"

if(s3_mode){
  restart_exists <- aws.s3::object_exists(object = file.path(forecast_site, "configure_run.yml"),
                                          bucket = "restart")
  if(restart_exists){
    aws.s3::save_object(object = file.path(forecast_site, "configure_run.yml"), bucket = "restart", file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
  }
  run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
  restart_file <- basename(run_config$restart_file)
  if(!is.na(restart_file)){
    aws.s3::save_object(object = file.path(forecast_site, restart_file),
                        bucket = "forecasts",
                        file = file.path(lake_directory, "forecasts", restart_file))
    run_config$restart_file <- file.path(lake_directory, "forecasts", basename(restart_file))
  }

  restart_file <- file.path(lake_directory, "forecasts", basename(run_config$restart_file))
}else{
  run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
  if(!is.na(run_config$restart_file)){
    file.copy(from = run_config$restart_file, to = file.path(lake_directory, "forecasts"))
  }
  restart_file <- file.path(lake_directory, "forecasts", basename(run_config$restart_file))
}

target_directory <- file.path(lake_directory, "data_processed")

if(s3_mode){
  aws.s3::save_object(object = file.path(forecast_site, "fcre-targets-insitu.csv"),
                      bucket = "targets",
                      file = file.path(target_directory, "fcre-targets-insitu.csv"))
}

pdf_file <- FLAREr::plotting_general_2(file_name = restart_file,
                                     target_file = file.path(target_directory, "fcre-targets-insitu.csv"))

if(s3_mode){
  success <- aws.s3::put_object(file = pdf_file, object = file.path(forecast_site, basename(pdf_file)), bucket = "analysis")
  if(success){
    unlink(pdf_file)
  }
}

source(file.path(lake_directory, "R","manager_plot.R"))

if(run_config$forecast_horizon == 16){
  png_file_name <- manager_plot(file_name = run_config$restart_file,
                           target_file = file.path(target_directory, "fcre-targets-insitu.csv"),
                           focal_depths = c(1, 5, 8))

  if(s3_mode){
    success <- aws.s3::put_object(file = png_file_name, object = file.path(forecast_site, basename(png_file_name)), bucket = "analysis")
    if(success){
      unlink(png_file_name)
    }
  }

}

if(s3_mode){
  unlink(file.path(target_directory, "fcre-targets-insitu.csv"))
  unlink(restart_file)
}

