renv::restore()

library(tidyverse)
library(lubridate)

Sys.setenv("AWS_DEFAULT_REGION" = "s3",
           "AWS_S3_ENDPOINT" = "flare-forecast.org")

lake_directory <- here::here()
configuration_file <- "configure_run.yml"
run_config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr",configuration_file))
forecast_site <- run_config$forecast_site
sim_name <- run_config$sim_name
files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

s3_mode <- TRUE

if(s3_mode){
  restart_exists <- aws.s3::object_exists(object = file.path(forecast_site, sim_name, "configure_run.yml"),
                                          bucket = "restart")
  if(restart_exists){
    aws.s3::save_object(object = file.path(forecast_site, sim_name, "configure_run.yml"),
                        bucket = "restart",
                        file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
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
    restart_file <- file.path(lake_directory, "forecasts", run_config$restart_file)
  }
}

target_directory <- file.path(lake_directory, "data_processed")

if(s3_mode){
  aws.s3::save_object(object = file.path(forecast_site, paste0(forecast_site, "-targets-insitu.csv")),
                      bucket = "targets",
                      file = file.path(target_directory, paste0(forecast_site, "-targets-insitu.csv")))
}

pdf_file <- FLAREr::plotting_general_2(file_name = restart_file,
                                     target_file = file.path(target_directory, paste0(forecast_site, "-targets-insitu.csv")))

if(s3_mode){
  success <- aws.s3::put_object(file = pdf_file, object = file.path(forecast_site, basename(pdf_file)), bucket = "analysis")
  if(success){
    unlink(pdf_file)
  }
}

if(run_config$forecast_horizon == 16){
  png_file_name <- manager_plot(file_name = run_config$restart_file,
                           target_file = file.path(target_directory, paste0(forecast_site, "-targets-insitu.csv")),
                           focal_depths = c(1, 5, 8))

  if(s3_mode){
    success <- aws.s3::put_object(file = png_file_name, object = file.path(forecast_site, basename(png_file_name)), bucket = "analysis")
    if(success){
      unlink(png_file_name)
    }
  }

}

if(s3_mode){
  unlink(file.path(target_directory, paste0(forecast_site, "-targets-insitu.csv")))
  unlink(restart_file)
}

