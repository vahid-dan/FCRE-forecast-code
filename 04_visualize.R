renv::restore()

library(tidyverse)
library(lubridate)

Sys.setenv("AWS_DEFAULT_REGION" = "s3",
           "AWS_S3_ENDPOINT" = "flare-forecast.org")

lake_directory <- here::here()
files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

configure_run_file <- "configure_run.yml"

config <- set_configuration(configure_run_file,lake_directory)

config <- get_restart_file(config, lake_directory)

get_targets(lake_directory, config)

pdf_file <- FLAREr::plotting_general_2(file_name = config$run_config$restart_file,
                                     target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")))

if(config$run_config$use_s3){
  success <- aws.s3::put_object(file = pdf_file, object = file.path(config$location$site_id, basename(pdf_file)), bucket = "analysis")
  if(success){
    unlink(pdf_file)
  }
}

if(config$run_con$forecast_horizon == 16){
  png_file_name <- manager_plot(file_name = config$run_config$restart_file,
                           target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")),
                           focal_depths = c(1, 5, 8))

  if(config$run_config$use_s3){
    success <- aws.s3::put_object(file = png_file_name, object = file.path(config$location$site_id, basename(png_file_name)), bucket = "analysis")
    if(success){
      unlink(png_file_name)
    }
  }

}

if(config$run_config$use_s3){
  unlink(file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")))
  unlink(config$run_config$restart_file)
}

