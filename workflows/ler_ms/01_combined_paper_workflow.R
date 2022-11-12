library(tidyverse)
library(lubridate)
set.seed(100)

readRenviron("~/.Renviron") # compatible with littler

Sys.setenv('AWS_DEFAULT_REGION' = 's3',
           'AWS_S3_ENDPOINT' = 'flare-forecast.org',
           'USE_HTTPS' = TRUE,
           'TZ' = 'UTC')

use_s3 <- TRUE
lake_directory <- here::here()
starting_index <- 1
#Pick up on 25

files.sources <- list.files(file.path(lake_directory, "R"), full.names = TRUE)
sapply(files.sources, source)

models <- c("GLM", "GOTM","Simstrat")
#models <- c("GOTM","Simstrat")
#models <- c("GLM")
config_files <- c("configure_flare_GLM.yml","configure_flare.yml","configure_flare.yml")
#config_files <- c("configure_flare.yml","configure_flare.yml")
configure_run_file <- "configure_run.yml"
config_set_name <- "ler_ms"

num_forecasts <- 52*2 #* 3 - 3
#num_forecasts <- 1#19 * 7 + 1
days_between_forecasts <- 7
forecast_horizon <- 14
starting_date <- as_date("2020-09-25")
#second_date <- as_date("2020-12-01") - days(days_between_forecasts)
#starting_date <- as_date("2018-07-20")
second_date <- as_date("2020-11-01") - days(days_between_forecasts)

start_dates <- as_date(rep(NA, num_forecasts + 1))
end_dates <- as_date(rep(NA, num_forecasts + 1))
start_dates[1] <- starting_date
end_dates[1] <- second_date
for(i in 2:(num_forecasts+1)){
  start_dates[i] <- as_date(end_dates[i-1])
  end_dates[i] <- start_dates[i] + days(days_between_forecasts)
}

j = 1
sites <- "fcre"

#function(i, sites, lake_directory, sim_names, config_files, )

message(paste0("Running site: ", sites[j]))

##'
# Set up configurations for the data processing
config_obs <- FLAREr::initialize_obs_processing(lake_directory, observation_yml = "observation_processing.yml", config_set_name = config_set_name)

#' Clone or pull from data repositories

FLAREr::get_git_repo(lake_directory,
                     directory = config_obs$realtime_insitu_location,
                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

FLAREr::get_git_repo(lake_directory,
                     directory = config_obs$realtime_met_station_location,
                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

#FLAREr::get_git_repo(lake_directory,
#                     directory = config_obs$realtime_inflow_data_location,
#                     git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

#get_git_repo(lake_directory,
#             directory = config_obs$manual_data_location,
#             git_repo = "https://github.com/FLARE-forecast/FCRE-data.git")

#' Download files from EDI

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/389/6/a5524c686e2154ec0fd0459d46a7d1eb",
                     file = config_obs$met_raw_obs_fname[2],
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/271/6/23a191c1870a5b18cbc17f2779f719cf",
                     file = config_obs$insitu_obs_fname[2],
                     lake_directory)

FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/198/8/336d0a27c4ae396a75f4c07c01652985",
                     file = config_obs$secchi_fname,
                     lake_directory)


FLAREr::get_edi_file(edi_https = "https://pasta.lternet.edu/package/data/eml/edi/202/7/f5fa5de4b49bae8373f6e7c1773b026e",
                     file = config_obs$inflow_raw_file1[2],
                     lake_directory)

#' Clean up observed meterology
cleaned_insitu_file <- in_situ_qaqc_csv(insitu_obs_fname = file.path(config_obs$file_path$data_directory,config_obs$insitu_obs_fname),
                                        data_location = config_obs$file_path$data_directory,
                                        maintenance_file = file.path(config_obs$file_path$data_directory,config_obs$maintenance_file),
                                        ctd_fname = NA,
                                        nutrients_fname =  NA,
                                        secchi_fname = NA,
                                        ch4_fname = NA,
                                        cleaned_insitu_file = file.path(config_obs$file_path$targets_directory, config_obs$site_id, paste0(config_obs$site_id,"-targets-insitu.csv")),
                                        lake_name_code = config_obs$site_id,
                                        config = config_obs)

##` Download NOAA forecasts`
config <- FLAREr::set_configuration(configure_run_file = configure_run_file, lake_directory = lake_directory, config_set_name = config_set_name, sim_name = models[1])

FLAREr::put_targets(site_id = config_obs$site_id,
                    cleaned_insitu_file,
                    cleaned_met_file = NA,
                    cleaned_inflow_file = NA,
                    use_s3 = config$run_config$use_s3,
                    config)

sims <- expand.grid(paste0(start_dates,"_",end_dates,"_", forecast_horizon), models)

names(sims) <- c("date","model")

sims$start_dates <- stringr::str_split_fixed(sims$date, "_", 3)[,1]
sims$end_dates <- stringr::str_split_fixed(sims$date, "_", 3)[,2]
sims$horizon <- stringr::str_split_fixed(sims$date, "_", 3)[,3]


sims <- sims |>
  mutate(model = as.character(model)) |>
  select(-date) |>
  distinct_all() |>
  arrange(start_dates)

sims$horizon[1:length(models)] <- 0


for(i in starting_index:nrow(sims)){


  message(paste0("index: ", i))
  message(paste0("     Running model: ", sims$model[i]))

  model <- sims$model[i]
  sim_names <- paste0(model)

  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name, sim_name = sim_names)

  cycle <- "00"

  if(file.exists(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))){
    unlink(file.path(lake_directory, "restart", sites[j], sim_names, configure_run_file))
    if(use_s3){
      FLAREr::delete_restart(site_id = sites[j],
                             sim_name = sim_names,
                             bucket = config$s3$warm_start$bucket,
                             endpoint = config$s3$warm_start$endpoint)
    }
  }
  run_config <- yaml::read_yaml(file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  run_config$configure_flare <- config_files[which(models == sims$model[i])]
  run_config$sim_name <- sim_names
  yaml::write_yaml(run_config, file = file.path(lake_directory, "configuration", config_set_name, configure_run_file))
  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name)
  config$run_config$start_datetime <- as.character(paste0(sims$start_dates[i], " 00:00:00"))
  config$run_config$forecast_start_datetime <- as.character(paste0(sims$end_dates[i], " 00:00:00"))
  config$run_config$forecast_horizon <- sims$horizon[i]
  if(i <= length(models)){
    config$run_config$restart_file <- NA
  }else{
    config$run_config$restart_file <- paste0(config$location$site_id, "-", lubridate::as_date(config$run_config$start_datetime), "-", sim_names, ".nc")
    if(!file.exists(config$run_config$restart_file )){
      warning(paste0("restart file: ", config$run_config$restart_file, " doesn't exist"))
    }
  }
  run_config <- config$run_config
  yaml::write_yaml(run_config, file = file.path(config$file_path$configuration_directory, configure_run_file))

  config <- FLAREr::set_configuration(configure_run_file,lake_directory, config_set_name = config_set_name, sim_name = sim_names)
  config$model_settings$model <- model
  config$run_config$sim_name <- sim_names
  config <- FLAREr::get_restart_file(config, lake_directory)


  message(paste0("     Running forecast that starts on: ", config$run_config$start_datetime))

  #pars_config <- NULL #readr::read_csv(file.path(config$file_path$configuration_directory, "FLAREr", config$model_settings$par_config_file), col_types = readr::cols())
  pars_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$par_config_file), col_types = readr::cols())
  obs_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$obs_config_file), col_types = readr::cols())
  states_config <- readr::read_csv(file.path(config$file_path$configuration_directory, config$model_settings$states_config_file), col_types = readr::cols())

  if(sims$model[i] != "GLM"){
    use_ler_vars = TRUE
  }else{
    use_ler_vars = FALSE
  }

  met_out <- FLAREr::generate_met_files_arrow(obs_met_file = NULL,
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
                                              use_ler_vars = use_ler_vars)

  met_out$filenames <- met_out$filenames[!stringr::str_detect(met_out$filenames, "31")]

  obs <- FLAREr::create_obs_matrix(cleaned_observations_file_long = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                   obs_config = obs_config,
                                   config)

  #obs[ ,2:dim(obs)[2], ] <- NA

  states_config <- FLAREr::generate_states_to_obs_mapping(states_config, obs_config)

  model_sd <- FLAREr::initiate_model_error(config, states_config)

  if(sims$model[i] != "GLM"){
    init <- FLARErLER::generate_initial_conditions_ler(states_config,
                                                       obs_config,
                                                       pars_config,
                                                       obs,
                                                       config,
                                                       historical_met_error = met_out$historical_met_error)
  }else{
    init <- FLAREr::generate_initial_conditions(states_config,
                                                obs_config,
                                                pars_config,
                                                obs,
                                                config,
                                                historical_met_error = met_out$historical_met_error)
  }
  if(model != "GLM"){ #GOTM and Simstrat have different diagnostics
    config$output_settings$diagnostics_names <- NULL
  }
  # if(model == "Simstrat"){  #Inflows doesn't work for Simstrat but inflows are not turned off for GLM with setting NULL
  inflow_file_names <- NULL
  outflow_file_names <- NULL
  # }else{
  #   inflow_file_names <- inflow_outflow_files$inflow_file_name
  #   outflow_file_names <- inflow_outflow_files$outflow_file_name
  # }
  #Run EnKF
  if(sims$model[i] != "GLM"){
    da_forecast_output <- FLARErLER::run_da_forecast_ler(states_init = init$states,
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
                                                         par_fit_method = config$da_setup$par_fit_method,
                                                         debug = FALSE)
  }else{
    config$model_settings$model_name <- "GLM"
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
                                                  management = NULL,
                                                  da_method = config$da_setup$da_method,
                                                  par_fit_method = config$da_setup$par_fit_method,
                                                  debug = FALSE)
  }

  # Save forecast
  if(sims$model[i] != "GLM"){
    #saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
    saved_file <- FLARErLER::write_forecast_netcdf_ler(da_forecast_output = da_forecast_output,
                                                       forecast_output_directory = config$file_path$forecast_output_directory,
                                                       use_short_filename = TRUE)

    forecast_df <- FLARErLER::write_forecast_arrow_ler(da_forecast_output = da_forecast_output,
                                                use_s3 = use_s3,
                                                bucket = config$s3$forecasts_parquet$bucket,
                                                endpoint = config$s3$forecasts_parquet$endpoint,
                                                local_directory = file.path(lake_directory, config$s3$forecasts_parquet$bucket))

  }else{
    saved_file <- FLAREr::write_forecast_netcdf(da_forecast_output = da_forecast_output,
                                                forecast_output_directory = config$file_path$forecast_output_directory,
                                                use_short_filename = TRUE)

   forecast_df <- FLAREr::write_forecast_arrow(da_forecast_output = da_forecast_output,
                                                use_s3 = use_s3,
                                                bucket = config$s3$forecasts_parquet$bucket,
                                                endpoint = config$s3$forecasts_parquet$endpoint,
                                                local_directory = file.path(lake_directory, config$s3$forecasts_parquet$bucket))

  }

  FLAREr::generate_forecast_score_arrow(targets_file = file.path(config$file_path$qaqc_data_directory,paste0(config$location$site_id, "-targets-insitu.csv")),
                                        forecast_df = forecast_df,
                                        use_s3 = use_s3,
                                        bucket = config$s3$scores$bucket,
                                        endpoint = config$s3$scores$endpoint,
                                        local_directory = file.path(lake_directory, config$s3$scores$bucket),
                                        variable_types = c("state","parameter"))

  message("Generating plot")
  FLAREr::plotting_general_2(file_name = saved_file,
                             target_file = file.path(config$file_path$qaqc_data_directory, paste0(config$location$site_id, "-targets-insitu.csv")),
                             ncore = 2,
                             obs_csv = FALSE)

  FLAREr::put_forecast(saved_file, eml_file_name = NULL, config)

  unlink(saved_file)

  unlink(config$run_config$restart_file)

  rm(da_forecast_output)
  gc()

  RCurl::getURL("https://hc-ping.com/4a9c9101-ab12-4f53-9736-24f9b1cffd63")

}
