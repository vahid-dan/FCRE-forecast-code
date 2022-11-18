get_obs_secchi_depth <- function(obs_file,
                                 start_datetime,
                                 end_datetime,
                                 forecast_start_datetime,
                                 forecast_horizon,
                                 secchi_sd){

start_datetime <- lubridate::as_datetime(start_datetime)
if(is.na(forecast_start_datetime)){
  end_datetime <- lubridate::as_datetime(end_datetime)
  forecast_start_datetime <- end_datetime
}else{
  forecast_start_datetime <- lubridate::as_datetime(forecast_start_datetime)
  end_datetime <- forecast_start_datetime + lubridate::days(forecast_horizon)
}

full_time <- seq(start_datetime, end_datetime, by = "1 day")

obs_secchi <- readr::read_csv(obs_file, show_col_types = FALSE) %>%
  dplyr::filter(variable == "secchi") %>%
  dplyr::mutate(date = lubridate::as_date(datetime)) %>%
  dplyr::right_join(tibble::tibble(date = lubridate::as_datetime(full_time)), by = "date") %>%
  dplyr::mutate(observation = ifelse(date > lubridate::as_date(forecast_start_datetime), NA, observation)) %>%
  dplyr::arrange(date) %>%
  dplyr::select(observation) %>%
  as_vector()

obs_depth <- readr::read_csv(obs_file, show_col_types = FALSE) %>%
  dplyr::filter(variable == "depth") %>%
  dplyr::mutate(date = lubridate::as_date(datetime),
                hour = hour(datetime)) %>%
  dplyr::filter(hour == 0)  %>%
  dplyr::right_join(tibble::tibble(date = lubridate::as_datetime(full_time)), by = "date") %>%
  dplyr::mutate(observation = ifelse(date > lubridate::as_date(forecast_start_datetime), NA, observation)) %>%
  dplyr::arrange(date) %>%
  dplyr::select(observation) %>%
  as_vector()

obs_secchi <- list(obs = obs_secchi,
                   secchi_sd = secchi_sd)

return(list(obs_secchi = obs_secchi, obs_depth = obs_depth))
}
