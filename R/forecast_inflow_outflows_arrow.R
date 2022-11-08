
forecast_inflows_outflows_arrow <- function(inflow_obs,
                                            obs_met_file,
                                            inflow_model,
                                            inflow_process_uncertainty,
                                            inflow_model_coeff,
                                            site_id,
                                            use_s3_met = TRUE,
                                            use_s3_inflow = FALSE,
                                            met_bucket = NULL,
                                            met_endpoint = NULL,
                                            met_local_directory = NULL,
                                            inflow_bucket = NULL,
                                            inflow_endpoint = NULL,
                                            inflow_local_directory = NULL,
                                            forecast_start_datetime,
                                            forecast_horizon = 0){

  lake_name_code <- site_id

  forecast_date <- lubridate::as_date(forecast_start_datetime)
  forecast_hour <- lubridate::hour(forecast_start_datetime)

  if (forecast_horizon > 0) {
    inflow_forecast_path <- file.path(inflow_model, lake_name_code, forecast_hour, forecast_date)
  }else {
    inflow_forecast_path <- NULL
  }


  if(!is.null(forecast_date)){

    if(use_s3_met){
      if(is.null(met_bucket) | is.null(met_endpoint)){
        stop("inflow forecast function needs bucket and endpoint if use_s3=TRUE")
      }
      vars <- FLAREr:::arrow_env_vars()
      forecast_dir <- arrow::s3_bucket(bucket = file.path(met_bucket, "stage2/parquet", lubridate::hour(forecast_start_datetime), forecast_date),
                                       endpoint_override =  met_endpoint)
      FLAREr:::unset_arrow_vars(vars)
    }else{
      if(is.null(met_local_directory)){
        stop("inflow forecast function needs local_directory if use_s3=FALSE")
      }
      forecast_dir <- arrow::SubTreeFileSystem$create(met_local_directory)
    }
    inflow <- readr::read_csv(inflow_obs, show_col_types = FALSE) |>
      tidyr::pivot_wider(names_from = variable, values_from = observation)

    curr_all_days <- NULL

    noaa_met <- arrow::open_dataset(forecast_dir) |>
      dplyr::filter(site_id == lake_name_code,
                    variable %in% c("air_temperature", "precipitation_flux")) |>
      dplyr::select(datetime, parameter,variable,prediction) |>
      dplyr::collect() |>
      tidyr::pivot_wider(names_from = variable, values_from = prediction) |>
      arrange(parameter, datetime)

    ensemble_members <- unique(noaa_met$parameter)

    met <- readr::read_csv(obs_met_file, show_col_types = FALSE) |>
      dplyr::filter(variable %in% c("air_temperature", "precipitation_flux")) |>
      tidyr::pivot_wider(names_from = variable, values_from = observation) |>
      dplyr::select(-site_id)

    run_date <- lubridate::as_date(noaa_met$datetime[1])
    run_cycle <- lubridate::hour(noaa_met$datetime[1])
    if(run_cycle < 10){run_cycle <- paste0("0",run_cycle)}

    run_dir <- file.path(inflow_model, lake_name_code, run_date, run_cycle)

    obs_met <- met %>%
      dplyr::filter(datetime >= noaa_met$datetime[1] - lubridate::days(1) & datetime < noaa_met$datetime[1])

    init_flow_temp <- inflow %>%
      filter(datetime == lubridate::as_date(noaa_met$datetime[1]) - lubridate::days(1))

    if(length(init_flow_temp$FLOW) == 0){
      previous_run_date <- run_date - lubridate::days(1)
      previous_end_date <- end_date - lubridate::days(1)
      previous_run_dir <- file.path(inflow_model, lake_name_code, previous_run_date, run_cycle)


      if(use_s3_inflow){
        FLAREr:::arrow_env_vars()
        inflow_s3_previous <- arrow::s3_bucket(bucket = file.path(inflow_bucket, previous_run_dir), endpoint_override = inflow_endpoint, anonymous = TRUE)
        on.exit(FLAREr:::unset_arrow_vars(vars))
      }else{
        inflow_s3_previous <- arrow::SubTreeFileSystem$create(file.path(output_dir,previous_run_dir))
      }

      df <- arrow::open_dataset(inflow_s3_previous) |>
        collect() |>
        dplyr::filter(model_id == "inflow-FLOWS-NOAAGEFS-AR1",
                      variable %in% c("FLOW", "TEMP"),
                      datetime == previous_run_date) |>
        dplyr::select(parameter, variable, prediction) |>
        tidyr::pivot_wider(names_from = variable, values_from = prediction) |>
        dplyr::arrange(parameter)

      init_flow <- df$FLOW
      init_temp <- df$TEMP

    }else{
      init_flow <- rep(init_flow_temp$FLOW[1], length(ensemble_members))
      init_temp <- rep(init_flow_temp$TEMP[1], length(ensemble_members))
    }


    d <- purrr::map_dfr(ensemble_members, function(ens, noaa_met, obs_met, init_flow, init_temp){
      df <- noaa_met |>
        dplyr::filter(parameter == ens) |>
        dplyr::select(-parameter) |>
        dplyr::bind_rows(obs_met) |>
        dplyr::arrange(datetime) |>
        dplyr::rename(AirTemp = air_temperature,
                      Rain = precipitation_flux) |>
        dplyr::mutate(AirTemp = AirTemp - 273.15,
                      Rain = Rain * (60 * 60 * 24)/1000) %>%
        dplyr::mutate(datetime = lubridate::with_tz(datetime, tzone = "UTC"),
                      datetime = datetime - lubridate::hours(lubridate::hour(datetime[1]))) %>%
        dplyr::mutate(datetime = lubridate::as_date(datetime)) %>%
        dplyr::group_by(datetime) %>%
        dplyr::summarize(Rain = mean(Rain),
                         AirTemp = mean(AirTemp),.groups = 'drop') %>%
        dplyr::mutate(ensemble = ens) %>%
        dplyr::mutate(AirTemp_lag1 = dplyr::lag(AirTemp, 1),
                      Rain_lag1 = dplyr::lag(Rain, 1)) %>%
        dplyr::slice(-1) %>%
        dplyr::mutate(FLOW = NA,
                      TEMP = NA)

      df$FLOW[1] <- init_flow[ens]
      df$TEMP[1] <- init_temp[ens]

      if(inflow_process_uncertainty == TRUE){
        inflow_error <- rnorm(nrow(df), 0, inflow_model_coeff$future_inflow_flow_error)
        temp_error <- rnorm(nrow(df), 0, inflow_model_coeff$future_inflow_temp_error)
      }else{
        inflow_error <- rep(0.0, nrow(df))
        temp_error <- rep(0.0, nrow(df))
      }

      for(i in 2:nrow(df)){
        df$FLOW[i] = inflow_model_coeff$future_inflow_flow_coeff[1] +
          inflow_model_coeff$future_inflow_flow_coeff[2] * df$FLOW[i - 1] +
          inflow_model_coeff$future_inflow_flow_coeff[3] * df$Rain_lag1[i] + inflow_error[i]
        df$TEMP[i] = inflow_model_coeff$future_inflow_temp_coeff[1] +
          inflow_model_coeff$future_inflow_temp_coeff[2] * df$TEMP[i-1] +
          inflow_model_coeff$future_inflow_temp_coeff[3] * df$AirTemp_lag1[i] + temp_error[i]
      }

      df <- df %>%
        dplyr::mutate(FLOW = ifelse(FLOW < 0.0, 0.0, FLOW))

      df <- df %>%
        dplyr::mutate(SALT = 0.0) %>%
        dplyr::select(datetime, ensemble, FLOW, TEMP, SALT, AirTemp, Rain) %>%
        dplyr::mutate_at(dplyr::vars(c("FLOW", "TEMP", "SALT")), list(~round(., 4)))

      clima_start <- lubridate::as_date("2015-07-07")
      clima_end <-  lubridate::as_date(max(obs_met$datetime))

      weir_inflow_dates <- inflow %>%
        dplyr::filter(datetime > clima_start & datetime < clima_end) %>%
        dplyr::mutate(DOY = yday(datetime)) %>%
        dplyr::select(datetime, DOY)

      #now make mean climatology
      mean_DOY_data <- inflow %>%
        dplyr::filter(datetime > clima_start & datetime < clima_end) %>%
        dplyr::mutate(DOY = yday(datetime)) %>%
        dplyr::group_by(DOY) %>%
        dplyr::summarise(across(c("CAR_dic":"PHY_diatom"),mean))

      df <- df %>%
        dplyr::mutate(DOY = yday(datetime),
                      OXY_oxy = rMR::Eq.Ox.conc(TEMP, elevation.m = 506, #creating OXY_oxy column using RMR package, assuming that oxygen is at 100% saturation in this very well-mixed stream
                                                bar.press = NULL, bar.units = NULL,
                                                out.DO.meas = "mg/L",
                                                salinity = 0, salinity.units = "pp.thou"),
                      OXY_oxy = OXY_oxy *1000*(1/32))


      df <- dplyr::left_join(df, mean_DOY_data, by = "DOY") %>%
        dplyr::select(-DOY) %>%
        dplyr::mutate(OGM_docr = -238.5586 + 4101.3976*FLOW + 2.1472*OGM_docr + (-19.1272*OGM_docr*FLOW))
      #this is my model, where I predict what stream OGM docr concentrations need to
      # be based off of my

      df_output <- df %>%
        dplyr::select(datetime,ensemble, FLOW, TEMP)

      reference_datetime <- min(df$datetime)

      df <- df |>
        tidyr::pivot_longer(-c("datetime","ensemble"), names_to = "variable", values_to = "prediction") |>
        dplyr::mutate(model_id = paste0("inflow-",basename(inflow_model)),
                      site_id = lake_name_code,
                      family = "ensemble",
                      flow_type = "inflow",
                      flow_number = 1,
                      reference_datetime = reference_datetime) |>
        dplyr::rename(parameter = ensemble) |>
        dplyr::select(model_id, site_id, reference_datetime, datetime, family, parameter, variable, prediction, flow_type, flow_number)

      df_output <- df_output |>
        tidyr::pivot_longer(-c("datetime","ensemble"), names_to = "variable", values_to = "prediction") |>
        dplyr::mutate(model_id = paste0("outflow-",basename(inflow_model)),
                      site_id = lake_name_code,
                      family = "ensemble",
                      flow_type = "outflow",
                      flow_number = 1,
                      reference_datetime = reference_datetime) |>
        dplyr::rename(parameter = ensemble) |>
        dplyr::select(model_id, site_id, reference_datetime, datetime, family, parameter, variable, prediction, flow_type, flow_number)


      combined <- dplyr::bind_rows(df, df_output)
    },
    noaa_met, obs_met, init_flow, init_temp)

    if(use_s3_inflow){
      FLAREr:::arrow_env_vars()
      inflow_s3 <- arrow::s3_bucket(bucket = file.path(inflow_bucket, inflow_forecast_path), endpoint_override = inflow_endpoint)
      on.exit(FLAREr:::unset_arrow_vars(vars))
    }else{
      inflow_s3 <- arrow::SubTreeFileSystem$create(file.path(inflow_local_directory, inflow_forecast_path))
    }

    arrow::write_dataset(d, path = inflow_s3)

    inflow_local_files <- list.files(file.path(inflow_local_directory, inflow_forecast_path), full.names = TRUE, recursive = TRUE)

  }else{
    message("nothing to forecast")
    inflow_local_files <- NULL

  }

  return(list(inflow_local_files))
}
