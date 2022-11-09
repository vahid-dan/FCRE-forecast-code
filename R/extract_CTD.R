extract_CTD <- function(fname,
                        input_file_tz,
                        focal_depths,
                        config){

  d_ctd <- readr::read_csv(fname,
                col_types = list(
                  Reservoir = readr::col_character(),
                  Site = readr::col_character(),
                  Date = readr::col_datetime(format = ""),
                  Depth_m = readr::col_double(),
                  Temp_C = readr::col_double(),
                  DO_mgL = readr::col_double(),
                  Cond_uScm = readr::col_double(),
                  Spec_Cond_uScm = readr::col_double(),
                  Chla_ugL = readr::col_double(),
                  Turb_NTU = readr::col_double(),
                  pH = readr::col_double(),
                  ORP_mV = readr::col_double(),
                  PAR_umolm2s = readr::col_double(),
                  Desc_rate = readr::col_double(),
                  Flag_Temp = readr::col_integer(),
                  Flag_DO = readr::col_integer(),
                  Flag_Cond = readr::col_integer(),
                  Flag_SpecCond = readr::col_integer(),
                  Flag_Chla = readr::col_integer(),
                  Flag_Turb = readr::col_integer(),
                  Flag_pH = readr::col_integer(),
                  Flag_ORP = readr::col_integer(),
                  Flag_PAR = readr::col_integer(),
                  Flag_DescRate = readr::col_integer())) %>%
    dplyr::mutate(Date = lubridate::force_tz(Date, tzone = input_file_tz)) %>%
    dplyr::filter(Reservoir == "FCR" & Site == "50") %>%
    dplyr::select(Date, Depth_m, Temp_C, DO_mgL, Chla_ugL) %>%
    dplyr::rename("time" = Date,
           "depth" = Depth_m,
           "temperature" = Temp_C,
           "oxygen" = DO_mgL,
           "chla" = Chla_ugL) %>%
    dplyr::mutate(oxygen = oxygen * 1000/32,
           chla = config$ctd_2_exo_sensor_chla[1] + config$ctd_2_exo_sensor_chla[2] * chla,
           oxygen = config$ctd_2_exo_sensor_do[1] + config$ctd_2_exo_sensor_do[2] * oxygen) %>%
    tidyr::pivot_longer(cols = c("temperature", "oxygen", "chla"), names_to = "variable", values_to = "observed") %>%
    dplyr::mutate(method = "ctd") %>%
    dplyr::select(time , depth, observed, variable, method) %>%
    dplyr::mutate(time = lubridate::as_datetime(time, tz = "UTC"))

  if(!is.na(focal_depths)){
    d_ctd <- d_ctd %>% dplyr::filter(depth %in% focal_depths)
  }

  return(d_ctd)
}
