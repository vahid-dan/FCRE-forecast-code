extract_nutrients <- function(fname,
                        input_file_tz,
                        focal_depths){

  d <- readr::read_csv(fname, guess_max = 1000000,
                show_col_types = FALSE) %>%
    dplyr::mutate(DateTime = lubridate::force_tz(DateTime, tzone = input_file_tz)) %>%
    dplyr::filter(Reservoir == "FCR" & Site == "50") %>%
    dplyr::mutate(TN = TN_ugL * 1000 * 0.001 * (1/14),
           TP = TP_ugL * 1000 * 0.001 * (1/30.97),
           NH4 = NH4_ugL * 1000 * 0.001 * (1 / 18.04),
           NO3NO2 = NO3NO2_ugL * 1000 * 0.001 * (1/62.00),
           SRP = SRP_ugL * 1000 * 0.001 * (1/95),
           DOC = DOC_mgL* 1000 * (1/12.01),
           DIC = DIC_mgL*1000*(1/52.515)) %>%
    dplyr::select(DateTime, Depth_m, TN, TP, NH4, NO3NO2, SRP, DOC, DIC) %>%
    dplyr::rename("time" = DateTime,
           "depth" = Depth_m,
           "fdom" = DOC) %>%
    tidyr::pivot_longer(cols = -c(time, depth), names_to = "variable", values_to = "observed") %>%
    dplyr::mutate(method = "grab_sample") %>%
    dplyr::filter(!is.na(observed)) %>%
    dplyr::select(time , depth, observed, variable, method)

    d <- d %>% dplyr::mutate(time = lubridate::with_tz(time, tzone = "UTC"))

  if(!is.na(focal_depths)){
    d <- d %>% dplyr::filter(depth %in% focal_depths)
  }

  return(d)
}
