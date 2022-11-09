extract_ch4 <- function(fname,
                        input_file_tz,
                        focal_depths){

  d <- readr::read_csv(fname, guess_max = 1000000,
                       show_col_types = FALSE) %>%
    dplyr::mutate(DateTime = lubridate::as_datetime(DateTime),
                  DateTime = lubridate::force_tz(DateTime, tzone = input_file_tz),
                  DateTime = DateTime + lubridate::hours(12))%>%
    dplyr::filter(Reservoir == "FCR" & Depth_m < 10 & Site == 100) %>%
    dplyr::mutate(CH4 = ch4_umolL * 1000 * 0.001) %>%
    dplyr::select(DateTime, Depth_m,CH4,Rep) %>%
    dplyr::group_by(DateTime,Depth_m) %>%
    dplyr::summarise(CH4 = mean(CH4, na.rm = TRUE), .groups = "drop") %>%
    dplyr::rename(time = DateTime,
                  depth = Depth_m) %>%
    tidyr::pivot_longer(cols = -c(time, depth), names_to = "variable", values_to = "observed") %>%
    dplyr::mutate(method = "grab_sample") %>%
    dplyr::filter(!is.na(observed)) %>%
    dplyr::select(time, depth, observed, variable, method)

  d <- d %>% dplyr::mutate(time = lubridate::with_tz(time, tzone = "UTC"))

  if(!is.na(focal_depths)){
    d <- d %>% dplyr::filter(depth %in% focal_depths)
  }

  return(d)
}
