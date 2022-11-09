extract_secchi <- function(fname,
                        input_file_tz,
                        focal_depths){

  d <- readr::read_csv(fname,
                show_col_types = FALSE) %>%
    dplyr::filter(Reservoir == "FCR" & Site == 50) %>%
    dplyr::select(DateTime, Secchi_m) %>%
    dplyr::mutate(DateTime = lubridate::force_tz(DateTime, input_file_tz)) %>%
    dplyr::mutate(DateTime = lubridate::with_tz(DateTime, tzone = "UTC")) %>%
    dplyr::group_by(DateTime) %>%
    dplyr::summarise(secchi = mean(Secchi_m, na.rm = TRUE), .groups = 'drop') %>%
    dplyr::rename("time" = DateTime) %>%
    tidyr::pivot_longer(cols = -c(time), names_to = "variable", values_to = "observed") %>%
    dplyr::mutate(depth = NA) %>%
    dplyr::filter(!is.na(observed)) %>%
    dplyr::select(time , depth, observed, variable) %>%
    dplyr::distinct()

  return(d)
}
