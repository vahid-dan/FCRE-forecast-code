
s <- arrow::schema(
    target_id = arrow::string(),
    model_id = arrow::string(),
    pub_time = arrow::string(),
    site_id = arrow::string(),
    time = arrow::timestamp("s",timezone = "UTC"),
    variable= arrow::string(),
    mean= arrow::float64(),
    sd= arrow::float64(),
    observed= arrow::float64(),
    crps= arrow::float64(),
    logs= arrow::float64(),
    quantile02.5= arrow::float64(),
    quantile10= arrow::float64(),
    quantile90= arrow::float64(),
    quantile97.5= arrow::float64(),
    start_time = arrow::timestamp("s",timezone = "UTC"),
    horizon= arrow::int64())

df <- arrow::open_dataset("/Users/quinn/workfiles/Research/SSC_forecasting/ccc_aed/FCRE-forecast-code/scores", format = "csv", schema = s, skip = 1)

d <- df |> collect()

d |>
  filter(variable %in% c("temperature", "oxygen", "chla", "fdom")) |>
  mutate(depth = as.numeric(stringr::str_split_fixed(site_id, pattern = "-", n = 2)[,2])) |>
  filter(horizon <= 0,
         depth == 1.5) |>
  ggplot(aes(x = time, y = mean, color = factor(start_time))) +
  geom_line() +
  ggplot2::geom_ribbon(ggplot2::aes(ymin = quantile10, ymax = quantile90, color = factor(start_time), fill = factor(start_time)),
                       alpha = 0.70) +
  geom_point(aes(y = observed), color = "black") +
  facet_wrap(~variable, scale = "free")
