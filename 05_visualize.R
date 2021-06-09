lake_directory <- "/Users/quinn/Downloads/FCRE-forecast-code/"

config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_flare.yml"))
run_config <- yaml::read_yaml(config$file_path$run_config)

FLAREr::plotting_general(file_name = run_config$restart_file,
                        qaqc_location = config$file_path$qaqc_data_directory)

source(file.path(lake_directory, "R","manager_plot.R"))

manager_plot(file_name = run_config$restart_file,
             qaqc_location = config$file_path$qaqc_data_directory,
             focal_depths = c(1, 5, 8))

