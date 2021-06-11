#Note: lake_directory need to be set prior to running this script

if(!exist(lake_directory)){
  stop("Missing lake_directory variable")
}

config <- yaml::read_yaml(file.path(lake_directory,"configuration","FLAREr","configure_flare.yml"))
run_config <- yaml::read_yaml(config$file_path$run_config)

FLAREr::plotting_general(file_name = run_config$restart_file,
                         qaqc_data_directory = config$file_path$qaqc_data_directory)

source(file.path(lake_directory, "R","manager_plot.R"))

manager_plot(file_name = run_config$restart_file,
             qaqc_data_directory = config$file_path$qaqc_data_directory,
             focal_depths = c(1, 5, 8))

