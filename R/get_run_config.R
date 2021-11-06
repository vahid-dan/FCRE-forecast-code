get_run_config <- function(lake_directory, forecast_site, sim_name, s3_mode, clean_start){

  if(clean_start | !s3_mode){
    restart_exists <- file.path(lake_directory, "restart", forecast_site, sim_name, "configure_run.yml")
    if(!restart_exists){
      file.copy(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"), file.path(lake_directory, "restart", forecast_site, sim_name, "configure_run.yml"))
    }
  }else if(s3_mode){
    restart_exists <- aws.s3::object_exists(object = file.path(forecast_site, sim_name, "configure_run.yml"), bucket = "restart")
    if(restart_exists){
      aws.s3::save_object(object = file.path(forecast_site, sim_name, "configure_run.yml"), bucket = "restart", file = file.path(lake_directory,"configuration","FLAREr","configure_run.yml"))
    }else{
      file.copy(file.path(lake_directory,"configuration","FLAREr","configure_run.yml"), file.path(lake_directory, "restart", forecast_site, sim_name, "configure_run.yml"))
    }
  }

  run_config <- yaml::read_yaml(file.path(lake_directory, "restart", forecast_site, sim_name, "configure_run.yml"))

  return(run_config)
}
