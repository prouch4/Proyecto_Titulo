from dotenv import dotenv_values

env_vars = dotenv_values("/opt/python_process/.env")
connection_params_time_sla = {
    "host"      : env_vars["DB_HOST"],
    "user"      : env_vars["DB_USER"],
    "password"  : env_vars["DB_PASSWORD"],
    "database"  : env_vars["DB_NAME_1"],
}