from dotenv import dotenv_values

env_var = dotenv_values("/opt/python_process/.env")
connection_params = {
    "host"      : env_var["DB_HOST"],
    "user"      : env_var["DB_USER"],
    "password"  : env_var["DB_PASSWORD"],
}