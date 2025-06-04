from dotenv import dotenv_values

env_var = dotenv_values("/opt/python_process/.env")

domain = env_var['app_DOMAIN']
token = env_var['app_TOKEN_1']

