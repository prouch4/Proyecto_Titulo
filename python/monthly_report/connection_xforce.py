from requests import get, post
from dotenv   import dotenv_values
from json     import loads, dumps

def __get_dot_env__(env_key = None):
    
    env_ruta    = "/opt/python_process/.env"

    if env_key == None:
        output_var  = dotenv_values(env_ruta)
    else:
        try:
            output_var  = dotenv_values(env_ruta)[env_key]
        except:
            print(f"No se encontro campo especificado: {env_key}")

    return output_var

def request_ibm_xforce(metodo, url):
    
    url     = f"{__get_dot_env__('IBMXFORCE_DOMAIN')}{url}"
    headers = {
        "Authorization" : str(__get_dot_env__("IBMXFORCE_TOKEN")),
        "accept"        : "application/json",
    }    
    
    output_var = None
    if metodo == "get":
        output_var  = get(url = url, headers = headers)
    elif metodo == "post":
        output_var  = post(url = url, headers = headers)

    if output_var.status_code not in (200, 201):
        print(f"Error {output_var.status_code}")
    elif output_var.status_code in (200, 201):
        output_var = output_var.json()
    
    return output_var