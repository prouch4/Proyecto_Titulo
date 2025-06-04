#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from requests import request, exceptions
from dotenv  import dotenv_values

#####################################
# SCRIPT                            #
#####################################

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

def __try_request__(url, headers, verify = False):
    
    output_var = None
    
    try:
        response = request(method = "GET", url = url, headers = headers, verify = verify)
        response.raise_for_status()
        
        if response.status_code == 200 or response.status_code == 201:
            output_var = response.json()

    except exceptions.HTTPError as http_error:
        print("HTTP error: ", http_error)
        output_var = None

    except exceptions.ConnectionError as conn_error:
        print("Connetion error: ", conn_error)
        output_var = None
    
    except exceptions.RequestException as requests_error:
        print("Request error: ", requests_error)
        output_var = None

    return output_var

def requests_to_cloudflare(url):
    base_url  = __get_dot_env__("CLOUDFLARE_DOMAIN")
    final_url = f"{base_url}{url}"

    headers = {
        'X-Auth-Email'  : __get_dot_env__("CLOUDFLARE_EMAIL"),
        'X-Auth-Key'    : __get_dot_env__("CLOUDFLARE_TOKEN"),
        'Content-Type'  : 'application/json',
    }
    
    output_var = __try_request__(final_url, headers)
    return output_var

url      = "https://10.128.40.4:55355/api/help"
url      = "https://10.128.40.4:8083/api/help"
response = request(method = "GET", url = url, verify = False, timeout = 3)
print(response)

# =========================================================================================================
