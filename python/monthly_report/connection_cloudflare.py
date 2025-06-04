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


# =========================================================================================================

# "/client/v4/zones/{zone_id}/custom_certificates"
# "/client/v4/zones/{zone_id}/custom_certificates/{certificates_id}"
# "/client/v4/zones/{zone_id}/ssl/certificate_packs?status=all"

# "/client/v4/accounts"                                               # Con resultados
# "/client/v4/zones"                                                  # Con resultados
# "/client/v4/zones/{zone_id}"                                        # Con resultados
# "/client/v4/zones/{zone_id}/hold"                                   # Con resultados
# "/client/v4/zones/{zone_id}/access/apps"                            # Con resultados
# "/client/v4/zones/{zone_id}/access/identity_providers"              # Con resultados
# "/client/v4/zones/{zone_id}/settings"                               # Con resultados
# "/client/v4/zones/{zone_id}/settings/ssl"                           # Con resultados
# "/client/v4/zones/{zone_id}/settings/ssl_recommender"               # Con resultados
# "/client/v4/zones/{zone_id}/rulesets"                               # Con resultados
# "/client/v4/zones/{zone_id}/rulesets/{ruleset_id}"                  # Con resultados
# "/client/v4/zones/{zone_id}/rulesets/{ruleset_id}/versions"         # Con resultados
# "/client/v4/zones/{zone_id}/available_plans"                        # Con resultados
# "/client/v4/zones/{zone_id}/available_rate_plans"                   # Con resultados
# "/client/v4/zones/{zone_id}/keyless_certificates"                   # Con resultados
# "/client/v4/zones/{zone_id}/ssl/verification"                       # Con resultados
# "/client/v4/zones/{zone_id}/ssl/universal/settings"                 # Con resultados
# "/client/v4/zones/{zone_id}/acm/total_tls"                          # Con resultados
# "/client/v4/zones/{zone_id}/custom_pages"                           # Con resultados
# "/client/v4/accounts/{customer_id}/access/certificates"             # Con resultados
# "/client/v4/accounts/{customer_id}/access/certificates/settings"    # Con resultados
# "/client/v4/zones/{zone_id}/acm/total_tls"                          # Con resultados
# "/client/v4/zones/{zone_id}/access/certificates"                    # Con resultados

# "/client/v4/zones/{zone_id}/origin_tls_client_auth"       # Sin resultados
# "/client/v4/zones/{zone_id}/access/certificates/settings" # Sin resultados
# "/client/v4/zones/{zone_id}/access/apps/ca"               # Sin resultados 
# "/client/v4/zones/{zone_id}/access/certificates"          # Sin resultados
# "/client/v4/zones/{identifier}/subscription"              # Sin resultados
# "/client/v4/zones/{identifier}/ssl/recommendation"        # Sin resultados