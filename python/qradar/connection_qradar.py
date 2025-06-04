#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

import requests
import urllib.parse
from urllib3 import disable_warnings
from time import sleep

#####################################
# SCRIPT                            #
#####################################

def get_url(option, extra_params):
    url_base = "https://172.16.17.10/api"
    url = {
        'url'               : None,
        'type'              : None,
        'wait_for_status'   : False,
    }

    if option == "search_generate":
        url['url']              = f"{url_base}/ariel/searches?query_expression={extra_params}"
        url['type']             = "POST"
        url['wait_for_status']  = True

    elif option == "search_status":
        url['url']  = f"{url_base}/ariel/searches/{extra_params}"
        url['type'] = "GET"

    elif option == "search_result":
        url['url']  = f"{url_base}/ariel/searches/{extra_params}/results"
        url['type'] = "GET"

    elif option == "offenses_by_timestamp":
        url['url']  = f"{url_base}/siem/offenses?fields=id%2Cdomain_id%2Cstart_time%2Cclose_time%2Cstatus%2Cevent_count%2Clog_sources%2Crules&filter=start_time%20%3E%3D%20{extra_params}%20OR%20close_time%20is%20null&sort=-id"
        url['type'] = "GET"

    elif option == "offenses_by_ids":
        url['url']  = f"{url_base}/siem/offenses?fields=id%2Cdomain_id%2Cstart_time%2Cclose_time%2Cstatus%2Cevent_count%2Clog_sources%2Crules&filter=id%20in%20({extra_params})&sort=-id"
        url['type'] = "GET"

    elif option == "domain":
        url['url']  = f"{url_base}/config/domain_management/domains/{extra_params}"
        url['type'] = "GET"

    elif option == "rules":
        url['url']  = f"{url_base}/analytics/rules"
        url['type'] = "GET"

    elif option == "custom_get":
        url['url']  = f"{url_base}/{extra_params}"
        url['type'] = "GET"

    return url

def try_request(url_params):
    headers = {'Authorization': 'Basic cG9ydGFsZXM6QWRhcHRpdmUuMTIz'}

    try:
        response = requests.request(url_params['type'], url_params['url'], headers=headers, verify=False)
        response.raise_for_status()
        if response.status_code == 200 or response.status_code == 201:
            return response.json()

    except requests.exceptions.HTTPError as http_error:

        print("HTTP error: ", http_error)
        return None
    except requests.exceptions.ConnectionError as conn_error:

        print("Connetion error: ", conn_error)
        return None
    except requests.exceptions.RequestException as requests_error:
        
        print("Request error: ", requests_error)
        return None

def get_qradar(option, extra_params = None, pre_encode = True):

    response    = None

    if pre_encode == False:
        extra_params = urllib.parse.quote(extra_params)

    url_params  = get_url(option, extra_params)
    response    = try_request(url_params)

    if url_params['wait_for_status'] == True and response != None:
        extra_params    = response['search_id']
        url_params      = get_url("search_status", extra_params) # URL para comprobar la busqueda
        search_status   = None

        # print("ESPERANDO RESULTADO")

        loop_timer  = 0 # CONTROLADOR DE BUCLE
        loop_errors = 0 # CONTROLADOR DE BUCLE
        while search_status != "COMPLETED":
            sleep(0.5)
            loop_timer += 0.5

            response = try_request(url_params) 
            try:
                search_status = response['status'] # Estado de la busqueda
            except:
                search_status = None
                loop_errors  += 1

                if loop_timer >= 30 and loop_errors >= 3: # SALIDA FORZADA
                    break

        # print(search_status, "Recibiendo resultado")

        if search_status == "COMPLETED":
            url_params = get_url("search_result", extra_params)
            response     = try_request(url_params)
    
    return response 

def get_qradar_result(query):
    disable_warnings()

    # Generando busqueda
    continuar = False
    payload={}
    headers = {
        'Authorization': 'Basic cG9ydGFsZXM6QWRhcHRpdmUuMTIz'
    }
    
    enconde_query = urllib.parse.quote(query)
    search_url = """https://172.16.17.10/api/ariel/searches?query_expression={0}""".format(enconde_query)
    try:

        search = requests.request("POST", search_url, headers=headers, data=payload, verify=False)
        search.raise_for_status()
        if search.status_code == 200 or search.status_code == 201:
            continuar = True

    except requests.exceptions.HTTPError as http_error:

        print("HTTP error: ", http_error)
    except requests.exceptions.ConnectionError as conn_error:

        print("Connetion error: ", conn_error)
    except requests.exceptions.RequestException as requests_error:
        
        print("Request error: ", requests_error)

    if continuar == True:
        
        search = search.json()

        # BUSQUEDA DE ESTATUS
        result_url = "https://172.16.17.10/api/ariel/searches/{0}".format(search['search_id'])
        status_busqueda = ''

        while status_busqueda != "COMPLETED":
            sleep(0.5)
            try:
                result = requests.request("GET", result_url , headers=headers, data=payload, verify=False)
                result = result.json()
                status_busqueda = result['status']
            except:
                print("Error en la busqueda de estatus")
        
        # RESULTADO DE BUSQUEDA
        result_url = "https://172.16.17.10/api/ariel/searches/{0}/results".format(search['search_id'])
        try:
        
            result = requests.request("GET", result_url, headers=headers, data=payload, verify=False)
            result = result.json()
            return result
        except:
            print("Error en obtencion del resultado de busqueda")
            return None
    else:

        return None
    

disable_warnings()
# get_qradar("search_generate", "SELECT * FROM events LIMIT 1", pre_encode = False) # QUERY
# get_qradar("offenses_by_timestamp", 1692720000000) # OFENSAS
# get_qradar("offenses_by_ids", 106990)              # OFENSAS
# get_qradar("domain", 3)                            # INFORMACION DE CLIENTE
# get_qradar("rules")                                # TODAS LAS REGLAS
