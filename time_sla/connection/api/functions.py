import requests
import connection.api.config as cf_api
from time     import sleep
from urllib3  import disable_warnings
import json

def get_url(domain, var, tn = None):

    if var == "tickets":            # 30 ultimos tickets
        url = f"https://{domain}.app.com/api/v2/tickets"
    elif var == "last_ticket":      # Trae el último ticket
        url = f"https://{domain}.app.com/api/v2/tickets?per_page=1&page=1"
    elif var == "first_ticket":     # trae el primer ticket
        url = f"https://{domain}.app.com/api/v2/tickets?per_page=1&page=1&order_type=asc"
    elif var == "ticket_specific":  
        url = f"https://{domain}.app.com/api/v2/tickets/{tn}"
    elif var == "ticket_specific_2":  
        url = f"https://{domain}.app.com/api/v2/tickets/{tn}?include=stats"
    elif var == "ticket_form_fields":  
        url = f"https://{domain}.app.com/api/v2/ticket_form_fields"
    elif var == "status":  
        url = f"https://{domain}.app.com/api/v2/tickets/filter?query=\"status:5\""
    elif var == "tickets_all": 
        url = f"https://{domain}.app.com/api/v2/tickets?filter=all_tickets&page=2>;rel=\"next\""
    elif var == "groups":  
        url = f"https://{domain}.app.com/api/v2/groups"
    elif var == "sla":  
        url = f"https://{domain}.app.com/api/v2/sla_policies"
    elif var == "object":  
        url = f"https://{domain}.app.com/api/v2/objects"
    elif var == "agents":  
        url = f"https://{domain}.app.com/api/v2/agents"
    elif var == "groups":  
        url = f"https://{domain}.app.com/api/v2/groups"
    elif var == "categories":  
        url = f"https://{domain}.app.com/api/v2/service_catalog/categories"
    elif var == "departments":  
        url = f"https://{domain}.app.com/api/v2/departments"
    elif var == "requesters":
        url = f"https://{domain}.app.com/api/v2/requesters"
    elif var == "roles":
        url = f"https://{domain}.app.com/api/v2/roles"
    elif var == "custom_end_point":
        url = f"https://{domain}.app.com/api/v2/{tn}"
    elif var == "conversations": #nuevo
        url = f"https://{domain}.app.com/api/v2/tickets/{tn}?include=conversations" #nuevo
    else:
        pass

    return url


def fresh_service_get(var, tn = None, disable_warning_msg = False): 

    payload = {}
    headers = {
        'Content-Type'  : 'application/json',
        'Authorization' : f"{cf_api.token}"
    }

    disable_warnings()
    url       =  get_url(cf_api.domain, var, tn)
    response  = requests.request("GET", url, headers=headers, data=payload, verify=False)
   
    if response.status_code != 200:

        print(f"Error al obtener los {var}: ", response.status_code)
        # Hacer algo con los tickets obtenidos 
    elif response.status_code == 200:  
        limit_warning(response, tn, disable_warning_msg)
        response = response.json()
    else:
        pass

    return response


def fresh_service_put(var, section, subsection, info, tn = None, disable_warning_msg = False):
    
    payload = json.dumps({
        f"{section}":
        {
            f"{subsection}": f"{info}"
        }
    })
    headers = {
        "Accept": "*/*",
        "User-Agent": "Thunder Client (https://www.thunderclient.com)",
        'Content-Type'  : 'application/json',
        'Authorization' : f"{cf_api.token}"
    }

    disable_warnings()
    url       =  get_url(cf_api.domain, var, tn)
    response  = requests.request("PUT", url, data=payload, headers=headers, verify=False)
   
    if response.status_code != 200:

        print(f"Error al obtener los {var}: ", response.status_code)
        # Hacer algo con los tickets obtenidos 
    elif response.status_code == 200:  
        limit_warning(response, tn, disable_warning_msg)
        response = response.json()
    else:
        pass

    return response


def limit_warning(response, responsable, disable_warning_msg = False):
    try:
        headers         = response.headers
        
        limit_remaining = headers["X-Ratelimit-Remaining"]  # Interaciones disponibles
        limit_max       = headers["X-Ratelimit-Total"]      # Interaciones maximas
        
        usage_percent   = int(1) - (int(limit_remaining) / int(limit_max))
        usage_percent   = round(usage_percent, 2)

        if usage_percent >= 0.65: # limite recomendado 45%, prisas al 65%
            if disable_warning_msg == False:
                print(f"PUNTO CRITICO DE INTERACIONES ALCANZADO, ESPERANDO 60 SEGUNDOS: limite alcanzado en {responsable}")
            sleep(60)
    except:
        print("No fue posible determinar el limite de requests, esperando 5 segundos")
        sleep(5)


