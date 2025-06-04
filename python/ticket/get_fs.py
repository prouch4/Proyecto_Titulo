#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Complementario, controla la comunicacion con FRESH SERVICE, todas las consultas posibles,
# ademas, herramientas para corregir los timezone y los json

#####################################
# IMPORTS NECESARIOS                #
#####################################

import requests
import pytz
from json import dumps
from datetime import datetime
from urllib3  import disable_warnings
from json     import dumps
from json     import loads
from time     import sleep
from dotenv   import dotenv_values

#####################################
# SCRIPT                            #
#####################################

disable_warnings()

def get_url_2(domain, var, tn = None):
    
    if var == "tickets":            # 30 ultimos tickets
        url = f"https://{domain}.freshservice.com/api/v2/tickets"
    elif var == "last_ticket":      # Trae el último ticket
        url = f"https://{domain}.freshservice.com/api/v2/tickets?per_page=1&page=1"
    elif var == "first_ticket":     # trae el primer ticket
        url = f"https://{domain}.freshservice.com/api/v2/tickets?per_page=1&page=1&order_type=asc"
    elif var == "ticket_specific":  
        url = f"https://{domain}.freshservice.com/api/v2/tickets/{tn}"
    elif var == "ticket_specific_2":  
        url = f"https://{domain}.freshservice.com/api/v2/tickets/{tn}?include=stats"
    elif var == "ticket_form_fields":  
        url = f"https://{domain}.freshservice.com/api/v2/ticket_form_fields"
    elif var == "status":  
        url = f"https://{domain}.freshservice.com/api/v2/tickets/filter?query=\"status:5\""
    elif var == "tickets_all": 
        url = f"https://{domain}.freshservice.com/api/v2/tickets?filter=all_tickets&page=2>;rel=\"next\""
    elif var == "groups":  
        url = f"https://{domain}.freshservice.com/api/v2/groups"
    elif var == "sla":  
        url = f"https://{domain}.freshservice.com/api/v2/sla_policies"
    elif var == "object":  
        url = f"https://{domain}.freshservice.com/api/v2/objects"
    elif var == "agents":  
        url = f"https://{domain}.freshservice.com/api/v2/agents"
    elif var == "groups":  
        url = f"https://{domain}.freshservice.com/api/v2/groups"
    elif var == "categories":  
        url = f"https://{domain}.freshservice.com/api/v2/service_catalog/categories"
    elif var == "departments":  
        url = f"https://{domain}.freshservice.com/api/v2/departments"
    elif var == "requesters":
        url = f"https://{domain}.freshservice.com/api/v2/requesters"
    elif var == "roles":
        url = f"https://{domain}.freshservice.com/api/v2/roles"
    else:
        pass

    return url

def get_url(domain, var):
    
    if var == "tickets":  # 30 ultimos tickets
        url = f"https://{domain}.freshservice.com/api/v2/tickets"
    elif var == "last_ticket":  # Trae el último ticket
        url = f"https://{domain}.freshservice.com/api/v2/tickets?per_page=1&page=1"
    elif var == "first_ticket":  # trae el primer ticket
        url = f"https://{domain}.freshservice.com/api/v2/tickets?per_page=1&page=1&order_type=asc"
    elif var == "ticket_specific":  
        url = f"https://{domain}.freshservice.com/api/v2/tickets/5949"
    elif var == "ticket_form_fields":  
        url = f"https://{domain}.freshservice.com/api/v2/ticket_form_fields"
    elif var == "status":  
        url = f"https://{domain}.freshservice.com/api/v2/tickets/filter?query=\"status:5\""
    elif var == "tickets_all": 
        url = f"https://{domain}.freshservice.com/api/v2/tickets?filter=all_tickets&page=2>;rel=\"next\""
    elif var == "groups":  
        url = f"https://{domain}.freshservice.com/api/v2/groups"
    elif var == "sla":  
        url = f"https://{domain}.freshservice.com/api/v2/sla_policies"
    elif var == "object":  
        url = f"https://{domain}.freshservice.com/api/v2/objects"
    elif var == "agents":  
        url = f"https://{domain}.freshservice.com/api/v2/agents"
    elif var == "categories":  
        url = f"https://{domain}.freshservice.com/api/v2/service_catalog/categories"
    elif var == "departments":  
        url = f"https://{domain}.freshservice.com/api/v2/departments"
    elif var == "attach":  
        url = f"https://{domain}.freshservice.com/api/v2/attachments/23000636602"
    elif var == "roles":
        url = f"https://{domain}.freshservice.com/api/v2/roles"
    elif var == "requesters":
        url = f"https://{domain}.freshservice.com/api/v2/requesters"
    else:
        pass

    return url

def fresh_service_get(var): 
    
    #Producción final (Ricardo Perez API)
    # domain          = dotenv_values("/opt/python_process/python/ticket/.env")["FRESHSERVICE_DOMAIN"]
    # authorization   = dotenv_values("/opt/python_process/python/ticket/.env")["FRESHSERVICE_TOKEN"]
    env_var       = dotenv_values("/opt/python_process/.env")
    domain        = env_var["FRESHSERVICE_DOMAIN"]
    authorization = env_var["FRESHSERVICE_TOKEN_1"]
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"{authorization}"
    }
    payload = {}

    url       =  get_url(domain, var)
    response  = requests.request("GET", url, headers=headers, data=payload, verify=False)
   
    if response.status_code != 200:
        print(f"Error al obtener los {var}: ", response.status_code)
        # Hacer algo con los tickets obtenidos
    
    elif response.status_code == 200:  
        response = response.json()
    else:
        pass

    return response

# Recibe todo el response y calcula el limite de interacciones con la API
def limit_warning(response, responsable):
    try:
        headers         = response.headers
        
        limit_remaining = headers["X-Ratelimit-Remaining"]  # Interaciones disponibles
        limit_max       = headers["X-Ratelimit-Total"]      # Interaciones maximas

        usage_percent   = int(1) - (int(limit_remaining) / int(limit_max))
        usage_percent   = round(usage_percent, 2)

        if usage_percent >= 0.45:
            print(f"PUNTO CRITICO DE INTERACIONES ALCANZADO, ESPERANDO 60 SEGUNDOS: limite alcanzado en {responsable}")
            sleep(60)
    except:
        print("No fue posible determinar el limite de requests, esperando 5 segundos")
        sleep(5)
        

def fresh_service_get_2(var, tn = None): 
    
    #Producción final (Ricardo Perez API)
    domain               = dotenv_values("/opt/python_process/python/ticket/.env")["FRESHSERVICE_DOMAIN"]
    authorization        = dotenv_values("/opt/python_process/python/ticket/.env")["FRESHSERVICE_TOKEN"]
    headers = {
        'Content-Type'  : 'application/json',
        'Authorization' : f"{authorization}"
    }
    payload = {}

    disable_warnings()
    url =  get_url_2(domain, var, tn)
    response  = requests.request("GET", url, headers=headers, data=payload, verify=False)
   
    if response.status_code != 200:

        print(f"Error al obtener los {var}: ", response.status_code)
        # Hacer algo con los tickets obtenidos 
    elif response.status_code == 200:  
        limit_warning(response, tn)
        response = response.json()
    else:
        pass

    return response

def convertir_fecha_santiago(fecha_str):
    # Convertir la cadena de fecha a un objeto datetime
    fecha = datetime.strptime(fecha_str, '%Y-%m-%dT%H:%M:%SZ')

    # Zona horaria de Santiago de Chile
    tz_santiago = pytz.timezone('Chile/Continental')

    # Ajustar la zona horaria de la fecha
    fecha_santiago = fecha.replace(tzinfo=pytz.utc).astimezone(tz_santiago)

    # Formatear la fecha en el formato deseado
    fecha_formateada = fecha_santiago.strftime('%Y-%m-%d %H:%M:%S')

    return fecha_formateada

def custom_field_corrector(tickets):

    # # Corrector de recorrido
    # if "tickets" in (list(tickets.keys())):
    #     container_var = "tickets"
    # else:
    #     container_var = "ticket"

    #     # Parche para no listas
    #     temp_container = tickets[container_var]
    #     tickets[container_var] = []
    #     tickets[container_var].append(temp_container)
    #     del temp_container # Eliminacion de duplicado

    # # Corrector de custom_fields
    # for ticket in tickets[container_var]:
    #     ticket.update(ticket.pop("custom_fields"))
    #     ticket["created_at"] = convertir_fecha_santiago(ticket["created_at"])
    #     ticket["updated_at"] = convertir_fecha_santiago(ticket["updated_at"])
    #     if "due_by" not in (list(ticket.keys())):
    #         ticket["due_by"] = "\"NULL\""
    #     else :
    #         ticket["due_by"] = convertir_fecha_santiago(ticket["due_by"])
    #     if "fr_due_by" not in (list(ticket.keys())):
    #         ticket["fr_due_by"] = "\"NULL\""
    #     else :
    #         ticket["fr_due_by"] = convertir_fecha_santiago(ticket["fr_due_by"])
        
    #     if "sla_policies_id" not in (list(ticket.keys())):
    #         ticket["sla_policies_id"] = "\"NULL\""

    # Corrector JSON
    tickets = dumps(tickets)                        # Convert to string
    tickets = tickets.replace("false", "0")         # Convert bool to 0
    tickets = tickets.replace("true", "1")          # Convert bool to 1
    tickets = tickets.replace("null", "\"NULL\"")   # Anulador de None
    tickets = loads(tickets)                        # Return to JSON
    
    return tickets

# fresh_service_get("tickets")
# fresh_service_get("last_ticket")
# fresh_service_get("first_ticket")
# fresh_service_get("ticket_specific")
# fresh_service_get("tickets_all")
# fresh_service_get("groups")
# fresh_service_get("sla")
# fresh_service_get("object")
# fresh_service_get("categories")
# fresh_service_get("departments")
# fresh_service_get("ticket_form_fields")
# fresh_service_get("status")
# fresh_service_get("agents")
# fresh_service_get("groups")

# tickets = fresh_service_get_2("ticket_specific_2", 52825)
# tickets = fresh_service_get_2("departments")
# print(dumps(tickets))
# tickets = fresh_service_get("sla")
# tickets = fresh_service_get("ticket_form_fields")
# tickets = custom_field_corrector(tickets)
# print(dumps(tickets))