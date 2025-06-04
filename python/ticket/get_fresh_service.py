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

from datetime import datetime
from urllib3  import disable_warnings
from json     import dumps, loads
from time     import sleep
from dotenv   import dotenv_values

#####################################
# SCRIPT                            #
#####################################

# COMUNICACION

def get_url(domain, var, tn = None):

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
    elif var == "custom_end_point":
        url = f"https://{domain}.freshservice.com/api/v2/{tn}"
    elif var == "conversations": #nuevo
        url = f"https://{domain}.freshservice.com/api/v2/tickets/{tn}?include=conversations" #nuevo
    else:
        pass

    return url

def fresh_service_get(var, tn = None, disable_warning_msg = False): 

    env_var       = dotenv_values("/opt/python_process/.env")
    domain        = env_var["FRESHSERVICE_DOMAIN"]
    authorization = env_var["FRESHSERVICE_TOKEN_1"]

    payload = {}
    headers = {
        'Content-Type'  : 'application/json',
        'Authorization' : f"{authorization}"
    }

    disable_warnings()
    url       =  get_url(domain, var, tn)
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

# HERRAMIENTAS
def convertir_fecha_santiago(fecha_str):
    if fecha_str != None:
        # Convertir la cadena de fecha a un objeto datetime
        fecha = datetime.strptime(fecha_str, '%Y-%m-%dT%H:%M:%SZ')

        # Zona horaria de Santiago de Chile
        tz_santiago = pytz.timezone('Chile/Continental')

        # Ajustar la zona horaria de la fecha
        fecha_santiago = fecha.replace(tzinfo=pytz.utc).astimezone(tz_santiago)

        # Formatear la fecha en el formato deseado
        fecha_formateada = fecha_santiago.strftime('%Y-%m-%d %H:%M:%S')
    
        return fecha_formateada
    else:
        return fecha_str

def list_corrector(ticket):
    for keys in ticket:

        # BUSCA LAS LISTAS Y LAS SUSTITUYE
        if isinstance(ticket[keys], list):
            if ticket[keys] == []:
                ticket[keys] = None
            elif isinstance(ticket[keys][0], dict): # Descarta los dictionary dentro de listas
                pass
            else:
                ticket[keys] = ",".join(ticket[keys])
        
        # PARA FUTURAS SUSTITUCIONES
        else:
            pass

    return ticket

def custom_field_corrector(tickets):
    
    # Corrector de recorrido
    if "tickets" in (list(tickets.keys())):
        container_var = "tickets"
    else:
        container_var = "ticket"

        # Parche para no listas
        temp_container = tickets[container_var]
        tickets[container_var] = []
        tickets[container_var].append(temp_container)
        del temp_container # Eliminacion de duplicado

    # Corrector de custom_fields        
    for ticket in tickets[container_var]:
        ticket.update(ticket.pop("custom_fields"))
        
        ticket = list_corrector(ticket)

        # Cambio de Fechas Santiago
        if "created_at" not in (list(ticket.keys())):
            ticket["created_at"] = None
        else :
            ticket["created_at"] = convertir_fecha_santiago(ticket["created_at"])
            
        if "updated_at" not in (list(ticket.keys())):
            ticket["updated_at"] = None
        else :
            ticket["updated_at"] = convertir_fecha_santiago(ticket["updated_at"])
            
        if "due_by" not in (list(ticket.keys())):
            ticket["due_by"] = None
        else :
            ticket["due_by"] = convertir_fecha_santiago(ticket["due_by"])
        
        if "fr_due_by" not in (list(ticket.keys())):
            ticket["fr_due_by"] = None
        else :
            ticket["fr_due_by"] = convertir_fecha_santiago(ticket["fr_due_by"])
        
        if "fechahora_ofensa" not in (list(ticket.keys())):
            ticket["fechahora_ofensa"] = None
        else :
            ticket["fechahora_ofensa"] = convertir_fecha_santiago(ticket["fechahora_ofensa"])

        if "fecha_y_hora_contacto_cliente" not in (list(ticket.keys())):
            ticket["fecha_y_hora_contacto_cliente"] = None
        else :
            ticket["fecha_y_hora_contacto_cliente"] = convertir_fecha_santiago(ticket["fecha_y_hora_contacto_cliente"])

        # Comprobar sla_policy
        if "sla_policies_id" not in (list(ticket.keys())) and "sla_policy_id" not in (list(ticket.keys())):
            ticket["sla_policy_id"]     = None
            ticket["sla_policies_id"]   = None
        elif "sla_policy_id" in (list(ticket.keys())):
            ticket["sla_policies_id"]   = ticket["sla_policy_id"]
        elif "sla_policies_id" in (list(ticket.keys())):
            ticket["sla_policy_id"]     = ticket["sla_policies_id"]
    
    return tickets

def dictionary_field_corrector(tickets):
    
    # Corrector de custom_fields
    def for_custom(tickets, container_var):
        
        for ticket in tickets[container_var]:
            
            ticket.update(ticket.pop("custom_fields"))
            ticket = list_corrector(ticket)

            # Cambio de Fechas Santiago
            if "created_at" not in (list(ticket.keys())):
                ticket["created_at"] = None
            else :
                ticket["created_at"] = convertir_fecha_santiago(ticket["created_at"])

            if "updated_at" not in (list(ticket.keys())):
                ticket["updated_at"] = None
            else :
                ticket["updated_at"] = convertir_fecha_santiago(ticket["updated_at"])

            if "due_by" not in (list(ticket.keys())):
                ticket["due_by"] = None
            else :
                ticket["due_by"] = convertir_fecha_santiago(ticket["due_by"])
            
            if "fr_due_by" not in (list(ticket.keys())):
                ticket["fr_due_by"] = None
            else :
                ticket["fr_due_by"] = convertir_fecha_santiago(ticket["fr_due_by"])
            
            if "fechahora_ofensa" not in (list(ticket.keys())):
                ticket["fechahora_ofensa"] = None
            else :
                ticket["fechahora_ofensa"] = convertir_fecha_santiago(ticket["fechahora_ofensa"])

            if "fecha_y_hora_contacto_cliente" not in (list(ticket.keys())):
                ticket["fecha_y_hora_contacto_cliente"] = None
            else :
                ticket["fecha_y_hora_contacto_cliente"] = convertir_fecha_santiago(ticket["fecha_y_hora_contacto_cliente"])

            # Corrector SLA
            if container_var == 'ticket':
                if "sla_policies_id" not in (list(ticket.keys())) and "sla_policy_id" not in (list(ticket.keys())):
                    ticket["sla_policy_id"]     = None
                    ticket["sla_policies_id"]   = None
                elif "sla_policy_id" in (list(ticket.keys())):
                    ticket["sla_policies_id"]   = ticket["sla_policy_id"]
                elif "sla_policies_id" in (list(ticket.keys())):
                    ticket["sla_policy_id"]     = ticket["sla_policies_id"]
            else:
                pass

        return tickets

    def for_asset(tickets, container_var):
        
        # Corrector de custom_fields
        for ticket in tickets[container_var]:
            
            # Corrige stats si lo tiene
            if 'stats' in (list(ticket.keys())):
                ticket_stats = ticket.pop('stats')
                if "created_at" in (list(ticket_stats.keys())): # Eliminacion por repetido
                    ticket_stats.pop('created_at')
                if "updated_at" in (list(ticket_stats.keys())): # Eliminacion por repetido
                    ticket_stats.pop('updated_at')
                
                ticket.update(ticket_stats)
                ticket = list_corrector(ticket)

                # Cambio de Fechas Santiago
                if "status_updated_at" not in (list(ticket.keys())):
                    ticket["status_updated_at"] = None
                else :
                    ticket["status_updated_at"] = convertir_fecha_santiago(ticket["status_updated_at"])
                
                if "resolved_at" not in (list(ticket.keys())):
                    ticket["resolved_at"] = None
                else :
                    ticket["resolved_at"] = convertir_fecha_santiago(ticket["resolved_at"])
                
                if "closed_at" not in (list(ticket.keys())):
                    ticket["closed_at"] = None
                else :
                    ticket["closed_at"] = convertir_fecha_santiago(ticket["closed_at"])

                if "first_assigned_at" not in (list(ticket.keys())):
                    ticket["first_assigned_at"] = None
                else :
                    ticket["first_assigned_at"] = convertir_fecha_santiago(ticket["first_assigned_at"])

                if "assigned_at" not in (list(ticket.keys())):
                    ticket["assigned_at"] = None
                else :
                    ticket["assigned_at"] = convertir_fecha_santiago(ticket["assigned_at"])

                if "agent_responded_at" not in (list(ticket.keys())):
                    ticket["agent_responded_at"] = None
                else :
                    ticket["agent_responded_at"] = convertir_fecha_santiago(ticket["agent_responded_at"])

                if "first_responded_at" not in (list(ticket.keys())):
                    ticket["first_responded_at"] = None
                else :
                    ticket["first_responded_at"] = convertir_fecha_santiago(ticket["first_responded_at"])

                if "pending_since" not in (list(ticket.keys())):
                    ticket["pending_since"] = None
                else :
                    ticket["pending_since"] = convertir_fecha_santiago(ticket["pending_since"])

                if "opened_at" not in (list(ticket.keys())):
                    ticket["opened_at"] = None
                else :
                    ticket["opened_at"] = convertir_fecha_santiago(ticket["opened_at"])

        return tickets

    # Corrector de recorrido
    if "tickets" in (list(tickets.keys())):
        container_var = "tickets"
    else:
        container_var = "ticket"

        # Parche para no listas
        temp_container         = tickets[container_var]
        tickets[container_var] = []
        tickets[container_var].append(temp_container)
        del temp_container # Eliminacion de duplicado

    tickets = for_custom(tickets, container_var)
    tickets = for_asset(tickets, container_var)

    return tickets

# Recibe todo el response y calcula el limite de interacciones con la API
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
        
# algo = test("ticket_specific", 2370)
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

# tickets = fresh_service_get("tickets")
#tickets = fresh_service_get("conversations")
#tickets = custom_field_corrector(tickets)
#print(tickets)