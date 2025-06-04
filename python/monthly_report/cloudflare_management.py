#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime, timedelta
from urllib3  import disable_warnings
from copy     import copy
from os       import listdir
from json     import dumps # Para muestras

import connection_cloudflare as cf
import connection_db         as db

#####################################
# SCRIPT                            #
#####################################

### FUNCIONES ###
# GENERALES #

def log_print(text_message):
    actual_date = datetime.now()
    actual_date = datetime(
        year    = actual_date.year,
        month   = actual_date.month,
        day     = actual_date.day,
        hour    = actual_date.hour,
        minute  = actual_date.minute,
        second  = actual_date.second,
    )

    print(f"{actual_date} - {text_message}")

def date_formater(date_string : str):
    output_var = None

    try:
        if len(date_string) > 18:
            output_var = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
            output_var = output_var - timedelta(microseconds = output_var.microsecond)

        elif len(date_string) == 8:
            output_var = datetime.strptime(date_string, "%y-%m-%d")

    except:
        log_print(f"No fue posible convertir la fecha: {date_string}")

    return output_var

def insert_new_data(monthly_conn, table, insert_list : list):
    
    if insert_list == None and insert_list == []:
        data_length = 0
    else:
        data_length = len(insert_list)
    log_print(f"Insertando {data_length} registros")
    
    list_query = []
    list_data  = []
    for data in insert_list:
        
        insert_data   = tuple(dict(data).values())
        escape_values = str("%s, " * len(insert_data)).rstrip(", ")
        columns       = ",".join(tuple(dict(data).keys()))
        insert_query  = f"INSERT INTO {table} ({columns}) VALUES ({escape_values})"
        
        list_query.append(insert_query)
        list_data.append(insert_data)
    
    db.__insert_multi_data__(monthly_conn, list_query, list_data)

def update_old_data(monthly_conn, table, id_colunm, update_list : list):
    
    if update_list == None and update_list == []:
        data_length = 0
    else:
        data_length = len(update_list)
    log_print(f"Actualizando {data_length} registros")

    list_query = []
    list_data  = []
    for data in update_list:
        
        id_value       = data.pop(id_colunm)
        update_data    = tuple(dict(data).values())
        update_columns = "`" + "` = %s, `".join(tuple(dict(data).keys())) + "` = %s"
        update_query   = f"UPDATE `{table}` SET {update_columns} WHERE (`{id_colunm}` = '{id_value}')"
        
        list_query.append(update_query)
        list_data.append(update_data)
    
    db.__insert_multi_data__(monthly_conn, list_query, list_data)

# CLOUDFLARE #

def get_customer_cloudflare(monthly_conn, force_api = False): # listar todas las zone_id
    
    def from_cloudflare():

        disable_warnings()
        output_var      = None
        zone_list_url   = "/client/v4/zones"
        request_results = cf.requests_to_cloudflare(f"{zone_list_url}?per_page=100")

        if request_results == None:
            
            log_print("Respuesta inesperada de customer_zones")

        elif request_results != None:

            total_page  = request_results["result_info"]["total_pages"]
            actual_page = request_results["result_info"]["page"]

            while actual_page <= total_page:
                
                if actual_page > 1:
                    request_results = cf.requests_to_cloudflare(f"{zone_list_url}?per_page=100&page={actual_page}")

                total_count  = request_results["result_info"]["count"]
                actual_count = 0
                    
                while (actual_count < total_count):
                    
                    if output_var == None:
                        output_var = []
                    
                    zone_dict = {
                        "zone_id"       : request_results["result"][actual_count]["id"],
                        "zone_name"     : request_results["result"][actual_count]["name"],
                        "status"        : request_results["result"][actual_count]["status"],
                        "created_at"    : request_results["result"][actual_count]["created_on"],
                        "modified_at"   : request_results["result"][actual_count]["modified_on"],
                        "activated_at"  : request_results["result"][actual_count]["activated_on"],
                        "customer"      : request_results["result"][actual_count]["owner"]["name"],
                        "customer_id"   : request_results["result"][actual_count]["owner"]["id"],
                        "account_id"    : request_results["result"][actual_count]["account"]["id"],
                        "account_name"  : request_results["result"][actual_count]["account"]["name"],
                    }
                    
                    output_var.append(zone_dict)

                    actual_count += 1
                actual_page += 1

        return output_var
    
    def from_datebase(monthly_conn):
        
        output_var = None
        query      = "SELECT * FROM cloudflare_customer_zone"
        output_var = db.__get_data__(monthly_conn, query, multi = True)

        return output_var

    output_var          = from_datebase(monthly_conn)
    customer_cloudflare = from_cloudflare()

    # Por si la tabla esta vacia
    if output_var == None or output_var == []:
        log_print(f"Insertando Clientes nuevos")
        insert_new_data(monthly_conn, "cloudflare_customer_zone", customer_cloudflare)
        output_var = customer_cloudflare

    elif datetime.now().day == 1 or force_api == True:

        insert_list = []
        update_list = []
        for origen in customer_cloudflare:
            
            update_data = False
            insert_data = True
            for destino in output_var:

                # Para Añadir
                if origen['zone_id'] == destino['zone_id']:
                    insert_data = False

                    if (
                        origen['zone_name']     == destino['zone_name']       and
                        origen['customer_id']   == destino['customer_id']     and 
                        origen['customer']      == destino['customer']        and
                        origen['account_id']    == destino['account_id']      and 
                        origen['account_name']  == destino['account_name']    and
                        origen['status']        == destino['status']
                        ):
                        update_data  = False
                    else:
                        origen['id'] = destino['id']
                        update_data  = True

                    break
                
            if insert_data == True:  # Añadir
                insert_list.append(origen)
            if update_data == True:  # Actualizar
                update_list.append(origen)

        if len(insert_list) > 0:
            log_print(f"Insertar clientes nuevos: {len(insert_list)}")
            insert_new_data(monthly_conn, "cloudflare_customer_zone", insert_list)
        if len(update_list) > 0:
            log_print(f"Actualizar clientes: {len(update_list)}")
            update_old_data(monthly_conn, "cloudflare_customer_zone", "id", update_list)

        output_var = customer_cloudflare

    else:
        pass # Placeholder para futuras ediciones
    
    # Corrector de variabilidad
    if output_var == []:
        output_var = None

    return output_var

def customer_sorted(customer_data):

    output_var = None
    for data in customer_data:
        
        if output_var == None:
            output_var = {}
        
        if data['customer'] not in (output_var):
            output_var.update({f"{data['customer']}" : []})
        
        if data['customer'] in (output_var):
            output_var[data['customer']].append(data)

    return output_var

def get_customer_id(customer_name, customer_data, var_from = None):
    
    output_var = None

    if var_from == "customer":
        for data in customer_data:
            if data == customer_name:
                if len(customer_data[data][0]) > 0:
                    output_var = customer_data[data][0]["customer_id"]
                break

    elif var_from != "customer":
        for data in customer_data:
            for internal_data in customer_data[data]:
                log_print(internal_data)
                if internal_data[var_from] == customer_name:
                    log_print(internal_data[var_from])
                

    return output_var

def dns_info(zone_data): # listar dns
    output_var = None
    
    disable_warnings()
    for data in zone_data:

        dns_records_url  = f"/client/v4/zones/{data['zone_id']}/dns_records"
        request_results = cf.requests_to_cloudflare(f"{dns_records_url}?per_page=100")

        if request_results == None:
    
            log_print("Respuesta Inesperada al obtener dns_records")
        
        elif request_results != None:
            
            # Examinar indices
            total_pages = request_results["result_info"]["total_pages"]
            actual_page = request_results["result_info"]["page"]
            while (actual_page <= total_pages):
                
                if actual_page > 1: # Evita la consulta duplicada del original
                    request_results = cf.requests_to_cloudflare(f"{dns_records_url}?per_page=100&page={actual_page}")
                
                if request_results["result"] != [] and request_results["result"] != None:
                    
                    if output_var == None:
                        output_var = []
                    
                    while len(request_results["result"]) > 0:

                        dns_data = request_results["result"].pop(0)
                        output_dict = {
                            "dns_id"      : dns_data["id"],
                            "customer_id" : data["customer_id"],
                            "zone_id"     : data["zone_id"],
                            "name"        : dns_data["name"],
                            "ttl"         : dns_data["ttl"],
                            "type"        : dns_data["type"],
                            "content"     : dns_data["content"],
                            "proxied"     : dns_data["proxied"],
                            "created_on"  : dns_data["created_on"],
                            "modified_on" : dns_data["modified_on"],
                            "start_date"  : datetime.now()
                        }

                        output_var.append(output_dict)

                actual_page += 1

    return output_var

def dns_old_info(monthly_conn, customer_id = None, determined_date = None):
    
    output_var = None

    if determined_date == None and customer_id == None:
        query      = "SELECT start_date FROM cloudflare_dns_records ORDER BY start_date DESC LIMIT 1"
        last_date  = db.__get_data__(monthly_conn, query, multi = False)

    elif determined_date == None and customer_id != None:
        query      = f"SELECT start_date FROM cloudflare_dns_records WHERE customer_id = '{customer_id}' ORDER BY start_date DESC LIMIT 1"
        last_date  = db.__get_data__(monthly_conn, query, multi = False)        

    elif determined_date != None and customer_id != None:

        query      = f"SELECT start_date FROM cloudflare_dns_records WHERE customer_id = '{customer_id}' and start_date <= '{determined_date}' ORDER BY start_date DESC LIMIT 1"
        last_date  = db.__get_data__(monthly_conn, query, multi = False)

    if last_date != None:
        last_date = last_date['start_date']
        last_date = datetime(last_date.year, last_date.month, last_date.day, 0, 0, 0)
        stop_date = datetime(last_date.year, last_date.month, last_date.day, 23, 59, 59)

        if customer_id == None:
            query      = f"SELECT * FROM cloudflare_dns_records WHERE start_date >= '{last_date}' AND start_date <= '{stop_date}'"
        elif customer_id != None:
            query      = f"SELECT * FROM cloudflare_dns_records WHERE start_date >= '{last_date}' AND start_date <= '{stop_date}' AND customer_id = '{customer_id}'"

        output_var = db.__get_data__(monthly_conn, query, multi = True)
    return output_var

def dns_records_historico(base_path : str, customer : str, date_folder : str):
    
    def extract_content(line_text):
        output_var = None
        
        split_line = copy(line_text)
        split_line = str(split_line).split(",")
        
        output_var = copy(line_text)
        output_var = str(output_var).replace(split_line[0],"")
        output_var = str(output_var).replace(split_line[1],"")
        output_var = str(output_var).replace(split_line[2],"")
        output_var = str(output_var).replace(split_line[-1],"")[3:-1]

        return output_var

    output_var  = None

    internal_folders = listdir(f"{base_path}/{customer}/{date_folder}")
    internal_folders = sorted(internal_folders)

    if "dns" in internal_folders or "DNS" in internal_folders:
        dns_folder = listdir(f"{base_path}/{customer}/{date_folder}/{internal_folders[0]}")

        for file in dns_folder:
            file_data = open(f"{base_path}/{customer}/{date_folder}/{internal_folders[0]}/{file}")

            for line in file_data:
                insert_data = {
                    "dns_id"        : None,
                    "customer_id"   : f"{customer}_placeholder", 
                    "zone_id"       : None,
                    "name"          : line.split(",")[0], 
                    "ttl"           : int(line.split(",")[1]), 
                    "type"          : line.split(",")[2], 
                    "content"       : extract_content(line), 
                    "proxied"       : int(str(line.split(",")[-1]).replace("\n","")), 
                    "start_date"    : date_formater(date_folder)
                }

                if output_var == None:
                    output_var = []

                output_var.append(insert_data)
            file_data.close()

    return output_var

def get_list_updated_dns(old_data, new_data, actual_date = None):
    output_var = None

    if actual_date == None:
        actual_date = datetime.now()

    if old_data != None and new_data != None:
        # Detectar nuevos
        list_dns_new    = []
        for new in new_data:
            is_new = True

            for old in old_data:
                # if new['dns_id'] == old['dns_id']:
                if new['name'] == old['name']:
                    is_new = False
                    break

            if is_new == True:
                temp_dict = {
                    'customer_id'   : new['customer_id'],
                    'name'          : new['name'],
                    'proxied'       : new['proxied'],
                    'action'        : 'Agregado',
                    'start_date'    : actual_date,
                }
                list_dns_new.append(temp_dict)

        # Detectar eliminados
        list_dns_deleted = []
        for old in old_data:
            is_deleted = True

            for new in new_data:
                # if new['dns_id'] == old['dns_id']:
                if new['name'] == old['name']:
                    is_deleted = False
                    break

            if is_deleted == True:
                temp_dict = {
                    'customer_id'   : new['customer_id'],
                    'name'          : new['name'],
                    'proxied'       : new['proxied'],
                    'action'        : 'Eliminado',
                    'start_date'    : actual_date,
                }
                list_dns_deleted.append(temp_dict)

        # Detectar cambios
        list_dns_updated = []
        for new in new_data:
            coincidencia = True

            for old in old_data:
                # if new['dns_id'] == old['dns_id']:
                if new['name'] == old['name']:
                    
                    if (coincidencia == True) and (new['customer_id'] != old['customer_id']):
                        coincidencia = False
                        
                    # if (coincidencia == True) and (new['zone_id'] != old['zone_id']):
                        coincidencia = False
                        
                    if (coincidencia == True) and (new['name'] != old['name']):
                        coincidencia = False
                        
                    # if (coincidencia == True) and (new['content'] != old['content']):
                    #     coincidencia = False
                        
                    if (coincidencia == True) and (new['ttl'] != old['ttl']) and (new['type'] != old['type']) and (new['proxied'] != old['proxied']):
                        coincidencia = False

                    if coincidencia == True:
                        pass
                    elif coincidencia == False:
                        temp_dict = {
                            'customer_id'   : new['customer_id'],
                            'name'          : new['name'],
                            'proxied'       : new['proxied'],
                            'action'        : 'Actualizado',
                            'start_date'    : actual_date,
                        }
                        list_dns_updated.append(temp_dict)

                    break

        if list_dns_new != [] or list_dns_updated != [] or list_dns_deleted != []:
            output_var = []

        for dns_new in list_dns_new:
            output_var.append(dns_new)

        for dns_updated in list_dns_updated:
            output_var.append(dns_updated)

        for dns_deleted in list_dns_deleted:
            output_var.append(dns_deleted)

    return output_var

def get_certificate(customer_data):
    output_var = None
    for data in customer_data:

        request_results  = cf.requests_to_cloudflare(f"/client/v4/zones/{data["zone_id"]}/ssl/certificate_packs")
        
        if request_results != None:    
            for result in request_results["result"]:
                for certificates in result["certificates"]:
                    
                    id                    = certificates["id"]
                    customer_id           = data["customer_id"]
                    zone_id               = certificates["zone_id"]
                    hosts                 = ",".join(certificates["hosts"])
                    certificate           = str(certificates["signature"]).replace("With", " ")
                    status                = certificates["status"]
                    certificate_pack_type = result["type"]
                    issuer                = certificates["issuer"]
                    bundle_method         = certificates["bundle_method"]
                    created_on            = result["created_on"]
                    uploaded_on           = certificates["uploaded_on"]
                    expires_on            = certificates["expires_on"]

                    data_dict = {
                        "certificate_id"        : id,
                        "customer_id"           : customer_id,
                        "zone_id"               : zone_id,
                        "hosts"                 : hosts,
                        "certificate"           : certificate,
                        "status"                : status,
                        "certificate_pack_type" : certificate_pack_type,
                        "issuer"                : issuer,
                        "bundle_method"         : bundle_method,
                        "created_on"            : created_on,
                        "uploaded_on"           : uploaded_on,
                        "expires_on"            : expires_on,
                        "start_date"            : datetime.now()
                    }

                    if output_var == None:
                        output_var = []
                    
                    output_var.append(data_dict)
    
    return output_var

def get_old_certificate(monthly_conn, customer_id = None):
    
    output_var = None

    if customer == None:
        query      = "SELECT start_date FROM cloudflare_ssl_certificate ORDER BY start_date DESC LIMIT 1"
    elif customer != None:
        query      = f"SELECT start_date FROM cloudflare_ssl_certificate WHERE customer_id = '{customer_id}' ORDER BY start_date DESC LIMIT 1"
    last_date  = db.__get_data__(monthly_conn, query, multi = False)
    
    if last_date != None:
        last_date = last_date['start_date']
        last_date = datetime(last_date.year, last_date.month, last_date.day, 0, 0, 0)
        stop_date = datetime(last_date.year, last_date.month, last_date.day, 23, 59, 59)

        query      = f"SELECT * FROM cloudflare_ssl_certificate WHERE customer_id = '{customer_id}' and start_date >= '{last_date}' AND start_date <= '{stop_date}'"
        output_var = db.__get_data__(monthly_conn, query, multi = True)
        
    return output_var

def new_dns_detector(datos_nuevos, datos_viejos = None, forced_date = None, customer_name = None):

    def remove_useless_data(temp_data):
        
        if "id" in list(temp_data.keys()):
            temp_data.pop("id")
        if "start_date" in list(temp_data.keys()):
            temp_data.pop("start_date")
        if "created_on" in list(temp_data.keys()):
            temp_data.pop("created_on")
        if "modified_on" in list(temp_data.keys()):
            temp_data.pop("modified_on")
        if "content" in list(temp_data.keys()): # Genera conflictos, debe ser mejorado
            temp_data.pop("content")

        return temp_data

    output_var = True

    if forced_date == None:
        exec_date  = datetime.now()
    elif forced_date != None:
        exec_date = forced_date

    
    if datos_viejos == None:
        log_print("(DNS_RECORDS) Insert de primera ejecucion")
        output_var = False
    elif exec_date.day in (1, 2, 3, 4) and (datos_viejos[0]['start_date'].day != 1 and datos_viejos[0]['start_date'].month != exec_date.month): # Forzar insert
        log_print("(DNS_RECORDS) Insert por primera semana")
        output_var = False
    elif len(datos_nuevos) != len(datos_viejos):
        log_print(f"(DNS_RECORDS) Insert por cambios 'modo rapido' {len(datos_nuevos)}/{len(datos_viejos)}")
        output_var = False
    else:
        coincidencias = []

        for nuevo in datos_nuevos:
            temp_new = copy(nuevo)
            temp_new = remove_useless_data(temp_new)
            
            for viejo in datos_viejos:
                
                temp_old = copy(viejo)
                temp_old = remove_useless_data(temp_old)

                if temp_new == temp_old:
                    coincidencias.append(temp_new['dns_id'])
                    break

        if len(datos_nuevos) != len(coincidencias):
            log_print("(DNS_RECORDS) Insert por cambios 'modo lento'")
            # print(f"Nuevos:        {len(datos_nuevos)}")
            # print(f"Coincidencias: {len(coincidencias)}")
            # print(f"Viejos:        {len(datos_viejos)}")
            output_var = False
        else:
            output_var = True
        
        if output_var == True:
            log_print("(DNS_RECORDS) Omitiendo...")

    return output_var

def new_certificate_detector(datos_nuevos, datos_viejos = None, customer_name = None):

    def remove_useless_data(temp_data):
        
        if "id" in list(temp_data.keys()):
            temp_data.pop("id")
        if "start_date" in list(temp_data.keys()):
            temp_data.pop("start_date")
        if "created_on" in list(temp_data.keys()):
            temp_data.pop("created_on")
        if "uploaded_on" in list(temp_data.keys()):
            temp_data.pop("uploaded_on")
        if "expires_on" in list(temp_data.keys()):
            temp_data.pop("expires_on")

        return temp_data

    output_var = True
    exec_date  = datetime.now()
    
    if datos_viejos == None:
        log_print("(CERTIFICATES) Insert de primera ejecucion")
        output_var = False
    elif exec_date.day in (1, 2, 3, 4) and (datos_viejos[0]['start_date'].day != 1 and datos_viejos[0]['start_date'].month != exec_date.month): # Forzar insert
        log_print("(CERTIFICATES) Insert por primera semana")
        output_var = False
    elif len(datos_nuevos) != len(datos_viejos):
        log_print(f"(CERTIFICATES) Insert por cambios 'modo rapido' {len(datos_nuevos)}/{len(datos_viejos)}")
        output_var = False
    else:
        coincidencias = []

        for nuevo in datos_nuevos:
            temp_new = copy(nuevo)
            temp_new = remove_useless_data(temp_new)
            
            for viejo in datos_viejos:
                
                temp_old = copy(viejo)
                temp_old = remove_useless_data(temp_old)

                if temp_new == temp_old:
                    coincidencias.append(temp_new['certificate_id'])
                    break

        if len(datos_nuevos) != len(coincidencias):
            # print(f"Nuevos:        {len(datos_nuevos)}")
            # print(f"Coincidencias: {len(coincidencias)}")
            # print(f"Viejos:        {len(datos_viejos)}")
            output_var = False
            log_print("(CERTIFICATES) Carga por cambios detectados")
        else:
            log_print("(CERTIFICATES) Omitiendo...")
            output_var = True

    return output_var

### EJECUCION ###

list_customer             = ["UDP"] # Deben llamarse igual a su carpeta contenedora
base_path                 = "/opt/python_process/python/monthly_report/cloudflare_historico"
cloudflare_dns_history    = False
cloudflare_domain_history = False

monthly_conn           = db.__try_open__(db.connection_params_2, True)

# Funcionamiento Rutinario
print("\n")
log_print("Procesando plataforma CLOUDFLARE")
customer_data   = get_customer_cloudflare(monthly_conn, True)
customer_data   = customer_sorted(customer_data)

for customer in customer_data:

    customer_id = get_customer_id(customer, customer_data, "customer")
    zone_data   = customer_data[customer]

    # DNS
    new_dns_data     = dns_info(zone_data)
    dns_old_data     = dns_old_info(monthly_conn, customer_id)
    if new_dns_detector(new_dns_data, dns_old_data) == False:
        insert_new_data(monthly_conn, "cloudflare_dns_records", new_dns_data)
        
        domain_history = get_list_updated_dns(dns_old_data, new_dns_data)
        if domain_history != None:
            insert_new_data(monthly_conn, "cloudflare_domain_history", domain_history)
    del new_dns_data, dns_old_data

    # CERTIFICATES
    certificate_data = get_certificate(zone_data)
    certificate_old  = get_old_certificate(monthly_conn, customer_id)
    if new_certificate_detector(certificate_data, certificate_old) == False:
        insert_new_data(monthly_conn, "cloudflare_ssl_certificate", certificate_data)

# CARGAR HISTORICOS
if cloudflare_dns_history == True:

    for customer in list_customer:

        data_folders  = listdir(f"{base_path}/{customer}")
        data_folders  = sorted(data_folders)
        for folder in data_folders:

            forced_date       = date_formater(folder)
            log_print(f"Leyendo historico: {forced_date}")
            
            dns_historico     = dns_records_historico(base_path, customer, folder)
            dns_old_historico = dns_old_info(monthly_conn, f"{customer}_placeholder", folder)
            if new_dns_detector(dns_historico, dns_old_historico, forced_date) == False:
                insert_new_data(monthly_conn, "cloudflare_dns_records", dns_historico)

if cloudflare_domain_history == True:
    
    list_customer = "SELECT customer_id FROM cloudflare_dns_records GROUP BY customer_id ORDER BY customer_id ASC"
    list_customer = db.__get_data__(monthly_conn, list_customer, multi = True)
    
    for customer in list_customer:
        customer_id = customer['customer_id']

        date_range_history = "SELECT customer_id, date_format(start_date, \"%Y-%m-%d 00:00:00\") as start_date_mod FROM cloudflare_dns_records WHERE customer_id = %s GROUP BY start_date_mod, customer_id ORDER BY customer_id ASC, start_date_mod ASC"
        date_range_history = db.__get_data__(monthly_conn, date_range_history, (customer_id,), multi = True)
        
        data_anterior = None
        for date_range in date_range_history:
            
            if date_range_history[0] == date_range:
                data_anterior   = date_range
            else:
                
                old_start_date  = datetime.strptime(data_anterior["start_date_mod"], "%Y-%m-%d 00:00:00")
                old_stop_date   = datetime(old_start_date.year, old_start_date.month, old_start_date.day, 23, 59, 59)
                
                new_start_date  = datetime.strptime(date_range["start_date_mod"], "%Y-%m-%d 00:00:00")
                new_stop_date   = datetime(new_start_date.year, new_start_date.month, new_start_date.day, 23, 59, 59)

                query_dns_data  = "SELECT * FROM cloudflare_dns_records WHERE customer_id = %s AND (start_date >= %s AND start_date <= %s)"
                old_dns_data    = db.__get_data__(monthly_conn, query_dns_data, (customer_id, old_start_date, old_stop_date), multi = True)
                new_dns_data    = db.__get_data__(monthly_conn, query_dns_data, (customer_id, new_start_date, new_stop_date), multi = True)

                domain_history  = get_list_updated_dns(old_dns_data, new_dns_data, new_start_date)
                if domain_history != None: 
                    print(f"{customer_id} : {new_start_date} : {len(domain_history)}")
                    insert_new_data(monthly_conn, "cloudflare_domain_history", domain_history)

                data_anterior = date_range

monthly_conn     = db.__try_close__(monthly_conn)