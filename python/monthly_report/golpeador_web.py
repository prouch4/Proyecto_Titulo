#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from requests import request, exceptions
from datetime import datetime, timedelta
from time     import sleep


import connection_db as db

#####################################
# SCRIPT                            #
#####################################

def __try_request__(url):
    
    output_var = {
        "response_detail" : None,
        "with_https"      : None,
    }

    try:
        response = request(method = "GET", url = url, timeout = 0.5)
        response.raise_for_status()
        
        if response.status_code == 200 or response.status_code == 201:
            # print(response.headers)
            if "IIS" in response.text:
                placeholder_text                = response.text[response.text.find("<title>"):response.text.find("</title>")].replace("<title>", "")
                output_var["response_detail"]   = f"Muestra página {placeholder_text} (Internet Information Services)"
            
            else:
                output_var["response_detail"]   = "Página desplegada"
            
            if str(url).replace("http:", "https:") in str(response.url):
                output_var["with_https"]        = True
            
    except exceptions.HTTPError as http_error:
        # print("Error HTTP")
        output_var["response_detail"] = str(http_error)
    
    except exceptions.Timeout as timeout_error:
        # print("Timeout Error")
        output_var["response_detail"] = "ERR_CONNECTION_TIMED_OUT"

    except exceptions.ConnectionError as conn_error:
        # print("Error Conexion")
        output_var["response_detail"] = str(conn_error)

    except exceptions.RequestException as requests_error:
        # print("Request Error")
        output_var["response_detail"] = str(requests_error)

    if "HTTPS" in str(output_var["response_detail"]) or "https" in str(output_var["response_detail"]):
        output_var["with_https"]      = True
    elif output_var["with_https"] == None:
        output_var["with_https"]      = False

    return output_var

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



print("\n")
log_print("Iniciando Golpeador_web.py")

monthly_conn       = db.__try_open__(db.connection_params_2, True)
customer_data_list = "SELECT name, short_name, cloudflare_id FROM customers WHERE cloudflare_id IS NOT NULL"
customer_data_list = db.__get_data__(monthly_conn, customer_data_list, multi = True)

stop_date   = datetime.now()
# stop_date   = datetime(stop_date.year, stop_date.month, stop_date.day) - timedelta(days = 1)
# start_date  = datetime(stop_date.year, stop_date.month, 1)
stop_date   = datetime(stop_date.year, stop_date.month, stop_date.day, 23, 59, 59)  - timedelta(days = 1)
start_date  = datetime(stop_date.year, stop_date.month, 1, 0, 0, 0)

for customer_data in customer_data_list:
    
    log_print(f"Leyendo: {customer_data['name']}")
    customer_id = customer_data['cloudflare_id']
    dns_records = "SELECT DISTINCT name FROM cloudflare_dns_records WHERE proxied = 0 AND customer_id = %s AND type IN (\"A\") AND (start_date >= %s AND start_date <= %s)"
    dns_records = db.__get_data__(monthly_conn, dns_records, (customer_id, start_date, stop_date), multi = True)

    if dns_records == None:
        log_print(f"No se encontraron dns_name para {start_date}~{stop_date}")
    else:
        insert_data = []
        for record in dns_records:
            sleep(0.5)
            check_url       = f"http://{record['name']}"
            response_detail = __try_request__(check_url)
            update_dict = { 
                "customer_id"       : customer_id, 
                "dns_name"          : record['name'],
                "with_https"        : response_detail["with_https"],
                "response_detail"   : str(response_detail["response_detail"]),
                "start_date"        : datetime.now(),
            }

            insert_data.append(update_dict)
        insert_new_data(monthly_conn, "cloudflare_dns_response", insert_data)
                
monthly_conn = db.__try_close__(monthly_conn)

# url_test        = "portal.udp.cl"
# check_url       = f"http://{url_test}"
# response_detail = __try_request__(check_url)
# print(response_detail)