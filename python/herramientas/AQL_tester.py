#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

import requests
import urllib.parse
from datetime import datetime
from datetime import timedelta

from time import sleep
from urllib3 import disable_warnings

import connection_db as db

#####################################
# SCRIPT                            #
#####################################

connection_params = {
    "host"     : "as-mysql-01.c8s4pjwjpeb6.us-east-1.rds.amazonaws.com",
    "user"     : "admin",
    "password" : "4rqu1t3ctur4.$",
    "database" : "qradar_portal"
}

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql (query_id):
    result = None

    query  = f"SELECT aql, to_table FROM ariel_search WHERE id = {query_id}"
    result = db.get_single_data(query)

    return result

# Genera el perdiodo de 5 minutos para limitar el AQL
def get_date_range():
    today = datetime.now()
    
    # ================================
    # Creacion de la fecha de termino e inicio
    # ================================
    minute = today.time().minute
    
    if int(str(minute)[-1]) > 5:
        diferencia  = int(str(minute)[-1]) - 5
        stop        = today - timedelta(minutes = diferencia)
    
    elif int(str(minute)[-1]) < 5:
        diferencia  = int(str(minute)[-1])
        stop        = today - timedelta(minutes = diferencia)
    
    else:
        stop        = today

    # eliminacion de los segundos
    stop    = datetime(stop.year, stop.month, stop.day, stop.hour, stop.minute)
    start   = stop - timedelta(minutes=5)

    return (start, stop)

# Limita el AQL con el periodo de 5 minutos
def replace_query (aql, dates) :
    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(dates[0]))
    aql = aql.replace(":stop_time:", str(dates[1]))
    
    return aql

# Consulta la query en QRadar
def get_qradar_result(query):
    disable_warnings()

    # Generando busqueda
    continuar   = False
    payload     = {}
    headers     = {'Authorization': 'Basic cG9ydGFsZXM6QWRhcHRpdmUuMTIz'}
    
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

# Consulta la estrutura de la tabla donde se guardara
def get_columns (tabla):
    query = f"SHOW COLUMNS FROM {tabla}"

    columns = []
    result  = db.get_multi_data(query)
    for x in result:
        if x[0] not in ('id', 'create_time'): # Columnas ignoradas
            columns.append(x[0])

    return columns

# Inserta los resultados obtenidos en QRadar a la base de datos
def insert_results(table, table_columns, semaforo_id, result):  
    columns = ", ".join(table_columns)

    # Realizar las operaciones necesarias en la base de datos
    list_query  = []
    list_data   = []

    for event in result["events"]:

        # Seleccion de los valores a insertar en DB
        temp_list = []
        temp_list.append(semaforo_id)
        for values in event.values():
            temp_list.append(values)

        print("columnas disponibles de la tabla: "  , len(table_columns))
        print("columnas que quiero insertar: "      , len(temp_list))
        
        # Creacion de la query de INSERT en DB
        insert_values = str("%s, " * len(temp_list)).rstrip(', ')
        list_query.append(f"INSERT INTO {table} ({columns}) VALUES ({insert_values})")
        list_data.append(tuple(temp_list))

    db.multi_insert(list_query, list_data)

query_id    = 1
insert_db   = False

print("\nTEST DE NUEVA AQL")
dates       = get_date_range()
querys      = get_aql(query_id)

# Busqueda en QRADAR
print("CONSULTANDO EN QRADAR")
new_query   = replace_query(querys[0], dates)
print(new_query)
results     = get_qradar_result(new_query)

print("RESULTADOS RECIBIDOS")
print(results)

# if insert_db == True:
#     print("INGRESANDO EN DB")
#     table_columns = get_columns(connection_params, querys[1])
#     insert_results(querys[1], table_columns, 0, dates, results)
# else:
#     print("RECORRIENDO LOS RESULTADOS: ", len(results['events']))
#     if results != [] and results != None:
#         for events in results['events']:
#             print("\n======================================================")
#             # print(events)
#             for aux in events:
#                 print(aux, events[aux])
#             break