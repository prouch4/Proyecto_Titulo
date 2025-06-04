# Consulta en QRadar las AQL viejas y las guarda en DB a partir de los
# semaforos creados en QRadar_create_old_semaforo.py

# Manejo de fechas
from datetime import datetime
from datetime import timedelta
from time import sleep

# Comunicaciones
from urllib3 import disable_warnings
import requests
import urllib.parse

import connection_db as db

def init():

    init_date = datetime.now()
    crons     = consulta_cron_data_old()
    
    if crons != None:
        
        min_date = crons[5]
        max_date = crons[6]

        print("RANGO DE FECHAS UTILIZADO: {0}  ~  {1}".format(min_date,max_date))
        
        update_cron_data_old(crons[0]) # Marca validation como 0
        semaforos = get_semaforos(min_date, max_date)
        
        if semaforos == [] or semaforos == None:
            timer = (datetime.now() - init_date) / timedelta(seconds=1)
            print(f"No se encontraron Semaforos/Tareas pendientes: {timer} Segundos")
        
        else:
    
            # Marcado de los semaforos especial, solo para las tareas tomadas de forma manual
            special_update_semaforo(min_date, max_date)
            timer = (datetime.now() - init_date) / timedelta(seconds=1)
            print(f"Semaforos/Tareas encontradas: {timer} Segundos") # tiempo transcurrido
            
            last_index = len(semaforos)
            index      = 0
            while index < last_index:
                
                # Guardado en variables de la informacion del semaforo/tarea, para facilitar la lectura        
                semaforo_id = semaforos[index][0]
                aql_id      = semaforos[index][1]
                start_time  = semaforos[index][2]
                stop_time   = semaforos[index][3]

                aql             = get_aql(aql_id)
                table           = aql[2] # Obtencion de la tabla destino
                aql             = aql[1] # Obtencion de la query AQL
                table_columns   = get_columns(table) # Obtencion de las columnas de la tabla
                
                aql = aql.replace(":start_time:", str(start_time))
                aql = aql.replace(":stop_time:",  str(stop_time))

                update_semaforo(semaforo_id, 'in_excecution_date', 'NOW()')
                update_semaforo(semaforo_id, 'in_excecution', True)
                update_semaforo(semaforo_id, 'aql_start', True)

                # print("EJECUTANDO AQL N° {0}".format(aql_id))
                qr_results = get_qradar_result(aql)
                
                if qr_results != None:

                    update_semaforo(semaforo_id, 'aql_end', True)
                    insert_results(table, table_columns, semaforo_id, qr_results)

                    if save_validation(qr_results, semaforo_id, table):
                        update_semaforo(semaforo_id, 'save_result', True)
                        retry_mark(semaforo_id)

                index += 1

            timer = (datetime.now() - init_date) / timedelta(seconds=1)
            print(f"Resultados Insertados: {timer} segundos")

        if crons[6] < crons[4]:
            print(f"{datetime.now()} - Creando nuevo sub-rango de fechas")
            create_new_cron_data_old(crons)
        else:
            print(f"{datetime.now()} - Fin de la parametrizacion alcanzado")    
    else:
        print(f"{datetime.now()} - No se encontraron tareas disponibles para el crontab")
    
    print("Tiempo total de ejecucion: {0} segundos".format(str((datetime.now() - init_date) / timedelta(seconds=1))))
    
def consulta_cron_data_old():
    
    query  = "SELECT * FROM cron_data_old where validation = 1"
    result = db.get_single_data(query)

    return result

def update_cron_data_old(id):

    # Realizar las operaciones necesarias en la base de datos
    update = f"UPDATE cron_data_old SET validation = 0 WHERE id = {id}"
    db.single_insert(update)

def create_new_cron_data_old(crons):
    
    # Realizar las operaciones necesarias en la base de datos
    new_date = crons[6] + timedelta(days = 1)
    query    = f"INSERT INTO cron_data_old (data_cron_id, data_cron, start_periodo, end_periodo, start_date, end_date) VALUES ({crons[1]},'{crons[2]}','{crons[3]}','{crons[4]}','{crons[6]}','{new_date}')"
    
    # Ejecucion de la querys        
    db.single_insert(query)   

def get_date():
    
    # Creacion de la fecha de termino e inicio
    today  = datetime.now()
    minute = today.time().minute
    if int(str(minute)[-1]) > 5:
        diferencia = int(str(minute)[-1]) - 5
        stop       = today - timedelta(minutes = diferencia)
    elif int(str(minute)[-1]) < 5:
        diferencia = int(str(minute)[-1])
        stop       = today - timedelta(minutes = diferencia)
    else:
        stop       = today

    # eliminacion de los segundos
    stop = datetime(stop.year, stop.month, stop.day, stop.hour, stop.minute)

    return stop

def get_semaforos(start_date, stop_date):
    
    query  = f"SELECT id, ariel_id, start_time, stop_time FROM semaforo WHERE (in_excecution IS Null) AND (start_time >= '{start_date}' AND stop_time <= '{stop_date}') AND retry = 1 ORDER BY start_time ASC"
    result = db.get_multi_data(query)

    return result

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(ariel_id):

    query  = f"SELECT id, aql, to_table FROM ariel_search WHERE disable = 0 AND id = {ariel_id}"
    result = db.get_single_data(query)

    return result

def get_qradar_result(query):
    disable_warnings()
    continuar = False
    
    # GENERANDO BUSQUEDA
    payload         = {}
    headers         = {'Authorization': 'Basic cG9ydGFsZXM6QWRhcHRpdmUuMTIz'}
    enconde_query   = urllib.parse.quote(query)
    search_url      = "https://172.16.17.10/api/ariel/searches?query_expression={0}".format(enconde_query)
    
    try:
        search = requests.request("POST", search_url, headers=headers, data=payload, verify=False)
        search.raise_for_status()
        if search.status_code == 200 or search.status_code == 201:
            continuar = True
    except requests.exceptions.HTTPError as http_error:

        print("HTTP error: ", http_error)
        continuar = False
    except requests.exceptions.ConnectionError as conn_error:

        print("Connetion error: ", conn_error)
        continuar = False
    except requests.exceptions.RequestException as requests_error:
        
        print("Request error: ", requests_error)
        continuar = False

    # OBSERVANDO LA BUSQUEDA
    if continuar == True:
        search = search.json()

        # BUSQUEDA DE ESTATUS
        result_url      = "https://172.16.17.10/api/ariel/searches/{0}".format(search['search_id'])
        status_busqueda = ''

        timer       = 0         # Contabiliza el tiempo transcurrido
        timer_limit = 120       # Tiempo maximo que puede transcurrir
        while status_busqueda != "COMPLETED":
            espera = 0.5        # Establece el tiempo de espera de una respuesta
            sleep(espera)       # Espera el tiempo indicado arriba
            timer += espera     # Añade el tiempo total esperado
            
            if timer <= timer_limit:
                try:
                    result = requests.request("GET", result_url , headers=headers, data=payload, verify=False)
                    result = result.json()
                    status_busqueda = result['status']
                except:
                    continuar = False
                    print("Error en la busqueda de estatus")
            else:
                continuar = False
                break
    
    
    # RESULTADO DE BUSQUEDA
    if continuar == True:
        result_url = "https://172.16.17.10/api/ariel/searches/{0}/results".format(search['search_id'])
        try:
            result = requests.request("GET", result_url, headers=headers, data=payload, verify=False)
            result = result.json()
        except:
            print("Error en obtencion del resultado de busqueda")
            continuar = False
    else:
        continuar = False

    if continuar == True:
        return result
    else:
        return None

def get_columns(tabla):
    query = f"SHOW COLUMNS FROM {tabla}"
    result = db.get_multi_data(query)
    
    columns = []
    for x in result:
        if x[0] not in ('id', 'create_time'): # Columnas ignoradas
            columns.append(x[0])

    return columns

# Inserta los resultados obtenidos en QRadar a la base de datos
def insert_results(table, table_columns, semaforo_id, result):  
    columns = ", ".join(table_columns)
    
    list_query = []
    list_data  = []
    for datas in result["events"]:

        # Seleccion de los valores a insertar en DB
        temp_list = []
        temp_list.append(semaforo_id)
        for values in datas.values():
            temp_list.append(values)
        
        # Creacion de la query de INSERT en DB
        insert_values = "%s, " * len(temp_list)  
        insert_values = insert_values.rstrip(', ')
        query         = f"INSERT INTO {table} ({columns}) VALUES ({insert_values})"

        list_query.append(query)
        list_data.append(tuple(temp_list))
    
    # Ejecucion de la querys
    db.multi_insert(list_query, list_data)
    
def update_semaforo(semaforo_id, columna, value):

    update = f"UPDATE semaforo SET {columna} = {value} WHERE id = {semaforo_id}"
    db.single_insert(update)

def retry_mark(semaforo_id):
    # Realizar las operaciones necesarias en la base de datos
    update = f"UPDATE semaforo SET retry = NULL, expire = NULL WHERE id = {semaforo_id}"
    db.single_insert(update)

def save_validation(qr_result, semaforo_id, table):
    
    db_result = []
    query = f"SELECT count(id) FROM {table} WHERE semaforo_id = {semaforo_id}"
    db_result = db.get_single_data(query)
    
    if len(qr_result['events']) == db_result[0]:
        return True
    else:
        return False

# Marca rapidamente los semaforos dentro del rango de fechas, para ejecucion manual.
# Utilidad, evitar que 2 usuarios ejecuten el mismo rango de fechas en modo manual   
def special_update_semaforo(min_date, max_date):
    
    # Creacion del semaforo en la base de datos
    query = f"SELECT group_concat(id) FROM semaforo WHERE (in_excecution IS Null) AND (start_time >= '{min_date}' AND start_time < '{max_date}') AND retry = 1"
    semaforos_ids = f"({db.get_single_data(query)[0]})"
    
    # Realizar las operaciones necesarias en la base de datos
    query = f"UPDATE semaforo SET in_excecution_date = NOW(), in_excecution = True WHERE id IN {semaforos_ids}"
    db.single_insert(query)

# =====================
# EJECUCION
# =====================
init()