from datetime import datetime
from datetime import timedelta
from time import sleep

import connection_qradar as qradar
import connection_db as db

def get_date():
    today = datetime.now()
    
    # ================================
    # Creacion de la fecha de termino e inicio
    # ================================
    minute = today.time().minute
    if int(str(minute)[-1]) > 5:
        diferencia = int(str(minute)[-1]) - 5
        stop = today - timedelta(minutes = diferencia)
    elif int(str(minute)[-1]) < 5:
        diferencia = int(str(minute)[-1])
        stop = today - timedelta(minutes = diferencia)
    else:
        stop = today

    # eliminacion de los segundos
    stop = datetime(stop.year, stop.month, stop.day, stop.hour, stop.minute)

    return stop

def replace_query(aql, start_time, stop_time):
    
    # Para el futuro, las timezone son una molestia con los cambios de hora
    # Hay que convertir la hora local (invierno/verano) en una neutra para evitar estos problemas
    start_time      = round(start_time.timestamp()) * 1000 # Para la AQL
    stop_time       = round(stop_time.timestamp())  * 1000 # Para la AQL

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(start_time))
    aql = aql.replace(":stop_time:",  str(stop_time))
    
    return aql

def get_semaforos(class_connection):
    query  = "SELECT id, ariel_id, start_time, stop_time FROM semaforo WHERE (in_excecution IS Null) AND retry = 1 ORDER BY start_time"
    # query  = "SELECT id, ariel_id, start_time, stop_time FROM semaforo WHERE (in_excecution IS Null) AND retry = 1 ORDER BY start_time limit 9"
    # query  = "SELECT id, ariel_id, start_time, stop_time FROM semaforo WHERE (in_excecution IS Null) AND retry = 1  AND stop_time <= '2024-1-6 00:00:00' ORDER BY start_time"
    result = db.__get_data__(class_connection, query, multi = True)

    return result

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(class_connection, ariel_id):

    query  = f"SELECT id, aql, to_table FROM ariel_search WHERE disable = 0 AND id = {ariel_id}"
    result = db.__get_data__(class_connection, query)
    # result = db.get_single_data(query)

    return result

def new_get_aql(class_connection):

    query  = f"SELECT id, aql, to_table FROM ariel_search WHERE disable = 0"
    result = db.__get_data__(class_connection, query, multi = True)

    return result

def select_aql(list_aql : list, aql_id : int):
    output_var = {}
    index = 0
    while index < len(list_aql):
        
        if list_aql[index]['id'] == aql_id:
            output_var = list_aql[index]
            break
        index += 1

    return output_var

def get_columns(class_connection, tabla):
    query   = f"SHOW COLUMNS FROM {tabla}"
    result  = db.__get_data__(class_connection, query, multi = True)
    # result  = db.get_multi_data(query)

    columns = []
    for x in result:
        if x['Field'] not in ('id', 'create_time'): # Columnas ignoradas
            columns.append(x['Field'])

    return columns

def new_get_columns(class_connection, table : str, list_table_columns : dict):
    
    if len(list_table_columns) > 0:
        list_tables = list(list_table_columns.keys())
    else:
        list_tables = []
    
    # Añade a la memoria una nueva estructura de tabla, si esta no estaba pre-cargada
    if table not in list_tables:
        query   = f"SHOW COLUMNS FROM {table}"
        result  = db.__get_data__(class_connection, query, multi = True)
        
        columns = []
        
        for x in result:
            if x['Field'] not in ('id', 'create_time'): # Columnas ignoradas
                columns.append(x['Field'])

        list_table_columns[table] = columns

    list_table_columns['selected'] = list_table_columns[table]

    return list_table_columns

# Inserta los resultados obtenidos en QRadar a la base de datos
def insert_results(class_connection, table, table_columns, semaforo_id, result):  
    columns    = ", ".join(table_columns)
    list_query = []
    list_data  = []

    # Realizar las operaciones necesarias en la base de datos
    for datas in result["events"]:

        # Seleccion de los valores a insertar en DB
        temp_list = []
        temp_list.append(semaforo_id)
        for values in datas.values():
            temp_list.append(values)

        # Creacion de la query de INSERT en DB
        insert_values = str("%s, " * len(temp_list)).rstrip(', ')
        list_query.append(f"INSERT INTO {table} ({columns}) VALUES ({insert_values})")
        list_data.append(tuple(temp_list))

    db.__insert_multi_data__(class_connection, list_query, list_data)
    # db.multi_insert(list_query, list_data)

# Inserta los resultados obtenidos en QRadar a la base de datos y retorna el total de datos insertados si todo sale bien
def new_insert_results(class_connection, table, table_columns, semaforo_id, result):  
    
    def custom_insert(class_connection, list_query, list_data = None):
        errors = False
        
        try:
            # Iniciar una transacción  
            class_connection.cursor.execute("START TRANSACTION")

            # Realizar las operaciones necesarias en la base de datos
            if list_data == None:
                for query in list_query:
                    class_connection.cursor.execute(query)
            else:
                index = 0
                for query in list_query:
                    class_connection.cursor.execute(query, list_data[index])
                    index += 1

            # Si todo ha ido bien, confirmar la transacción
            class_connection.cursor.execute("COMMIT")
        except:
            # Si se produce un error, deshacer la transacción
            class_connection.cursor.execute("ROLLBACK")
            errors = True

        return errors

    delete_query = f"DELETE FROM {table} WHERE semaforo_id = {semaforo_id}"
    db.__insert_data__(class_connection, delete_query)
    
    columns    = ", ".join(table_columns)
    list_query = []
    list_data  = []

    # Realizar las operaciones necesarias en la base de datos
    for datas in result["events"]:

        # Seleccion de los valores a insertar en DB
        temp_list = []
        temp_list.append(semaforo_id)
        for values in datas.values():
            temp_list.append(values)

        # Creacion de la query de INSERT en DB
        insert_values = str("%s, " * len(temp_list)).rstrip(', ')
        list_query.append(f"INSERT INTO {table} ({columns}) VALUES ({insert_values})")
        list_data.append(tuple(temp_list))

    detected_errors = custom_insert(class_connection, list_query, list_data)

    if detected_errors == True:
        total_save = 0
    else:
        total_save = len(list_query)

    return total_save

def save_validation(class_connection, qr_result, semaforo_id, table):
    query     = f"SELECT count(id) FROM {table} WHERE semaforo_id = {semaforo_id}"
    db_result = db.__get_data__(class_connection, query)

    if len(qr_result['events']) == db_result[0]:
        return True
    else:
        return False

def new_save_validation(qr_result, total_guardados):
    
    if len(qr_result['events']) == total_guardados:
        return True
    else:
        return False

def init_update_semaforo(class_connection, semaforo_id):
    query = f"UPDATE semaforo SET in_excecution_date = NOW(), in_excecution = True, aql_start = True WHERE id = {semaforo_id}"
    db.__insert_data__(class_connection, query)
    # db.single_insert(query)

def end_update_semaforo(class_connection, semaforo_id):
    query = f"UPDATE semaforo SET aql_end = True, save_result = True, retry = NULL, expire = NULL WHERE id = {semaforo_id}"
    db.__insert_data__(class_connection, query)
    # db.single_insert(query)

# Activa o desactiva los mensajes, puede generar conflicto con el crontab y los mensajes se muestran por consola
def manual_message(activar_mensajes, nro_msg, extra_params = {}):
    if activar_mensajes:
        if nro_msg == 0: 
            print((datetime.now() - extra_params['init_date']) / timedelta(seconds=1), " Segundos")  # Muy importante, tiempo transcurrido en segundos
        elif nro_msg == 1:
            print("No se encontraron Semaforos/Tareas pendientes")
        elif nro_msg == 2:
            print()
        elif nro_msg == 1:
            ""

# cuenta el tiempo que tarda un segmento
def timer(internal_timer, texto):
    print(f"El proceso -{texto}- tardo {round((datetime.now()-internal_timer)/timedelta(seconds=1), 2)} Segundos")

# ======================
# Ejecucion del script
# ======================

init_date        = datetime.now()

class_connection = db.__try_open__(db.connection_params, True)
list_aql         = new_get_aql(class_connection)
semaforos        = get_semaforos(class_connection) # Consultando las querys fallidas

if semaforos == [] or semaforos == None:
    temporizador = (datetime.now() - init_date) / timedelta(seconds=1)
    print(f"{temporizador} Segundos, No se encontraron Semaforos/Tareas pendientes")

else:

    print(f"RANGO DE FECHAS UTILIZADO: {semaforos[0]['start_time']}  ~  {semaforos[-1]['stop_time']}")
    print("Ejecutando Semaforos/Tareas marcadas")
    list_table_columns = {}
    last_index         = len(semaforos)
    index              = 0

    while len(semaforos) > 0:

        # Guardado en variables de la informacion del semaforo/tarea, para facilitar la lectura        
        semaforo    = semaforos.pop(0)
        semaforo_id = semaforo['id']
        aql_id      = semaforo['ariel_id']
        start_time  = semaforo['start_time']
        stop_time   = semaforo['stop_time']
        
        print(f"{(index+1)} / {last_index} Tareas reintentadas, Reintento correspondiente a {start_time}")
   
        # aql    = select_aql(list_aql, aql_id)
        aql    = get_aql(class_connection, aql_id)
        table  = aql['to_table'] # Obtencion de la tabla destino
        aql    = aql['aql'] # Obtencion de la query AQL
        aql    = replace_query(aql, start_time, stop_time)
 
        # table_columns = get_columns(class_connection, table) # Obtencion de las columnas de la tabla
        list_table_columns = new_get_columns(class_connection, table, list_table_columns) # Memoria de estructura de tablas
        table_columns      = list_table_columns['selected']                               # Carga de estructura de tabla
        
        init_update_semaforo(class_connection, semaforo_id)        
        
        qr_results = qradar.get_qradar("search_generate", aql, False)
        
        if qr_results != None:

            # insert_results(class_connection, table, table_columns, semaforo_id, qr_results)
            total_guardados = new_insert_results(class_connection, table, table_columns, semaforo_id, qr_results)
        
            # if save_validation(class_connection, qr_results, semaforo_id, table):
            if new_save_validation(qr_results, total_guardados):
                end_update_semaforo(class_connection, semaforo_id)
        
        # sleep(0.05)
        index += 1

    temporizador = round(((datetime.now() - init_date) / timedelta(seconds=1)), 2)
    print(f"Fin del script, tiempo transcurrido: {temporizador} Segundos \n") # Muy importante, tiempo transcurrido en segundos

class_connection = db.__try_close__(class_connection)