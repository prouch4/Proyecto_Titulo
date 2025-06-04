# Archivo Principal:
# Controla los semaforos, obtencion de AQLs para guardar los resultados en la base de datos
# No permite la obtencion de informacion antigua o reintentar los semaforos fallidos

import requests
import urllib.parse
from urllib3 import disable_warnings
import mysql.connector

from datetime import datetime
from datetime import timedelta
from time import sleep

connection_params = {
    "host" : "as-mysql-01.c8s4pjwjpeb6.us-east-1.rds.amazonaws.com",
    "user" : "admin",
    "password" : "4rqu1t3ctur4.$",
    "database" : "qradar_portal"
}

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(connection_params):
    conn = mysql.connector.connect(**connection_params)

    result = None

    # Crear un cursor para realizar operaciones en la base de datos
    cursor = conn.cursor()
    try:

        cursor.execute("SELECT id, aql, to_table FROM ariel_search WHERE disable = 0")
        result = cursor.fetchall()
    except mysql.connector.Error as error:
        
        # Si se produce un error, deshacer la transacción
        print("Error al consultar la base de datos: {}".format(error))

    # Cerrar la conexión a la base de datos
    cursor.close()
    conn.close()

    return result

# Genera el perdiodo de 5 minutos para limitar el AQL
def get_date_range():
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

    start = stop - timedelta(minutes=5)

    return (start, stop)
 
# Limita el AQL con el periodo de 5 minutos
def replace_query (aql, dates) :

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(dates[0]))
    aql = aql.replace(":stop_time:", str(dates[1]))
    
    return aql

# Crea el semaforo en la base de datos (qradar_portal.semaforo)
def initialize_semaforo(connection_params, query_id, dates):
    conn = mysql.connector.connect(**connection_params)

    semaforo_id = None

    # Creacion del semaforo en la base de datos
    cursor = conn.cursor()
    try:
        # Iniciar una transacción  
        cursor.execute("START TRANSACTION")

        # Realizar las operaciones necesarias en la base de datos
        insert = "INSERT INTO semaforo (ariel_id, start_time, stop_time, in_excecution_date, in_excecution) VALUES (%s, %s, %s, NOW(), True)"
        data = (query_id, dates[0], dates[1])
        cursor.execute(insert, data)

        # Si todo ha ido bien, confirmar la transacción
        cursor.execute("COMMIT")

        # Obtencion del ID creado
        cursor.execute("SELECT LAST_INSERT_ID()")
        semaforo_id = cursor.fetchone()[0]
        
    except mysql.connector.Error as error:
        # Si se produce un error, deshacer la transacción
        print("Error al crear en la base de datos: {}".format(error))
        cursor.execute("ROLLBACK")

    # Cerrar la conexión a la base de datos
    cursor.close()
    conn.close()

    return semaforo_id

# Actualiza la tarea en curso del semaforo de la base de datos (qradar_portal.semaforo)
def update_semaforo (connection_params, semaforo_id, columna, value):
    conn = mysql.connector.connect(
        host = connection_params["host"], 
        user = connection_params["user"], 
        password = connection_params["password"], 
        database = connection_params["database"]
    )
    
    # Creación del semaforo en la base de datos
    cursor = conn.cursor()
    try:
        # Iniciar una transacción  
        cursor.execute("START TRANSACTION")

        # Realizar las operaciones necesarias en la base de datos
        update = "UPDATE semaforo SET {0} = {1} WHERE id = {2}".format(columna, value, semaforo_id)
        cursor.execute(update)

        # Si todo ha ido bien, confirmar la transacción
        cursor.execute("COMMIT")
    except mysql.connector.Error as error:

        # Si se produce un error, deshacer la transacción
        print("Error al crear en la base de datos: {0}".format(error))
        cursor.execute("ROLLBACK")

    # Cerrar la conexión a la base de datos
    cursor.close()
    conn.close()

# Envia el AQL limitado al periodo actual a QRadar, obtine un JSON o un NONE si falla
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

# Obtencion de la estructura de la tabla de destino, funcion usada para la generacion del insert
def get_columns (connection_params, tabla):
    conn = mysql.connector.connect(**connection_params)

    mycursor = conn.cursor()
    mycursor.execute("SHOW COLUMNS FROM {0}".format(tabla))

    columns = []

    result = mycursor.fetchall()
    for x in result:
        if x[0] not in ('id', 'create_time'): # Columnas ignoradas
            columns.append(x[0])

    mycursor.close()
    conn.close()

    return columns

# Inserta los resultados obtenidos en QRadar a la base de datos
def insert_results(connection_params, table, table_columns, semaforo_id, dates, result):  
    columns = ", ".join(table_columns)

    conn = mysql.connector.connect(**connection_params)

    # Crear un cursor para realizar operaciones en la base de datos
    mycursor = conn.cursor()
    try:
        # Iniciar una transacción  
        mycursor.execute("START TRANSACTION")
        
        # Realizar las operaciones necesarias en la base de datos
        for datas in result["events"]:

            # Seleccion de los valores a insertar en DB
            temp_list = []
            temp_list.append(semaforo_id)
            for values in datas.values():
                temp_list.append(values)
            # temp_list.append(dates[0])
            # temp_list.append(dates[1])

            # Creacion de la query de INSERT en DB
            insert_values = """%s, """ * len(temp_list)  
            insert_values = insert_values.rstrip(', ')
            query = """INSERT INTO {0} ({1}) VALUES ({2})""".format(table, columns, insert_values)

            # Ejecucion de la querys        
            mycursor.execute(query, tuple(temp_list))

        # Si todo ha ido bien, confirmar la transacción
        mycursor.execute("COMMIT")
    except mysql.connector.Error as error:
        # Si se produce un error, deshacer la transacción
        print("Error al actualizar la base de datos: {}".format(error))
        mycursor.execute("ROLLBACK")
    
    # Cerrar la conexión a la base de datos
    mycursor.close()
    conn.close()

# Compara el conteo de total de registros obtenidos de QRadar con los guardados en la base de datos
def save_validation (connection_params, qr_result, semaforo_id, table):
    db_result = []
    conn = mysql.connector.connect(**connection_params)

    cursor = conn.cursor()
    try:    
        query = "SELECT * FROM {0} WHERE semaforo_id = {1}".format(table, semaforo_id)
        cursor.execute(query)
        db_result = cursor.fetchall()
    except mysql.connector.Error as error:
        print("Error al consultar la base de datos: {}".format(error))

    cursor.close()
    conn.close()

    if len(qr_result['events']) == len(db_result):
        return True
    else:
        return False
    

# =================================
# Ejecucion del script
# =================================

# Mensaje de Inicio
init_date = datetime.now()
print("\n{0}  :Iniciando Script".format(init_date))

# Acotando fechas
dates = get_date_range()

# Obtencion de los AQL
aqls = get_aql(connection_params)

# Creacion de los semaforos a partir de los AQL activos recibidos de DB
task_dictionary = {} # Guarda los id de los semaforos y los asocia al ariel_id
for aql in aqls:

    ariel_id = aql[0]
    
    semaforo_id = initialize_semaforo(connection_params, ariel_id, dates)
    task_dictionary[ariel_id] = semaforo_id

# Composicion y Ejecucion de los AQLs
for aql in aqls:

    print("{0}  :Ejecutando AQL N°{1}".format(datetime.now(), aql[0]))

    ariel_id = aql[0]
    semaforo_id_actual = task_dictionary[aql[0]]
    complete_aql = replace_query(aql[1], dates)
    to_table = aql[2]
    
    # Obtencion de la estructura de la DB
    table_columns = get_columns(connection_params, to_table)
    
    # Busqueda en QRADAR
    update_semaforo(connection_params, semaforo_id_actual, 'aql_start', True) # update_semaforo(conector, semaforo_id, tarea_actual)
    results = get_qradar_result(complete_aql)
    
    if results != None:
        update_semaforo(connection_params, semaforo_id_actual, 'aql_end', True)
        insert_results(connection_params, to_table, table_columns, semaforo_id_actual, dates, results)

        if save_validation(connection_params, results, semaforo_id_actual, to_table):
            update_semaforo(connection_params, semaforo_id_actual, 'save_result', True)

print("{0}  :Finalizado Script en un tiempo de {1} segundos".format(datetime.now(), str((datetime.now() - init_date) / timedelta(seconds=1))))