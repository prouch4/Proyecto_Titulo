#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Fusion de los script, QRadar_script y QRadar_per_hour los cuales se encargaban alternativamente de mantener actualizada la informacion
# cada 1hr o cada 5 minutos en por lotes de 5 minutos utilizando las querys almacenadas en la base de datos

#####################################
# IMPORTS NECESARIOS                #
#####################################

from copy import copy
from time import sleep
from datetime import datetime
from datetime import timedelta

import connection_db as db
import connection_qradar as qr

#####################################
# SCRIPT                            #
#####################################

# Obtencion de las AQL activas
def get_active_aql():
    active_aqls = "SELECT id, aql, to_table FROM qradar_portal.ariel_search WHERE disable = 0"
    active_aqls = db.get_multi_data(active_aqls, dictionary = True)

    return active_aqls

def select_aql(list_aql : list, aql_id : int):
    output_var = {}
    index      = 0
    while index < len(list_aql):
        
        if list_aql[index]['id'] == aql_id:
            output_var = list_aql[index]
            break
        index += 1

    return output_var

# Usando las AQL activas, obtiene la ultima vez que se usaron
def get_last_active_semaforos(active_aqls):
    last_semaforos = None

    if active_aqls != None:
        aql_ids = []

        try:
            for aqls in active_aqls:
                aql_ids.append(str(aqls["id"]))
            aql_ids = ", ".join(aql_ids)
            
            last_semaforos = f"SELECT ariel_id, MAX(stop_time) AS last_date FROM qradar_portal.semaforo WHERE ariel_id IN ({aql_ids}) GROUP BY ariel_id"
            last_semaforos = db.get_multi_data(last_semaforos, dictionary = True)
        except:
            pass
    
    if not last_semaforos:
        return None
    else:
        return last_semaforos

# Obtener las columnas utilizadas de la tabla
def get_table_columns(to_table):
    query = f"SHOW COLUMNS FROM {to_table}"

    columns = []

    result = db.get_multi_data(query)

    for x in result:
        if x[0] not in ('id', 'create_time'): # Columnas ignoradas
            columns.append(x[0])
    
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

# Establece el rango de fechas a utilizar, aqui se puede limitar el tiempo ulitilizado
def get_time_range(modo : int):
    
    def for_defualt():

        stop_time   = datetime(
            year    = datetime.today().year,
            month   = datetime.today().month,
            day     = datetime.today().day,
            hour    = datetime.today().hour,
            minute  = datetime.today().minute,
        )

        # Creacion de la fecha de termino e inicio
        minute = (stop_time.minute // 10**0 % 10)
        if minute > 5:
            stop_time = stop_time - timedelta(minutes = (minute - 5))
        elif minute < 5:
            stop_time = stop_time - timedelta(minutes = minute)

        start_time = stop_time - timedelta(minutes = 5)

        time_range = {
            "start_time" : start_time,
            "stop_time"  : stop_time, 
        }

        return time_range

    def for_hour():

        stop_time   = datetime(
            year    = datetime.today().year,
            month   = datetime.today().month,
            day     = datetime.today().day,
            hour    = datetime.today().hour,
        )

        start_time  = stop_time - timedelta(hours = 1)

        time_range  = { 
            "start_time" : start_time,
            "stop_time"  : stop_time, 
        }

        return time_range

    def for_day():

        stop_time   = datetime(
            year    = datetime.today().year,
            month   = datetime.today().month,
            day     = datetime.today().day,
        )

        start_time  = stop_time - timedelta(days = 1)

        time_range  = { 
            "start_time" : start_time,
            "stop_time"  : stop_time, 
        }

        return time_range

    output_var = None
    match modo:
        case 1:
            output_var = for_defualt()
        case 2:
            output_var = for_hour()
        case 3:
            output_var = for_day()

    return output_var

def replace_query(aql, start_time, stop_time):

    # Para el futuro, las timezone son una molestia con los cambios de hora
    # Hay que convertir la hora local (invierno/verano) en una neutra para evitar estos problemas
    start_time      = round(start_time.timestamp()) * 1000 # Para la AQL
    stop_time       = round(stop_time.timestamp())  * 1000 # Para la AQL

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(start_time))
    aql = aql.replace(":stop_time:",  str(stop_time))
    
    return aql

# Creacion de las tareas en la base de datos, para su posterior ejecución
def create_semaforos(last_semaforos, time_range):

    for semaforo in last_semaforos: 
        
        temp_range = copy(time_range)
        print(f"==============> Creando semaforos para Ariel_id N° {semaforo['ariel_id']}")

        # corrector para dias sin completar o en curso
        if temp_range["start_time"] <= semaforo["last_date"]:
            temp_range["start_time"] = semaforo["last_date"]

        insert_list = []
        while temp_range["start_time"] < temp_range["stop_time"]:
            temp_stop_time = temp_range["start_time"] + timedelta(minutes = 5)

            insert_dict = {
                "ariel_id"           : semaforo["ariel_id"],
                "start_time"         : temp_range["start_time"],
                "stop_time"          : temp_stop_time,
                "in_excecution_date" : datetime.now(),
                "in_excecution"      : 1,
                "retry"              : 1,
            }

            temp_range["start_time"] = temp_stop_time # Incremento del periodo/fecha

            insert_list.append(insert_dict)

        db.multi_insert(db.connection_params, "semaforo", insert_list)

# Compara la informacion recibida de qradar con la almacenada
def validate_insert(semaforo_id, to_table, results):
    query = f"SELECT COALESCE(COUNT(id), 0) AS total FROM {to_table} WHERE semaforo_id = {semaforo_id}"
    total_insert = db.get_single_data(query)

    try:
        if len(results) == total_insert[0]:
            return True
        else:
            return False
    except:
        return False

# Rellene el semaforo con las marcas faltantes
def update_semaforo(semaforo_id, data_dict = None):
    data_dict = {
        "aql_start"   : 1,
        "aql_end"     : 1,
        "save_result" : 1,
        "retry"       : None,
    }

    update_set = str(" = %s, ".join(list(data_dict.keys()))) + " = %s"
    query      = f"UPDATE semaforo SET {update_set} WHERE id = {semaforo_id}"
    values     = list(data_dict.values())

    db.single_insert(db.connection_params, query, values)

# Añade el ID de tarea a los resultados para guardarlos en la DB
def insert_corrector(qradar_results, to_table, table_columns = None, semaforo_id = 0):
    
    if qradar_results != None:
        qradar_results = qradar_results['events']

        temp_list = []
        for element in qradar_results:
            temp_dict = {"semaforo_id" : semaforo_id}
            temp_dict.update(element)
            temp_list.append(temp_dict)

        qradar_results = temp_list
    else:

        qradar_results = None
    
    list_query = []
    list_data  = []
    for result in qradar_results:

        # Determinando las columnas a usar
        if table_columns == None:
            columns = ",".join(list(result.keys()))
        else:
            columns = ",".join(table_columns)

        # Creacion de la query de INSERT en DB
        escape_values   =   str("%s, " * len(result)).rstrip(', ')
        list_query.append(f"INSERT INTO {to_table} ({columns}) VALUES ({escape_values})")
        list_data.append(tuple(result.values()))


    return {
        'list_query'    : list_query,
        'list_data'     : list_data,
    }


# sleep(15)                           # Pequeño retraso de arranque para reducir margen de error con la hora del dia
init_date          = datetime.now() # Fecha de inicio
list_table_columns = {}
print(f"SCRIPT iniciado a las {init_date}")

class_connection    = db.__try_open__(db.connection_params, True)
time_range          = get_time_range(3)                     # stop_time, Nunca debe manipular
list_aql            = get_active_aql()                      # Llamando las AQL activas
last_semaforos      = get_last_active_semaforos(list_aql)   # Consultado la ultima vez que se ejecutaron
create_semaforos(last_semaforos, time_range)                # Generando semaforos del día

print(f"Fechas estimadas utilizadas {time_range['start_time']}   {time_range['stop_time']}")

# Recorriendo la ultima vez que se ejecutaron las AQL
while len(last_semaforos) > 0:
    semaforo = last_semaforos.pop(0)

    # Mensaje de inicio de la AQL
    print(f"Ejecutando Ariel_id N°{semaforo['ariel_id']} DESDE {time_range['start_time']} HASTA {time_range['stop_time']}")

    # corrector para dias sin completar o en curso
    semaforo_date_filter = copy(time_range)
    if time_range["start_time"] <= semaforo["last_date"]:
        semaforo_date_filter["start_time"] = semaforo["last_date"]
    else:
        semaforo_date_filter["start_time"] = time_range["start_time"]

    # Consulta los semaforos generados dentro del rango de fechas
    pendiente = f"SELECT * FROM semaforo WHERE ariel_id = {semaforo['ariel_id']} AND start_time >= '{semaforo_date_filter['start_time']}' AND stop_time <= '{semaforo_date_filter['stop_time']}' AND retry = 1"
    pendiente = db.get_multi_data(pendiente, dictionary = True)
    
    
    if pendiente == None:
        print(f"==============> Omitiendo Ariel_id N° {semaforo['ariel_id']}")
    else:

        # Obtencion de las columnas de la tabla
        active_aql          = select_aql(list_aql, semaforo['ariel_id'])
        to_table            = active_aql['to_table']
        query_aql           = active_aql['aql']
        list_table_columns  = new_get_columns(class_connection, to_table, list_table_columns)
        table_columns       = list_table_columns['selected']
        # table_columns       = get_table_columns(to_table)

        # Recorrido de los semaforos generados del día
        while len(pendiente) > 0:
            semaforo_actual = pendiente.pop(0)      # Descargando semaforos a medida que se ejecutan
            semaforo_id     = semaforo_actual['id'] # Para el INSERT
            query_aql       = replace_query(query_aql, semaforo_actual['start_time'], semaforo_actual['stop_time'])

#             results = qr.get_qradar("search_generate", query_aql, pre_encode = False)
#             results = insert_corrector(results, to_table, table_columns, semaforo_id)
#             db.multi_insert(results['list_query'], results['list_data'])

#             if validate_insert(semaforo_id, to_table, results['list_query']):
#                 update_semaforo(semaforo_id)
#             else:
#                 print(f"???? {semaforo_id}")

# tiempo_transcurrido = (datetime.now() - init_date) / timedelta(seconds=1) # Fin del script
# print(f"Tiempo total de ejecucion: {str(tiempo_transcurrido)} segundos")
class_connection = db.__try_close__(class_connection)