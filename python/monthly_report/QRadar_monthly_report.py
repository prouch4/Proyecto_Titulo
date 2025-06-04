#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime
from datetime import timedelta
from time import sleep

import connection_qradar as qradar
import connection_db     as db

#####################################
# SCRIPT                            #
#####################################

# Genera el periodo de 1 dia para limitar el AQL
def get_date_range():

    stop  = datetime(datetime.now().year, datetime.now().month, datetime.now().day)
    start = stop - timedelta(days = 1) # Ajustar las fechas
    
    return (start, stop)

# Limita el AQL con el periodo especificado
def replace_query(aql, dates) :

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(dates[0]))
    aql = aql.replace(":stop_time:", str(dates[1]))
    
    return aql

# Obtiene las tareas pendientes de ejecutar
def get_aql_pendientes(class_connection, list_aql, start_date, stop_date):
    list_id = []
    for aql in list_aql:
        list_id.append(str(aql['id']))
    list_id = ",".join(list_id)

    query   = "SELECT id, ariel_id, start_time, stop_time, in_excecution_date, in_excecution, aql_start, aql_end, save_result "
    query  += "FROM semaforo " 
    query  += f"WHERE (start_time = '{start_date}' AND stop_time = '{stop_date}') AND ariel_id IN ({list_id})"
    results = db.__get_data__(class_connection, query, multi = True)

    temp_list = []
    if results == None: # Retorna lo recibido, sin cambios
        return list_aql
    else: # Solo descarta AQLs si recibe algo

        for aql in list_aql:
            detected = False 
            for result in results:
                if (result['ariel_id'] == aql['id']):
                    detected = True

            if detected == False:
                temp_list.append(aql)

        list_aql = temp_list

    return list_aql


# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(class_connection):

    result = None
    query  = "SELECT id, company_id, company_column, aql, to_table FROM ariel_search WHERE disable = 0"
    result = db.__get_data__(class_connection, query, multi = True)
    
    return result

def retry_fail_task(class_connection, init_date, start_date, stop_date):
    
    def get_fail_task(class_connection, init_date, start_date, stop_date):
        
        # CONSULTANDO FALLIDOS
        query   = "SELECT id, ariel_id, start_time, stop_time, in_excecution_date, in_excecution, aql_start, aql_end, save_result "
        query  += "FROM semaforo " 
        query  += f"WHERE (start_time = '{start_date}' AND stop_time = '{stop_date}') "
        query  += "AND (in_excecution IS NULL OR aql_start IS NULL OR aql_end IS NULL OR save_result IS NULL) "
        query  += f"AND (in_excecution_date <= '{(init_date - timedelta(hours = 2))}')"
        results = db.__get_data__(class_connection, query, multi = True)
        
        if results != None:
            # REINICIANDO FALLIDOS
            reset_ids = []
            for result in results:
                reset_ids.append(str(result['id']))
            query = f"UPDATE monthly_report.semaforo SET in_excecution = NULL, aql_start = NULL, aql_end = NULL WHERE (id IN ({','.join(reset_ids)}))"
            db.__insert_data__(class_connection, query)

        return results

    def borrar_fallidos(class_connection, to_table, semaforo_id):
        
        # LIMPIANDO FALLIDOS
        query = f"DELETE FROM monthly_report.{to_table} WHERE (semaforo_id = {semaforo_id});"
        db.__insert_data__(class_connection, query)

    def custom_get_aql(class_connection, aql_id):
        
        result = None
        query  = f"SELECT id, aql, to_table FROM ariel_search WHERE disable = 0 AND id = {aql_id}"
        result = db.__get_data__(class_connection, query, multi = False)
        
        return result

    print("Reintentando AQLs fallidas")
    list_fail = get_fail_task(class_connection, init_date, start_date, stop_date)

    if list_fail == None:
        print("No se encontraron AQLs fallidas")
    else:
        for fail in list_fail:

            internal_date   = datetime.now() # Para calcular el tiempo que se tarda cada sub-tarea
            aql             = custom_get_aql(class_connection, fail['ariel_id'])
            # print(aql)
            aql_query       = replace_query(aql['aql'], date_range)
            semaforo_id     = fail['id']
            to_table        = aql['to_table']
            company_id      = aql['company_id']
            
            print(f"Reintentando informacion del día: {to_table}")
            borrar_fallidos(class_connection, to_table, semaforo_id)

            registrar_semaforo(class_connection, "in_excecution", semaforo_id = semaforo_id)
            registrar_semaforo(class_connection, "aql_start"    , semaforo_id = semaforo_id)
            qradar_response = qradar.get_qradar("search_generate", aql_query, pre_encode = False)
            
            if qradar_response != None:

                registrar_semaforo(class_connection, "aql_end", semaforo_id = semaforo_id)    
                insert_qradar_results(class_connection, qradar_response, to_table, company_id, start_date, stop_date, semaforo_id)
                if validar_guardado(class_connection, qradar_response, semaforo_id, to_table):
                    finalizar_semaforo(class_connection, internal_date, semaforo_id)

    print("Reintento terminado de cargas fallidas")

def registrar_semaforo(class_connection, actividad, start_time = None, stop_time = None, aql_id = 0, semaforo_id = 0):
    
    if actividad == "crear":
        query  = "INSERT INTO semaforo (ariel_id, start_time, stop_time) VALUES (%s, %s, %s)"
        values = (aql_id, start_time, stop_time)
    elif actividad == "in_excecution":
        query  = f"UPDATE semaforo SET in_excecution = %s, in_excecution_date = NOW() WHERE (id = %s)"
        values = (1, semaforo_id)
    elif actividad != "crear" and actividad != "in_excecution":
        query  = f"UPDATE semaforo SET {actividad} = %s WHERE (id = %s)"
        values = (1, semaforo_id)

    db.__insert_data__(class_connection, query, values)
    
    if actividad == "crear":
        semaforo_id = db.__get_data__(class_connection, "SELECT LAST_INSERT_ID()")['LAST_INSERT_ID()']
    
    return semaforo_id

def finalizar_semaforo(class_connection, internal_date, semaforo_id = 0):

    timer = round((datetime.now() - internal_date) / timedelta(seconds = 1), 1)
    
    query  = "UPDATE semaforo SET save_result = %s, timer = %s WHERE (id = %s)"
    values = (1, timer, semaforo_id)

    db.__insert_data__(class_connection, query, values)
    
def insert_qradar_results(class_connection, qradar_response : dict, to_table, company_id, company_column, start_date, stop_date, semaforo_id = 0):
    
    if qradar_response['events'] != [] and qradar_response['events'] != None:
    
        list_query = []
        list_value = []

        for event in qradar_response['events']:
            
            company_id = get_company_id(company_id, company_column, event)

            values = list(event.values())
            values.append(company_id)
            values.append(semaforo_id)
            values.append(start_date)
            values.append(stop_date)
            values = tuple(values)

            columns = list(event.keys())
            columns.append('company_id')
            columns.append('semaforo_id')
            columns.append('start_date')
            columns.append('stop_date')
            columns = ", ".join(columns)

            escape_values = str("%s, "*len(values))[:-2]
            query         = f"INSERT INTO {to_table} ({columns}) VALUES ({escape_values})"

            list_query.append(query)
            list_value.append(values)

        db.__insert_multi_data__(class_connection, list_query, list_value)

def validar_guardado(class_connection, qradar_response, semaforo_id, to_table):

    query  = f"SELECT coalesce(count(id), 0) as registros_insertados FROM {to_table} WHERE semaforo_id = {semaforo_id}"
    try:
        result = db.__get_data__(class_connection, query)['registros_insertados']
    except:
        result = 0

    if result == len(qradar_response['events']):
        return True
    else:
        return False

# Remplaza el company_id si detecta la existencia de uno dinamico
def get_company_id(company_id, company_column, qradar_event):
    
    if company_column != None:
        try:
            company_id = qradar_event[company_column]
        except:
            pass

    return company_id

sleep(60)
init_date = datetime.now() # Duracion de ejecucion
print(f"Script iniciado: {init_date}")

# Pre-configuracion de fechas
date_range = get_date_range()
start_date = date_range[0] # Fecha de inicio  del semaforo/start_date
stop_date  = date_range[1] # Fecha de termino del semaforo/stop_date

# Apertura de la conexion con DB
class_connection = db.__try_open__(db.connection_params_2, True)

# Obtencion de las AQL
print("Obtencion de AQLs")
list_aql    = get_aql(class_connection)                                             # AQL activas
list_aql    = get_aql_pendientes(class_connection, list_aql, start_date, stop_date) # Se confirma si hoy se ejecutaron las AQL

if list_aql == []:
    print("No se encontaron AQLs activas")

for aql in list_aql:

    # Para calcular el tiempo que se tarda cada sub-tarea en segundos, vease semaforo.timer en DB
    internal_date  = datetime.now() 

    aql_id         = aql['id']
    to_table       = aql['to_table']
    company_id     = aql['company_id']
    company_column = aql['company_column']
    aql_query      = replace_query(aql['aql'], date_range)
    semaforo_id    = registrar_semaforo(class_connection, "crear", start_date, stop_date, aql_id)

    print(f"Cargando informacion del día: {to_table}")

    registrar_semaforo(class_connection, "in_excecution", semaforo_id = semaforo_id)
    registrar_semaforo(class_connection, "aql_start"    , semaforo_id = semaforo_id)
    qradar_response = qradar.get_qradar("search_generate", aql_query, pre_encode = False)

    if qradar_response != None:

        registrar_semaforo(class_connection, "aql_end", semaforo_id = semaforo_id)    
        insert_qradar_results(class_connection, qradar_response, to_table, company_id, company_column, start_date, stop_date, semaforo_id)
        if validar_guardado(class_connection, qradar_response, semaforo_id, to_table):
            finalizar_semaforo(class_connection, internal_date, semaforo_id)
        
retry_fail_task(class_connection, init_date, start_date, stop_date)

# Cierre de la conexion con DB
class_connection = db.__try_close__(class_connection)

timer = round((datetime.now() - init_date) / timedelta(seconds=1), 2)
print(f"Script finalizado: {timer} Segundos\n")