#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime
from datetime import timedelta
from time     import sleep
from json     import dumps

import connection_qradar as qradar
import connection_db     as db

#####################################
# SCRIPT                            #
#####################################

# Genera el perdiodo de 1 DIA para limitar el AQL PELIGRO, USO INVERSO
def get_date_range(start_date):

    start_date = start_date
    stop_date  = start_date + timedelta(days = 1)
    
    return (start_date, stop_date)
 
# Limita el AQL con el periodo de 5 minutos
def replace_query(aql, dates) :

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(dates[0]))
    aql = aql.replace(":stop_time:", str(dates[1]))
    
    return aql

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(class_connection, aql_selector):

    result = None
    query  = f"SELECT id, company_id, company_column, aql, to_table FROM ariel_search WHERE id = {aql_selector}" # Cambiar el id para traer solo lo de una tabla
    result = db.__get_data__(class_connection, query, multi = True)
    
    return result

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_multi_aql(class_connection):

    result = None
    query  = f"SELECT id, company_id, company_column, aql, to_table FROM ariel_search WHERE disable = 0"
    result = db.__get_data__(class_connection, query, multi = True)
    
    return result

def registrar_semaforo(class_connection, actividad, start_time = None, stop_time = None, aql_id = 0, semaforo_id = 0):
    
    if actividad == "crear":
        query  = "INSERT INTO semaforo (ariel_id, start_time, stop_time, in_excecution, in_excecution_date) VALUES (%s, %s, %s, %s, NOW())"
        values = (aql_id, start_time, stop_time, 1)
    elif actividad != "crear":
        query  = f"UPDATE semaforo SET {actividad} = %s WHERE (id = %s)"
        values = (1, semaforo_id)

    db.__insert_data__(class_connection, query, values)
    
    if actividad == "crear":
        semaforo_id = db.__get_data__(class_connection, "SELECT LAST_INSERT_ID()")[0]
    
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

            query = f"INSERT INTO {to_table} ({columns}) VALUES ({escape_values})"

            list_query.append(query)
            list_value.append(values)

        db.__insert_multi_data__(class_connection, list_query, list_value)

def validar_guardado(class_connection, qradar_response, semaforo_id, to_table):

    query  = f"SELECT coalesce(count(id), 0) as cuenta FROM {to_table} WHERE semaforo_id = {semaforo_id}"
    try:
        result = db.__get_data__(class_connection, query)[0]
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

timer_date   = datetime.now()
init_date    = datetime(2024, 1, 1)
limit_date   = datetime(2024, 3, 20)
#43 a 48
aql_selector = 48

print(f"Script iniciado: {timer_date}")

# Apertura de la conexion con DB
class_connection = db.__try_open__(db.connection_params_2, False)

while init_date < limit_date:
    print(init_date)
    # Pre-configuracion de fechas
    date_range = get_date_range(init_date)
    start_date = date_range[0]
    stop_date  = date_range[1]

    # Obtencion de las AQL
    list_aql    = get_aql(class_connection, aql_selector)
    # list_aql    = get_multi_aql(class_connection)

    for aql in list_aql:

        internal_date = datetime.now() # Para calcular el tiempo que se tarda cada sub-tarea

        aql_id          = aql[0]
        company_id      = aql[1]
        company_column  = aql[3]
        to_table        = aql[4]
        aql_query       = replace_query(aql[3], date_range)
        semaforo_id     = registrar_semaforo(class_connection, "crear", start_date, stop_date, aql_id)
        
        registrar_semaforo(class_connection, "in_excecution", semaforo_id = semaforo_id)
        registrar_semaforo(class_connection, "aql_start"    , semaforo_id = semaforo_id)
        qradar_response = qradar.get_qradar("search_generate", aql_query, pre_encode = False)
        # print(dumps(qradar_response))

        if qradar_response != None:

            registrar_semaforo(class_connection, "aql_end", semaforo_id = semaforo_id)    
            insert_qradar_results(class_connection, qradar_response, to_table, company_id, company_column, start_date, stop_date, semaforo_id)
            if validar_guardado(class_connection, qradar_response, semaforo_id, to_table):
                finalizar_semaforo(class_connection, internal_date, semaforo_id)

    init_date = init_date + timedelta(days = 1)
    sleep(0.1)

# Cierre de la conexion con DB
class_connection = db.__try_close__(class_connection)
print(((datetime.now() - timer_date) / timedelta(seconds=1)))