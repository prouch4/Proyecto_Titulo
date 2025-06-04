#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime
from datetime import timedelta

import connection_qradar as qradar
import connection_db     as db

#####################################
# SCRIPT                            #
#####################################

def get_semaforo_data(class_connection, semaforo_id):

    query  = f"SELECT * FROM monthly_report.semaforo WHERE id = {semaforo_id}"
    result = db.__get_data__(class_connection, query, multi = False)

    query  = f"UPDATE monthly_report.semaforo SET in_excecution_date = NOW(), in_excecution = 1, aql_start = NULL, aql_end = NULL WHERE id = {semaforo_id}"
    db.__insert_data__(class_connection, query)

    return result

# Obtine los AQL de la base de datos (qradar_portal.ariel_search)
def get_aql(class_connection, aql_selector):

    result = None
    query  = f"SELECT id, company_id, company_column, aql, to_table FROM ariel_search WHERE disable = 0 AND id = {aql_selector}" # Cambiar el id para traer solo lo de una tabla
    result = db.__get_data__(class_connection, query, multi = True)
    
    
    return result

# Limita el AQL con el periodo especificado
def replace_query(aql, dates) :

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace(":start_time:", str(dates[0]))
    aql = aql.replace(":stop_time:", str(dates[1]))
    
    return aql

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

def borrar_fallidos(class_connection, to_table, semaforo_id):
    
    # LIMPIANDO FALLIDOS
    query = f"DELETE FROM monthly_report.{to_table} WHERE (semaforo_id = {semaforo_id});"
    db.__insert_data__(class_connection, query)

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

def finalizar_semaforo(class_connection, internal_date, semaforo_id = 0):

    timer = round((datetime.now() - internal_date) / timedelta(seconds = 1), 1)
    
    query  = "UPDATE semaforo SET save_result = %s, timer = %s WHERE (id = %s)"
    values = (1, timer, semaforo_id)

    db.__insert_data__(class_connection, query, values)

# Remplaza el company_id si detecta la existencia de uno dinamico
def get_company_id(company_id, company_column, qradar_event):
    
    if company_column != None:
        try:
            company_id = qradar_event[company_column]
        except:
            pass

    return company_id

semaforo_id = 798

print(f"Reintentando el semaforo_id: {semaforo_id}")
class_connection = db.__try_open__(db.connection_params_2, dictionary=True)

internal_date = datetime.now()
semaforo_data = get_semaforo_data(class_connection, semaforo_id)
aql_data      = get_aql(class_connection, semaforo_data['ariel_id'])
aql_query     = replace_query(aql_data[0]['aql'], (semaforo_data['start_time'], semaforo_data['stop_time']))

borrar_fallidos(class_connection, aql_data[0]['to_table'], semaforo_id)

registrar_semaforo(class_connection, "aql_start"    , semaforo_id = semaforo_id)
qradar_result = qradar.get_qradar("search_generate", aql_query, pre_encode = False)
        
if qradar_result != None:

    registrar_semaforo(class_connection, "aql_end", semaforo_id = semaforo_id)
    insert_qradar_results(class_connection, qradar_result, aql_data[0]['to_table'], aql_data[0]['company_id'], aql_data[0]['company_column'], semaforo_data['start_time'], semaforo_data['stop_time'], semaforo_id)
    if validar_guardado(class_connection, qradar_result, semaforo_id, aql_data[0]['to_table']):
        finalizar_semaforo(class_connection, internal_date, semaforo_id)
     
class_connection = db.__try_close__(class_connection)
print("Fin del script")