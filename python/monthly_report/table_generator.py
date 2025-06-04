from datetime import datetime, timedelta
from time     import sleep
from json     import dumps
from re       import findall

import connection_qradar as qradar
import connection_db     as db

def get_aql(class_connection):

    result = None
    query  = "SELECT id, aql, to_table FROM ariel_search WHERE disable = 1"
    result = db.__get_data__(class_connection, query, multi = True)
    
    return result

# Detecta fechas dentro de los textos recibidos
def detect_datetime(value : str):
    regex          = r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})"
    date_detection = findall(regex, value)
    if date_detection != [] and date_detection != None:
        return True
    else:
        return False

def replace_query(aql) :

    # Sustitucion de los placeholder por los valores del diccionario
    aql = aql.replace("START ':start_time:' STOP ':stop_time:'", "LIMIT 1")
    
    return aql

def create_table_columns(query, result_columns):
    for data in query:
        if type(query[data])    == str:
            if len(query[data]) == 19 and detect_datetime(query[data]) == True:
                result_columns.append(f"`{data}` DATETIME DEFAULT NULL")
            elif len(query[data])   < 256:
                result_columns.append(f"`{data}` VARCHAR(255) DEFAULT NULL")
            elif len(query[data])   < 512:
                result_columns.append(f"`{data}` VARCHAR(512) DEFAULT NULL")
            elif len(query[data])   < 1024:
                result_columns.append(f"`{data}` VARCHAR(1024) DEFAULT NULL")
        elif type(query[data])  == float:
            result_columns.append(f"`{data}` BIGINT DEFAULT NULL")
        elif type(query[data])  == int:
            result_columns.append(f"`{data}` BIGINT DEFAULT NULL")
        else: 
            result_columns.append(f"`{data}` TEXT DEFAULT NULL")

    return result_columns

default_colunms_first = [
    "`id` bigint NOT NULL AUTO_INCREMENT"
]

default_colunms_last = [
    "`company_id` bigint NOT NULL DEFAULT '0'",
    "`semaforo_id` bigint NOT NULL",
    "`start_date` datetime NOT NULL",
    "`stop_date` datetime NOT NULL",
    "PRIMARY KEY (`id`)",
]

print("Iniciando creacion de tablas")
class_connection = db.__try_open__(db.connection_params_2, True)

aqls = get_aql(class_connection)
if aqls != None:
    for aql in aqls:
        query_id   = aql['id']
        table_name = aql['to_table']
        query      = replace_query(aql['aql'])
        query      = qradar.get_qradar("search_generate", query, pre_encode = False)

        result_columns = []
        if query != None and query['events'] != []:
            print(f"Creando tabla: {table_name}")
            query          = query['events'][0]
            result_columns = create_table_columns(query, result_columns)

            temp_columns   = []
            temp_columns.extend(default_colunms_first)
            temp_columns.extend(result_columns)
            temp_columns.extend(default_colunms_last)

            temp_columns = ", ".join(temp_columns)
            temp_columns = f"CREATE TABLE `{table_name}` ({temp_columns})"

            db.__insert_data__(class_connection, temp_columns)
            
            update_query = f"UPDATE `monthly_report`.`ariel_search` SET `disable` = '0' WHERE (`id` = '{query_id}')"
            db.__insert_data__(class_connection, update_query)
        else:
            print(f"No se encontro información: {table_name}")

    class_connection = db.__try_close__(class_connection)
    print("Creacion de tablas, Finalizado")