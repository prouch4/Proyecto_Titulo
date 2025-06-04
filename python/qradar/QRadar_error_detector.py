from datetime import datetime
from datetime import timedelta

import connection_db as db

def get_date():
    
    # Creacion de la fecha de termino e inicio
    today          = datetime.now()
    minute         = today.time().minute
    
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

def get_semaforos(date):
    
    query  = f"SELECT group_concat(id) FROM semaforo WHERE (save_result IS NULL OR aql_end IS NULL OR aql_start IS NULL) AND in_excecution_date <= '{date}' ORDER BY start_time DESC"
    result = db.get_single_data(query)
    
    return result

def semaforo_set_nulls(semaforos):
    
    query = f"UPDATE semaforo SET in_excecution_date = NULL, in_excecution = NULL, aql_start = NULL, aql_end = NULL, save_result = NULL WHERE id IN ({semaforos})"
    db.single_insert(query)

def retry_mark(semaforos):
    
    # Creacion del semaforo en la base de datos
    query = f"UPDATE semaforo SET retry = 1 WHERE id IN ({semaforos})"
    db.single_insert(query)
    
# =============================
# Ejecucion del script
# =============================

# Para medir el tiempo de ejecucion
date_1 = datetime.now()

# Fecha para traer las semaforos que llevan mas de 30 minutos sin finalizar
in_excecution_time_limit = get_date() # Fecha actual aproximada en periodos de 5 minutos
in_excecution_time_limit = (in_excecution_time_limit - timedelta(minutes = 30)) # Retroceso en 30 minutos

# Trae los semaforos que no se han completado
semaforos = get_semaforos(in_excecution_time_limit)[0] # Retorna un string con todos los ID
print(str((datetime.now() - date_1) / timedelta(seconds=1))) # Muy importante, tiempo transcurrido en segundos

if semaforos != None:
    
    # Eliminador de comas al final
    if semaforos[-1] == ",":
        semaforos = semaforos[:-1]
    
    semaforo_set_nulls(semaforos) # Limpia los semaforos
    retry_mark(semaforos)         # Marca los semaforos que se reintentaran
print(str((datetime.now() - date_1) / timedelta(seconds=1))) # Muy importante, tiempo transcurrido en segundos