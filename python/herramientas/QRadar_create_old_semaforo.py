# Script exclusivamente para generar y traer informacion antigua
# Genera semaforos viejos
from datetime import datetime
from datetime import timedelta
from time import sleep

import connection_db as db

# Parametrizacion de arranque del script de apoyo
date_1          = datetime.now()
query_id_list   = [1,2,3,4,5,6,7,8,9,10,11] 
min_date        = datetime(2024,1,12,13,0,0)
max_date        = datetime(2024,1,12,16,0,0)
start_date      = []
stop_date       = []

print(f"Inicio del script: {date_1}")

# Generacion del listado de fechas
recorrido = min_date
while recorrido <= max_date:
    if recorrido == min_date:
        start_date.append(recorrido)
    elif recorrido == max_date:
        stop_date.append(recorrido)
    else:
        start_date.append(recorrido)
        stop_date.append(recorrido)
    recorrido = recorrido + timedelta(minutes=5)

connection_class = db.__try_open__(db.connection_params, False)

# Recorrido del listado de fechas para la creacion de INSERT
for index in range(len(start_date)):
    print(f"Utilizando: {start_date[index]}")
    
    list_query  = []
    list_data   = []

    # Recorrido de las QUERYS AQLs para crear el INSERT
    for query_id in query_id_list:
        list_query.append("INSERT INTO qradar_portal.semaforo (ariel_id, start_time, stop_time, retry, create_time) VALUES (%s, %s, %s, '1', NOW())")
        list_data.append((query_id, start_date[index], stop_date[index]))
    db.__insert_multi_data__(connection_class, list_query, list_data)
    sleep(0.05)

connection_class = db.__try_close__(connection_class)

script_timer = round(((datetime.now() - date_1) / timedelta(seconds=1)), 2)
print(f"Tiempo transcurrido: {script_timer} Segundos") # Muy importante, tiempo transcurrido en segundos