#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Complementario, interactua con los tickets de FRESH SERVICE guardados
# en la base de datos y su adaptacion a OTRS, actualizando los tickets adaptados en caso de
# existir un cambio en los campos o su traducción

#####################################
# IMPORTS NECESARIOS                #
#####################################

import correction_for_db as patch
import connection_db     as db2 # Archivo de pruebas

from datetime import datetime
from datetime import timedelta

from time import sleep

#####################################
# SCRIPT                            #
#####################################

init_date = datetime.now()
print(f"{init_date} Inicio del script")

# query           = "SELECT min(fs_id), max(fs_id) FROM z_fs_ticket"
query           = "SELECT 39363 AS minimum, max(fs_id) FROM z_fs_ticket"
results         = db2.get_single_data(query = query, dictionary = True)

# first_ticket    = results["min(fs_id)"]
first_ticket    = results["minimum"]
last_ticket     = results["max(fs_id)"]
contador        = None

try:
    registros_aprox = (last_ticket - first_ticket)
except:
    registros_aprox = "Error"

columns = (
    # FRESH SERVICE     # -> OTRS OLD
    "fs_id",            # -> tn
    "subject",          # -> title
    "group_id",         # -> queue_id
    "category",         # -> type_id && service_id
    "sub_category",     # -> type_id && service_id
    "item_category",    # -> type_id && service_id
    "plataforma",       # -> type_id && service_id
    "producto",         # -> type_id && service_id
    "sla_policies_id",  # -> type_id && service_id && sla_id
    "requester_id",     # -> user_id && responsible_user_id && create_by && change_by
    "priority",         # -> ticket_priority_id
    "impact",           # -> ticket_priority_id
    "urgency",          # -> ticket_priority_id
    "status",           # -> ticket_state_id
    "department_id",    # -> customer_id
    "created_at",       # -> create_time
    "updated_at",       # -> change_time
)
selected_columns = ", ".join(columns)

while first_ticket <= last_ticket:

    query = f"select {selected_columns} from z_fs_ticket where fs_id >= {first_ticket} LIMIT 1000"
    results = db2.get_multi_data(query=query, dictionary=True)

    while len(results) > 0:
        result      = results.pop(0)
        ticket_otrs = patch.modification_for_otrs(result)
        
        update_columns = ""
        for key in ticket_otrs:
            update_columns += f"{key} = %s, "
        update_columns = update_columns.rstrip(', ')
        query          =   f"UPDATE ticket SET {update_columns} where tn = {ticket_otrs['tn']}"
        db2.single_insert_disable_keys(query, list(ticket_otrs.values()))

        # Mensaje actual
        contador_temp = round(((result['fs_id'] / last_ticket)*100), 1)
        if contador != contador_temp or len(results) == 0:
            contador = contador_temp
            print(f"Progreso actual: {contador}%, Total de registros {last_ticket}")

    first_ticket = ticket_otrs["tn"]
    if first_ticket == last_ticket:
        break

script_timer = round(((datetime.now() - init_date) / timedelta(seconds = 1)), 2)
print(f"{datetime.now()} Fin del script \n Tiempo de ejecución: {script_timer} Segundos \n")
