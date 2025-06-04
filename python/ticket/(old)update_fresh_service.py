#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Actualiza los ticket guardados en la base de datos con estado distinto a cerrado,
# volviendo a consultarlos por medio de la API de fresh service

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime, timedelta

import correction_for_db as patch
import get_fresh_service as get_fs
import connection_db     as db2

#####################################
# SCRIPT                            #
#####################################

def get_table_columns(table):
    query       = f"SHOW COLUMNS FROM {table}"
    result      = db2.get_multi_data(query)
    db_columns  = []

    for column in result:
        db_columns.append(column[0])
    
    return db_columns

def columns_detector(insert_data, columns_db, table):

    for key in list(insert_data.keys()):
        datatype = None
        
        if key == "attachments":    # Ignora attachments
            pass
        
        elif key in columns_db:     # La columna ya existe
            pass
        
        else:                       # Creando nueva columna
            new_column = key
            if isinstance(insert_data[key], str):
                datatype = "TEXT"
            elif isinstance(insert_data[key], bool):
                datatype = "BOOLEAN"
            elif isinstance(insert_data[key], int):
                datatype = "BIGINT"
            else:
                datatype = "TEXT"
                print(f"DESCONOCIDO - {key} - {type(insert_data[key])}")

        if datatype != None:        # Manipulacion de DB
            query = f"ALTER TABLE {table} ADD {new_column} {datatype}"
            db2.single_insert(query)

    return columns_db

def get_tickets_no_closed(limit = False):
    
    query   = "SELECT fs_id, updated_at FROM z_fs_ticket WHERE status NOT IN (5, 9) AND deleted = 0"
    
    if limit == True:
        limit_date = datetime.now() - timedelta(days = 10)
        query   = f"SELECT fs_id, updated_at FROM z_fs_ticket WHERE status AND deleted = 0 AND updated_at >= '{limit_date}'"

    results = db2.get_multi_data(query)

    if not results:
        return None
    else:
        list_ids = []
        
        while len(results) > 0:
            result = results.pop(0)
            id = result[0]
            updated_date = result[1]

            list_ids.append({"id" : id, "updated" : updated_date})

        return list_ids

def update_detector(tickets, updated_at):
    ticket_update   = datetime.strptime(tickets["updated_at"], '%Y-%m-%d %H:%M:%S')
    
    if updated_at < ticket_update:
        return True
    else:
        return False

columns_db          = get_table_columns("z_fs_ticket") 
tickets_pendientes  = get_tickets_no_closed(limit = False)

last_id = tickets_pendientes[len(tickets_pendientes)-1]["id"] # El ticket con id mas grande

while len(tickets_pendientes) > 0:
    
    pendiente       = tickets_pendientes.pop(0)
    pendiente_id    = pendiente["id"]
    updated         = pendiente["updated"]
    
    print(f"{pendiente_id} / {last_id}") # Visual, estimado de tickets faltantes

    # tickets = get_fs.fresh_service_get("ticket_specific", pendiente_id)
    tickets = get_fs.fresh_service_get("ticket_specific_2", pendiente_id)
    
    if isinstance(tickets, dict):
        # tickets = get_fs.custom_field_corrector(tickets)
        tickets = get_fs.dictionary_field_corrector(tickets)

        # LIMPIEZA DE VARIABLE
        tickets = tickets["ticket"][0]

        if patch.autotesting_checker(tickets) == False:
            if update_detector(tickets, updated):

                # INSERT EN OTRS (COPIA)
                ticket_otrs = patch.modification_for_otrs(tickets)
                update_columns = ""
                for key in ticket_otrs:
                    update_columns += f"{key} = %s, "
                update_columns = update_columns.rstrip(', ')
                query          =   f"UPDATE ticket SET {update_columns} WHERE tn = {pendiente_id}"
                db2.single_insert_disable_keys(query, list(ticket_otrs.values()))
                del update_columns, query
                del ticket_otrs

                # INSERT EN FS (LOCAL)
                ticket_fs = tickets
                ticket_fs["fs_id"] = ticket_fs.pop("id")
                
                # DESCARTADOS
                ticket_fs.pop("attachments")        # POR FUTURO INSERT
                ticket_fs.pop("description")        # REPETIDO DE DESCTRIPCION_TEXT
                ticket_fs.pop("description_text")   # PESA DEMASIADO
                
                # AQUI SE PUEDEN COMPARAR
                columns_db = columns_detector(ticket_fs, columns_db, "z_fs_ticket")
                
                update_columns = ""
                for key in ticket_fs:
                    update_columns += f"{key} = %s, "
                update_columns = update_columns.rstrip(', ')
                query          =   f"UPDATE z_fs_ticket SET {update_columns} where fs_id = {ticket_fs['fs_id']}"
                db2.single_insert(query, list(ticket_fs.values()))
                del update_columns, query
                del ticket_fs

print("")