#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Fusion de ticket_updater.py y update_fresh_service.py

#####################################
# IMPORTS NECESARIOS                #
#####################################

import get_fresh_service as get_fs
import correction_for_db as patch
import connection_db     as db

from datetime     import date, datetime, timedelta
from urllib.parse import quote, unquote
from time         import sleep
from json         import dumps

#####################################
# SCRIPT                            #
#####################################

def __set_date_for_query__(init_date : date = date.today(), limit_days = None):
    
    output_var = init_date - timedelta(days = 90) # Tres meses aprox
    
    return output_var

def __set_date_ranges_for_query__(start_date : date = None, stop_date  : date = None, limit_days = 30):
    
    output_var = []

    # Remplaza los None por valores default
    if start_date == None or stop_date == None:      
        max_date = date.today()
        min_date = max_date - timedelta(days = limit_days) # Ajustar limite de dias aqui
    else:
        max_date = stop_date
        min_date = start_date
    
    loop_date = min_date
    while loop_date < max_date:
        temp_dict = {
            'start_date'    : loop_date,
            'stop_date'     : loop_date + timedelta(days = 1)
        }
        loop_date = temp_dict['stop_date']
        output_var.append(temp_dict)

    # Corrector
    if len(output_var) == 0:
        output_var = None

    return output_var

def __format_ticket__(list_ticket):

    list_ticket_ids = None
    new_tickets     = None

    # Corrector de recorrido
    if "tickets" in (list(list_ticket.keys())):
        container_var = "tickets"
    elif "ticket" in (list(list_ticket.keys())):
        container_var = "ticket"

    while len(list_ticket[container_var]) > 0:

        actual_ticket = list_ticket[container_var].pop(0)

        # DESCARTADOS
        if "attachments" in list(actual_ticket.keys()):
            actual_ticket.pop("attachments")                 # POR FUTURO INSERT
        if "description" in list(actual_ticket.keys()):
            actual_ticket.pop("description")                 # REPETIDO DE DESCTRIPCION_TEXT
        if "description_text" in list(actual_ticket.keys()):
            actual_ticket.pop("description_text")            # PESA DEMASIADO
        
        if "stats" in list(actual_ticket.keys()):
            actual_ticket.update(actual_ticket.pop("stats")) # ELIMINAR SUB-JSON

        # Se sustituye el formato None de la variable por LIST, guarda el primer valor
        if list_ticket_ids == None:
            list_ticket_ids = [actual_ticket['id']]
            new_tickets     = [actual_ticket]
        else:
            list_ticket_ids.append(actual_ticket['id'])
            new_tickets.append(actual_ticket)
    
    return {'list_ticket_ids': list_ticket_ids, 'new_tickets' : new_tickets}

def tickets_to_update(old_tickets, new_tickets):
    
    update_list = None
    
    if old_tickets != None and new_tickets != None:
        for old in old_tickets:
            for new in new_tickets:
                if old['fs_id'] == new['id']:
                    old_date = old['updated_at']

                    if 'T' in str(new['updated_at']) and 'Z' in str(new['updated_at']):
                        new_date = datetime.strptime(new['updated_at'], "%Y-%m-%dT%H:%M:%SZ")
                    else:
                        new_date = datetime.strptime(new['updated_at'], "%Y-%m-%d %H:%M:%S")

                    if old_date == None:
                        # print("fecha none")
                        if update_list == None:
                            update_list = []

                        update_list.append(new)

                    elif old_date < new_date:
                        # print("actualizado")
                        if update_list == None:
                            update_list = []

                        update_list.append(new)

                    break
    
    return update_list

def get_old_tickets(otrs_conn, list_ticket_ids):
    
    old_tickets = None

    if list_ticket_ids != None:
        old_tickets = str("%s, " * len(list_ticket_ids)).rstrip(", ")
        old_tickets = f"SELECT `fs_id`, `closed_at`, `updated_at` FROM `z_fs_ticket` WHERE `fs_id` IN ({old_tickets})"
        old_tickets = db.__get_data__(otrs_conn, old_tickets, list_ticket_ids, multi = True)

    return old_tickets

def update_fs_tickets(otrs_conn, update_tickets):

    update_querys = None
    update_datas  = None
    if update_tickets != None:
        for ticket in update_tickets:
        
            ticket_id      = ticket.pop('id')
            update_values  = list(ticket.values())
            update_values.append(ticket_id)
            update_columns = list(ticket.keys())
            update_columns = str(' = %s, '.join(update_columns) + ' = %s')


            query = f"UPDATE z_fs_ticket SET {update_columns} WHERE fs_id = %s"    
            if update_querys == None or update_datas == None:
                update_querys = []
                update_datas  = []
            
            update_querys.append(query)
            update_datas.append(update_values)

        if update_querys != None and update_datas != None:
            db.__insert_multi_data__(otrs_conn, update_querys, update_datas)

def get_table_columns(otrs_conn, table):
    
    query       = f"SHOW COLUMNS FROM {table}"
    result      = db.__get_data__(otrs_conn, query, multi = True)
    db_columns  = []

    for column in result:
        db_columns.append(column['Field'])
    
    return db_columns

def columns_detector(otrs_conn, insert_data, columns_db, table):

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
            db.__insert_data__(otrs_conn, query)

    return columns_db

def get_tickets_no_closed(otrs_conn, limit = False):
    
    if limit == True:
        limit_date = datetime.now() - timedelta(days = 10)
        query      = f"SELECT fs_id, updated_at FROM z_fs_ticket WHERE status NOT IN (5, 9) AND deleted = 0 AND updated_at >= %s"
        results    = db.__get_data__(otrs_conn, query, (limit_date, ), multi = True)
    elif limit == False:
        query      = "SELECT fs_id, updated_at FROM z_fs_ticket WHERE (status NOT IN (5, 9) OR closed_at IS NULL) AND deleted = 0"
        # query      = "SELECT fs_id, updated_at FROM z_fs_ticket WHERE status NOT IN (9) AND deleted = 0 AND created_at >= '2024-06-01'"
        # query      = "SELECT fs_id, updated_at FROM z_fs_ticket WHERE status NOT IN (9) AND fs_id = 70613"
        # query      = "SELECT fs_id, updated_at FROM z_fs_ticket WHERE status NOT IN (9) AND closed_at is null"
        results    = db.__get_data__(otrs_conn, query, multi = True)

    if not results:
        return None
    
    else:
        list_ids = []  
        while len(results) > 0:
            result       = results.pop(0)
            id           = result['fs_id']
            updated_date = result['updated_at']

            list_ids.append({"id" : id, "updated" : updated_date})

        return list_ids

def update_detector(tickets, updated_at):
    ticket_update   = datetime.strptime(tickets["updated_at"], '%Y-%m-%d %H:%M:%S')
    
    if updated_at < ticket_update:
        return True
    else:
        return False

def get_tickets_id_list(otrs_conn, date_limit : date, str_date_column : str, id_customer : int = None):

    # Query para los tickets por fecha
    if str_date_column == 'created_at':
        query   = f"SELECT fs_id FROM z_fs_ticket WHERE ({str_date_column} >= '{date_limit}')"
    elif str_date_column != 'created_at':
        query   = f"SELECT fs_id FROM z_fs_ticket WHERE ({str_date_column} >= '{date_limit}' OR updated_at IS NULL)"
    
    # Filtro por cliente
    if id_customer != None:
        query = f"{query} AND department_id = 23000038546"
        # CHULETA
        # BNP                                   23000209665 check
        # Habitat                               23000203546 check
        # AAN                                   23000038482 check
        # EVERTEC                               23000038548 check
        # Capital                               23000038553 check
        # SBPay                                 23000038550 check
        # Emaresa                               23000038547 check
        # BDO Chile                             23000038541 check
        # Sinacofi                              23000038551 check
        # COPEC                                 23000157750 check
        # Seguros Continental                   23000070194 check
        # Adaptive Security                     23000035894 check
        # Viña Concha y Toro                    23000038556 check
        # Universidad Diego Portales            23000038555 check
        # Centro de Compensación Automatizado   23000038446 check
        # Banco de Crédito e Inversiones        23000038540 check
        # BTG Pactual Chile                     23000038543 check
        # CMPC                                  23000038545 check
        # Consorcio                             23000038546 check

    results = db.__get_data__(otrs_conn, query, multi = True)
    if not results:
        return None
    
    else:
        list_ids = []    
        while len(results) > 0:
            result = results.pop(0)
            id = result[0]

            list_ids.append(id)

        return list_ids

stop_date       = None # date(2024, 6, 25)
start_date      = None # date(2024, 5, 15)
id_customer     = None # 23000157750
ticket_list     = None

list_date_range = __set_date_ranges_for_query__(start_date, stop_date, limit_days = 30)
otrs_conn       = db.__try_open__(db.connection_params, True)
# ticket_list     = get_tickets_id_list(otrs_conn, date(2024, 3, 1), 'created_at')

### CODIGO VIEJO (????-??-??) ####
# Actualizador de tickets abiertos o pendientes (Pendiente Corregir comunicacion con DB, SCRIP VIEJO)
columns_db          = get_table_columns(otrs_conn, "z_fs_ticket") 
tickets_pendientes  = get_tickets_no_closed(otrs_conn, limit = False)
last_id             = tickets_pendientes[len(tickets_pendientes)-1]["id"] # El ticket con id mas grande

print("Actualizando Tickets en curso")
while len(tickets_pendientes) > 0:
    
    pendiente       = tickets_pendientes.pop(0)
    pendiente_id    = pendiente["id"]
    updated         = pendiente["updated"]
    
    print(f"{pendiente_id} / {last_id}") # Visual, estimado de tickets faltantes

    tickets = get_fs.fresh_service_get("ticket_specific_2", pendiente_id)

    if isinstance(tickets, dict):
        tickets = get_fs.dictionary_field_corrector(tickets)
        
        # LIMPIEZA DE VARIABLE
        tickets = tickets["ticket"][0]
        
        if patch.autotesting_checker(tickets) == False:
            # if update_detector(tickets, updated):
            if True == True:
        
                # INSERT EN OTRS (COPIA)
                ticket_otrs = patch.modification_for_otrs(tickets)
                update_columns = ""
                for key in ticket_otrs:
                    update_columns += f"{key} = %s, "
                update_columns = update_columns.rstrip(', ')
                query          =   f"UPDATE ticket SET {update_columns} WHERE tn = {pendiente_id}"
                db.__insert_data__(otrs_conn, query, list(ticket_otrs.values()))
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
                columns_db = columns_detector(otrs_conn, ticket_fs, columns_db, "z_fs_ticket")
                
                update_columns = ""
                for key in ticket_fs:
                    update_columns += f"{key} = %s, "
                update_columns = update_columns.rstrip(', ')
                query          =   f"UPDATE z_fs_ticket SET {update_columns} where fs_id = {ticket_fs['fs_id']}"
                db.__insert_data__(otrs_conn, query, list(ticket_fs.values()))
                del update_columns, query
                del ticket_fs

### CODIGO MAS NUEVO (2024-06-24) ###

print("Actualizando Tickets cerrados")
if ticket_list == None: # Por ticket_all
    """
    Procesado de los tickets por rango de fechas
    """
    for date_range in list_date_range:

        ticket_count = 0
        ticket_total = 0
        actual_page  = 1 # Valor minimo 1
        first_loop   = True

        print(f"Cargando: {date_range['start_date']} / {date_range['stop_date']}")

        query_date_range = f"updated_at:>'{date_range['start_date']}' AND updated_at:<'{date_range['stop_date']}'"
        base_query       = f"tickets/filter?query=\"{query_date_range} AND status:5\"&page=1&order_type=asc"
        list_ticket      = get_fs.fresh_service_get("custom_end_point", base_query, disable_warning_msg = True)
        if list_ticket != None:
            ticket_count = len(list_ticket['tickets'])
            ticket_total = list_ticket['total']

        while ticket_count < ticket_total:

            if first_loop == True:
                first_loop = False            
            else:
                query_date_range = f"updated_at:>'{date_range['start_date']}' AND updated_at:<'{date_range['stop_date']}'"
                base_query       = f"tickets/filter?query=\"{query_date_range} AND status:5\"&page={actual_page}&order_type=asc"
                list_ticket      = get_fs.fresh_service_get("custom_end_point", base_query)
                
                ticket_count += len(list_ticket['tickets'])
                ticket_total = list_ticket['total']

                if ticket_total == 0:
                    print("Error inesperado ocurrido, no se han recibido tickets")

            if list_ticket != None:
                list_ticket     = get_fs.dictionary_field_corrector(list_ticket)

                temp_dict       = __format_ticket__(list_ticket)
                list_ticket_ids = temp_dict['list_ticket_ids']
                new_tickets     = temp_dict['new_tickets']
                del temp_dict
                
                old_tickets    = get_old_tickets(otrs_conn, list_ticket_ids)
                update_tickets = tickets_to_update(old_tickets, new_tickets)
                update_fs_tickets(otrs_conn, update_tickets)
            
            actual_page += 1

elif len(ticket_list) >= 1: # Por ticket_specific_2
    """
    Procesado de los tickets por listado de ids
    """

    internal_count = 0
    for ticket_specific in ticket_list:
        internal_count += 1
        print(f'Revisando Ticket: {ticket_specific} ({internal_count}/{len(ticket_list)})')
        tickets = get_fs.fresh_service_get("ticket_specific_2", ticket_specific)
        tickets = get_fs.dictionary_field_corrector(tickets)

        # Formatear tickets y obtener listados de id para comparar
        temp_dict       = __format_ticket__(tickets)
        update_tickets  = temp_dict['new_tickets']
        update_fs_tickets(otrs_conn, update_tickets)


otrs_conn   = db.__try_close__(otrs_conn)
print("")