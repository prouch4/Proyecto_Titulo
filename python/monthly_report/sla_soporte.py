#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Genera copias actualizadas de los tickets que corresponden al SLA, precalculando el cumplimiento

#####################################
# IMPORTS NECESARIOS                #
#####################################

import connection_db as db

from datetime import datetime, date, timedelta
from json     import dumps
from time     import sleep

#####################################
# SCRIPT                            #
#####################################

def get_sla_monitoreo(tipo : str, timer):
    output_var = None

    # Notacion en segundos
    match tipo:
        
        case "abrir ticket": # No se considera
            if timer == None:
                output_var = None
            elif timer <= 900: # 15 minutos
                output_var = True
            else:
                output_var = False

        case "notificar ticket":
            if timer == None:
                output_var = None
            elif timer <= 900: # 15 minutos
                output_var = True
            else:
                output_var = False

        case "cerrar ticket":
            if timer == None:
                output_var = None
            elif timer <= 900: # 15 minutos
                output_var = True
            else:
                output_var = False

    return output_var

def get_last_id_monthly_report(con_monthly, sla_table):
    output_var = {}

    sla_table = [
        'sla_monitoreo',
    ]

    # Base Query
    for table in sla_table:
        base_query  = f"SELECT id FROM {sla_table} ORDER BY id DESC LIMIT 1"
        last_id     = db.__get_data__(con_monthly, base_query, multi = False)

        if last_id == None:
            output_var.update({table : 0})
        else:
            output_var.update({table : last_id['id']})

    return output_var

def check_first_load(con_monthly, sla_table):
    output_var = False

    # Base Query
    base_query  = f"SELECT id FROM {sla_table} ORDER BY id DESC LIMIT 1"
    last_id     = db.__get_data__(con_monthly, base_query, multi = False)

    if last_id != None and last_id != () and last_id != []:
        output_var = True

    return output_var

def get_last_id_ticket(con_ticket):
    output_var = None

    # Base Query
    base_query  = f"SELECT fs_id FROM z_fs_ticket ORDER BY fs_id DESC LIMIT 1"
    last_id     = db.__get_data__(con_ticket, base_query, multi = False)

    if last_id != None:
        output_var = last_id['fs_id']

    return output_var

def get_customer(con_monthly_report):
    
    customers = "SELECT * FROM customers"
    customers = db.__get_data__(con_monthly_report, customers, multi = True)

    return customers

def set_customer(list_customers, department_id):
    
    output_var = None
    
    for customer in list_customers:
        if str(customer['id']) == str(department_id):
            output_var = customer['name']
            break

    return output_var

def get_month(created_at):
    try:
        month = created_at.month
    except:
        month = None
    
    match month:
        case 1:
            month = "enero"
        case 2:
            month = "febrero"
        case 3:
            month = "marzo"
        case 4:
            month = "abril"
        case 5:
            month = "mayo"
        case 6:
            month = "junio"
        case 7:
            month = "julio"
        case 8:
            month = "agosto"
        case 9:
            month = "septiembre"
        case 10:
            month = "octubre"
        case 11:
            month = "noviembre"
        case 12:
            month = "diciembre"

    return month

def get_tickets_z_fs_tickets(con_ticket, per_limit : int = None, offset_limit : int  = None, date_limiter : bool = True, selected_tickets = None):

    # Base Query
    base_query      = "SELECT * FROM z_fs_ticket"
    # Filtros fijos
    default_where = [
        "WHERE (department_id IS NULL OR department_id NOT IN (23000173121,23000038554))", 
        "AND (category IS NULL OR category IN ('Soporte'))",
        "AND (item_category IS NULL OR item_category NOT IN ('Deploy Programado Evertec'))",
        "AND (ticket_fusionado IS NULL OR ticket_fusionado = 0)",
        "AND (deleted != 1)",
        "AND (group_id IS NULL OR group_id NOT IN (23000086944))",
    ]
    default_where = " ".join(default_where)
    
    # Filtros dinamicos
    if date_limiter == True:
        date_limit     = date.today() - timedelta(days = 90)
        default_where += f" AND (created_at >= '{date_limit}')"

    if selected_tickets != None and selected_tickets != []:
        default_where += f" AND (fs_id in ("
        for id in selected_tickets:
            if id == selected_tickets[-1]:
                default_where += f"{id}))"
            else:
                default_where += f"{id},"

    # Limit result per page
    if per_limit != None and offset_limit != None:
        limit_result = f"LIMIT {per_limit} OFFSET {offset_limit}"    
    else:
        limit_result = ""
    
    # Orden
    order_by = "ORDER BY fs_id ASC"
   
    # Armado de QUERY
    tickets = f"{base_query} {default_where} {order_by} {limit_result}"
    tickets = db.__get_data__(con_ticket, tickets, multi = True)

    return tickets

def get_tickets_sla_selected(con_monthly_report, sla_table : str, fs_tickets : list = None, per_limit : int = None, offset_limit : int = None, date_limiter : bool = False):
    
    def create_id_list(fs_tickets):
        
        output_var = list()
        for ticket in fs_tickets:
            output_var.append(str(ticket['fs_id']))

        if len(output_var) == 0:
            output_var = None

        return output_var
    
    sla_tickets = f"SELECT id, updated_at FROM {sla_table}"
    
    # Creando el where para la query de sla_tickets
    where_filter = []
    if fs_tickets != None:
        list_id    = create_id_list(fs_tickets)
        where_filter.append(f"id IN ({','.join(list_id)})")
    if date_limiter == True:
        date_limit = date.today() - timedelta(days = 90)
        where_filter.append(f"(created_at >= '{date_limit}')")
    
    if where_filter == []:
        where_filter = ""
    elif where_filter != []:
        where_filter = f"WHERE {' AND '.join(where_filter)}"

    # Limitar el maximo de resultados
    limit_and_offset = ""
    if per_limit != None and offset_limit != None:
        limit_and_offset = f"LIMIT {per_limit} OFFSET {offset_limit}"

    sla_tickets = f"{sla_tickets} {where_filter} ORDER BY id ASC {limit_and_offset}"
    sla_tickets = db.__get_data__(con_monthly_report, sla_tickets, multi = True)

    # Corrector de listas vacias
    if sla_tickets == None or sla_tickets == []:
        sla_tickets = None

    return sla_tickets

def get_ticket_fusionado(ticket):
    output_var = None # Retornara un True/False durante la deteccion de fusionados
    
    if ticket["ticket_fusionado"] == 1:
        output_var = True
    elif ticket["item_category"] == "Ticket sin Notificación":
        output_var = True
    else:
        output_var = False

    return output_var

def get_problematico(ticket, ticket_fusionado):
    output_var = None # Retornara un True/False durante la deteccion de problemas
    
    if 'Solicitud de Gestion manual' in ticket['subject']:
        output_var = True
    elif '[XXXX]' in ticket['subject']:
        output_var = True
    elif 'test' in ticket['subject'] and ('Pentest' not in ticket['subject'] and 'pentest' not in ticket['subject']):
        output_var = True
    elif 'Test' in ticket['subject'] and ('Pentest' not in ticket['subject'] and 'pentest' not in ticket['subject']):
        output_var = True
    elif ticket_fusionado == True:
        output_var = True
    elif (ticket['category'] == None or ticket['sub_category'] == None or ticket['item_category'] == None) and ticket['status'] not in (5, 9):
        output_var = True
    else:
        output_var = False

    return output_var

def comparador_fechas(fecha_obtenida, fecha_limite):
    output_var = None

    try:
        if fecha_obtenida <= fecha_limite:
            output_var = True
        elif fecha_obtenida > fecha_limite:
            output_var = False
    except:
        pass
    
    return output_var

def date_diferencia(date_a, date_b):

    output_var = None
    try:
        output_var = round((date_a - date_b) / timedelta(seconds = 1))
    except:
        pass

    return output_var

def construir_insert_and_data(dict_data, table):
    
    # Construccion de QUERY y DATA
    columns       = ", ".join(list(dict_data.keys()))
    datas         = tuple(dict_data.values())
    escape_values = str("%s, "*len(datas))[:-2]

    # Guardando listado de QUERYS y DATAS
    insert_query    = f"INSERT INTO {table} ({columns}) VALUES ({escape_values})"
    insert_data     = datas

    return {'query' : insert_query, 'data' : insert_data}

def construir_delete_and_data(input_data, table):
    # Construccion de QUERY y DATA
    delete_query    = f'DELETE FROM {table} WHERE (`id` = %s)'
    if isinstance(input_data, dict):
        delete_data = (input_data['id'], )
    elif isinstance(input_data, int) or isinstance(input_data, str):
        delete_data = (input_data, )

    return {'query' : delete_query, 'data' : delete_data}

def construir_update_and_data(dict_data, table):
    
    # Construir la QUERY
    update_columns = ""
    for key in dict_data:
        update_columns += f"{key} = %s, "
    update_columns = update_columns.rstrip(', ')
    update_query          =   f"UPDATE {table} SET {update_columns} WHERE id = {dict_data['id']}"

    # Construir la DATA
    update_data       = tuple(dict_data.values())

    return {'query' : update_query, 'data' : update_data}

def detector_insert_or_update(fs_tickets, sla_tickets, list_customers, ignore_date = False):

    def detector_for_new_or_old(fs_tickets, sla_tickets, list_customers, ignore_date = False):
        
        updates = list()
        news    = list() 
        for fs_ticket in fs_tickets:

            is_new = True
            if sla_tickets != None and sla_tickets != []:
                for sla_ticket in sla_tickets:

                    # Detectar Actualizaciones
                    if int(fs_ticket['fs_id']) == int(sla_ticket['id']):
                        is_new = False
                        if sla_ticket['updated_at'] == None:
                            updates.append(build_ticket_data(fs_ticket, list_customers))
                        elif fs_ticket['updated_at'] > sla_ticket['updated_at']:
                            updates.append(build_ticket_data(fs_ticket, list_customers))
                        elif ignore_date == True:
                            updates.append(build_ticket_data(fs_ticket, list_customers))
                        break

            if is_new == True:
                news.append(build_ticket_data(fs_ticket, list_customers))

        # Corrector de vacios
        if news == None or news == []:
            news = None
        if updates == None or updates == []:
            updates = None    

        return {'news' : news, 'updates' : updates}

    output_var = {
        'insert' : list(),
        'update' : list(),
    }

    if fs_tickets == None:
        print('ERROR - No se recibieron fs_ticket')

    elif sla_tickets == None:
        temp_var             = detector_for_new_or_old(fs_tickets, sla_tickets, list_customers, ignore_date)
        output_var['insert'] = temp_var['news']
        output_var['update'] = None

    else:
        temp_var             = detector_for_new_or_old(fs_tickets, sla_tickets, list_customers, ignore_date)
        output_var['insert'] = temp_var['news']
        output_var['update'] = temp_var['updates']

    return output_var

def build_ticket_data(single_ticket, list_customers):

    # sla_cumplimiento    = get_sla_from_csv(ticket["fs_id"], csv_data)
    customer            = set_customer(list_customers, single_ticket["department_id"])
    ticket_fusionado    = get_ticket_fusionado(single_ticket)
    ticket_problematico = get_problematico(single_ticket, ticket_fusionado)
    sla_crear           = None
    sla_resolucion      = None
    sla_cerrar          = None

    if single_ticket['sub_category'] in ('Consulta', 'Cambio', 'Incidente'):
        sla_crear       = None # Dificultades para determinar
        sla_resolucion  = comparador_fechas(single_ticket["resolved_at"], single_ticket["due_by"])
        sla_cerrar      = comparador_fechas(single_ticket["closed_at"], single_ticket["due_by"])
    
    elif single_ticket['category'] == 'Soporte' and single_ticket["sub_category"] in ('Alto', 'Medio', 'Bajo'):
        sla_crear       = None # Dificultades para determinar
        sla_resolucion  = comparador_fechas(single_ticket["resolved_at"], single_ticket["due_by"])
        sla_cerrar      = comparador_fechas(single_ticket["closed_at"], single_ticket["due_by"])

    elif single_ticket['category'] == 'Reportes' and ('Mensual', 'Semanal', 'Diario', 'On-Demand'):
        sla_crear       = None # Dificultades para determinar
        sla_resolucion  = comparador_fechas(single_ticket["resolved_at"], single_ticket["due_by"])
        sla_cerrar      = comparador_fechas(single_ticket["closed_at"], single_ticket["due_by"])

    # REQUERIMIENTOS (CONSULTA, CAMBIO, INCIDINTE) , SOPORTE, REPORTES, OTROS
    ticket_data = {
        "id"                        : single_ticket["fs_id"],
        "priority"                  : single_ticket["priority"],
        "status"                    : single_ticket["status"], 
        "subjetc"                   : single_ticket["subject"],         # Errata por aqui
        "subject"                   : single_ticket["subject"],         # Arreglo
        "sla_policy_id"             : single_ticket["sla_policy_id"], 

        "first_resp_time"           : single_ticket["first_resp_time_in_secs"],
        "resolution_time"           : single_ticket["resolution_time_in_secs"],
        "resolved_at"               : single_ticket["resolved_at"],
        "closed_at"                 : single_ticket["closed_at"],
        "fechahora_ofensa"          : single_ticket["fechahora_ofensa"],
        "due_by"                    : single_ticket["due_by"], 
        "fr_due_by"                 : single_ticket["fr_due_by"], 
        "created_at"                : single_ticket["created_at"], 
        "updated_at"                : single_ticket["updated_at"], 
        
        "category"                  : single_ticket["category"], 
        "sub_category"              : single_ticket["sub_category"], 
        "item_category"             : single_ticket["item_category"], 
        "producto"                  : single_ticket["producto"],
        "id_cdu"                    : single_ticket["id_cdu"],
        "namecdu"                   : single_ticket["namecdu"],
        "cant_eventos"              : single_ticket["cant_eventos"],
        'evaluacion_analista'       : single_ticket['evaluacin_analista'], 

        "customer"                  : customer,
        'ticket_fusionado'          : ticket_fusionado,
        'ticket_problematico'       : ticket_problematico,
        
        "sla_crear"                 : sla_crear,
        "sla_cerrar"                : sla_cerrar,
        "sla_resolucion"            : sla_resolucion,
        
    }

    return ticket_data

def datetime_now():
    output_var = datetime.now()
    output_var = datetime(
        year    = output_var.year,
        month   = output_var.month,
        day     = output_var.day,
        hour    = output_var.hour,
        minute  = output_var.minute,
        second  = output_var.second,
    )

    return output_var

# Selector de tabla, realmente no funciona
sla_table        = 'sla_soporte'

# Controlador por ticket especifico
selected_tickets = None  # Default None, Usa listas '[]'

# Controlador de paginador
offset_limit     = 0     # Default value 0
per_limit        = 1000  # Default value 1000

# Para actualizar todo
# limitar_dias = False y ignore_date = True
enable_delete    = True
limitar_dias     = True  # Default True, Limita hasta 90 días
ignore_date      = False # Default False, Ignorar Fechas al actualizar

init_datetime = datetime_now()
print(f"\n{init_datetime} - Iniciando script sla_soporte.py")

con_ticket  = db.__try_open__(db.connection_params, dictionary = True)
con_monthly = db.__try_open__(db.connection_params_2, dictionary = True)

list_customers = get_customer(con_monthly)

if enable_delete == True:
    print("Iniciando borrado de los tickets que incumplen el criterio")
    delete_offset_limit = offset_limit
    delete_per_limit    = per_limit

    delete_loop_breaker = False
    while delete_loop_breaker == False:

        # Obtencion de tickets
        sla_tickets    = get_tickets_sla_selected(con_monthly, sla_table, per_limit=delete_per_limit, offset_limit=delete_offset_limit, date_limiter=limitar_dias)
        
        if sla_tickets != None and sla_tickets != []:
            
            # Generando listado de sla_tickets
            sla_tickets_id = list()
            for ticket in sla_tickets:
                sla_tickets_id.append(ticket['id'])

            # Comprobar si el ticket aun cumple las condiciones del SLA
            fs_tickets = get_tickets_z_fs_tickets(con_ticket, date_limiter = limitar_dias,selected_tickets = sla_tickets_id)
            if fs_tickets != None and fs_tickets != []:
                for fs_ticket in fs_tickets:
                    sla_tickets_id.remove(fs_ticket['fs_id'])
                    
            # Comprobacion Pre-borrado
            if sla_tickets_id != None and sla_tickets_id != []:
                delete_data    = list()
                delete_queries = list()
                for id in sla_tickets_id:
                    temp_var = construir_delete_and_data(id, sla_table)
                    delete_queries.append(temp_var['query'])
                    delete_data.append(temp_var['data'])
                db.__insert_multi_data__(con_monthly, delete_queries, delete_data)
                del temp_var, delete_queries, delete_data
        
        # Salir del bucle
        elif sla_tickets == None or sla_tickets == []:
            delete_loop_breaker = True 
        
        delete_offset_limit += delete_per_limit

    # del delete_offset_limit, delete_per_limit, delete_loop_breaker, sla_tickets, sla_tickets_id, fs_tickets

if selected_tickets != None and selected_tickets != []: # Ejecucion "Forzada"
    print("Iniciando actualizacion de los tickets por ID")
    fs_tickets  = get_tickets_z_fs_tickets(con_ticket, selected_tickets = selected_tickets)
    sla_tickets = get_tickets_sla_selected(con_monthly, sla_table, fs_tickets)
    fs_tickets  = detector_insert_or_update(fs_tickets, sla_tickets, list_customers, True)
    
    list_queries = list()
    list_datas   = list()

    # IF para los INSERT en DB
    if fs_tickets['insert'] != None:
        print(f"Tickets para insertar: {len(fs_tickets['insert'])}")
        for fs_ticket in fs_tickets['insert']:
            temp_var = construir_insert_and_data(fs_ticket, sla_table)
            list_queries.append(temp_var['query'])
            list_datas.append(temp_var['data'])

    # IF para los UPDATE en DB
    if fs_tickets['update'] != None:
        print(f"Tickets para Actualizar: {len(fs_tickets['update'])}")
        for fs_ticket in fs_tickets['update']:
            temp_var = construir_update_and_data(fs_ticket, sla_table)
            list_queries.append(temp_var['query'])
            list_datas.append(temp_var['data'])

    if (list_queries != [] or list_queries != None) and (list_datas != [] or list_datas != None):
        db.__insert_multi_data__(con_monthly, list_queries, list_datas)
    del list_queries, list_datas

else: # Ejecucion "Normal"

    print("Iniciando actualizacion de los tickets por fecha")
    if check_first_load(con_monthly, sla_table) == False:
        print("No se encontraron registros previos, limitador de días desactivado")
        limitar_dias = False

    loop_breaker = False
    while loop_breaker == False:

        fs_tickets = get_tickets_z_fs_tickets(con_ticket, per_limit, offset_limit, limitar_dias)

        if fs_tickets != None and fs_tickets != []:
            list_queries = list()
            list_datas   = list()
            
            sla_tickets = get_tickets_sla_selected(con_monthly, sla_table, fs_tickets)
            fs_tickets  = detector_insert_or_update(fs_tickets, sla_tickets, list_customers, ignore_date)

            # IF para los INSERT en DB
            if fs_tickets['insert'] != None:
                print(f"Tickets para insertar: {len(fs_tickets['insert'])}")
                for fs_ticket in fs_tickets['insert']:
                    temp_var = construir_insert_and_data(fs_ticket, sla_table)
                    list_queries.append(temp_var['query'])
                    list_datas.append(temp_var['data'])

            # IF para los UPDATE en DB
            if fs_tickets['update'] != None:
                print(f"Tickets para Actualizar: {len(fs_tickets['update'])}")
                for fs_ticket in fs_tickets['update']:
                    temp_var = construir_update_and_data(fs_ticket, sla_table)
                    list_queries.append(temp_var['query'])
                    list_datas.append(temp_var['data'])

            if (list_queries != [] or list_queries != None) and (list_datas != [] or list_datas != None):
                db.__insert_multi_data__(con_monthly, list_queries, list_datas)
            del list_queries, list_datas
            offset_limit += per_limit
        
        elif fs_tickets == None or fs_tickets == []:
            loop_breaker = True

con_ticket  = db.__try_close__(con_ticket)
con_monthly = db.__try_close__(con_monthly)

stop_datetime = datetime_now()
print(f"{stop_datetime} - Finalizado script sla_soporte.py")