#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# INSERTE DESCRIPCION

#####################################
# IMPORTS NECESARIOS                #
#####################################

import connection_db as db

from datetime import datetime, timedelta
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

def get_last_id_monthly_report(con_monthly):
    output_var = {}

    sla_table = [
        'sla_monitoreo',
    ]

    # Base Query
    for table in sla_table:
        base_query  = f"SELECT id FROM {table} ORDER BY id DESC LIMIT 1"
        last_id     = db.__get_data__(con_monthly, base_query, multi = False)

        if last_id == None:
            output_var.update({table : 0})
        else:
            output_var.update({table : last_id['id']})

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

def get_tickets(con_ticket, last_id, limit = None):

    # Base Query
    base_query      = "SELECT * FROM z_fs_ticket"
    # Filtros fijos
    default_where = [
        "WHERE (department_id IS NULL OR department_id IN (23000035894,23000038553,23000203546,23000038482,23000038541,23000209665,23000038546,23000157750,23000038547,23000038548,23000038550,23000070194))", # Clientes incluidos Adaptive Security,AFP Capital,AFP Habitat,Aguas Andinas,BDO Chile,BNP Paribas Cardif,COPEC,Consorcio,Emaresa S.A.,Evertec Inc.,SBPay,Seguros Continental
        "AND (plataforma IS NULL OR plataforma NOT IN ('Beyond Trust'))",                   # Excluido de Monitoreo
        "AND (category IS NULL OR category IN ('Reportes', 'Monitoreo'))",                  # SLA Monitoeo
        "AND (sub_category IS NULL OR sub_category IN ('SIEM', 'Informe de Servicio'))",    # SLA Monitoreo
        "AND (deleted != 1)",
        "AND (ticket_fusionado IS NULL OR ticket_fusionado = 0)",                           # Corrector
        "AND (group_id IS NULL OR group_id NOT IN (23000086944))",                          # Corrector
    ]
    default_where = " ".join(default_where)
    # Filtros dinamicos
    dinamic_where   = f"AND (fs_id > {last_id})"
    # Orden
    order_by        = "ORDER BY fs_id ASC"
   
    # Armado de QUERY
    tickets         = f"{base_query} {default_where} {dinamic_where} {order_by}"
 
    # Limitar registros
    if limit != None:
        tickets += f" LIMIT {str(limit)}"

    tickets    = db.__get_data__(con_ticket, tickets, multi = True)

    return tickets

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

def get_ticket_pendiente_update(con_monthly, con_ticket, tabla_destino):
    
    def get_pendientes(con_monthly, tabla_destino):
        output_var = None
        query      = f"SELECT id FROM {tabla_destino} WHERE status NOT IN (5, 9) ORDER BY id ASC" # Solo se puede usar con monthly_report
        results    = db.__get_data__(con_monthly, query, multi = True)

        if results != None:

            output_var = []
            for result in results:
                output_var.append(str(result['id']))
            output_var = tuple(output_var)

        return output_var
    
    def get_tickets(con_ticket, list_id):
        list_id    = ', '.join(list_id)
        query      = f"SELECT * FROM z_fs_ticket WHERE fs_id in ({list_id}) ORDER BY fs_id ASC"
        output_var = db.__get_data__(con_ticket, query, multi = True)

        return output_var

    output_var  = None
    list_id     = get_pendientes(con_monthly, tabla_destino) # USAR AQUI con_monthly
    if list_id != None:
        output_var = get_tickets(con_ticket, list_id)
    
    return output_var # Son tickets
    
def construir_ticket_data(single_ticket, list_customers):

    customer            = set_customer(list_customers, single_ticket['department_id'])      # CUSTOMER = department_id
    month               = get_month(single_ticket['created_at'])                            # Extraer mes de la fecha de creacion
    ticket_fusionado    = get_ticket_fusionado(single_ticket)                               # determina si el ticket es fusionado
    ticket_problematico = get_problematico(single_ticket, ticket_fusionado)                 # Marca los ticket raros y fusionados
    
    timer_crear         = None
    timer_cerrar        = None
    timer_notificar     = None

    if single_ticket['category'] == 'Reportes' and ('Mensual', 'Semanal', 'Diario', 'On-Demand'):
        timer_crear     = None 
        timer_cerrar    = date_diferencia(single_ticket['closed_at'], single_ticket['created_at'])  # Tiempo de diferencia de crear para un ticket
        timer_notificar = date_diferencia(single_ticket['closed_at'], single_ticket['created_at'])  # Tiempo de diferencia de cerrar para un ticket
    else:
        timer_crear     = date_diferencia(single_ticket['created_at'], single_ticket['fechahora_ofensa'])
        timer_cerrar    = date_diferencia(single_ticket['closed_at'], single_ticket['fechahora_ofensa'])  # Tiempo de diferencia de crear para un ticket
        timer_notificar = date_diferencia(single_ticket['closed_at'], single_ticket['fechahora_ofensa'])  # Tiempo de diferencia de cerrar para un ticket
    
    sla_crear           = get_sla_monitoreo("abrir ticket", timer_crear)
    sla_cerrar          = get_sla_monitoreo("cerrar ticket", timer_cerrar)
    sla_notificar       = get_sla_monitoreo("notificar ticket", timer_notificar)

    ticket_data = {
        'id'                     : single_ticket['fs_id'],
        'priority'               : single_ticket['priority'],
        'status'                 : single_ticket['status'],
        'subject'                : single_ticket['subject'],
        'sla_policies_id'        : single_ticket['sla_policies_id'],

        "first_resp_time"        : single_ticket["first_resp_time_in_secs"],
        "resolution_time"        : single_ticket["resolution_time_in_secs"],
        "resolved_at"            : single_ticket["resolved_at"],
        "closed_at"              : single_ticket["closed_at"],
        'fechahora_ofensa'       : single_ticket['fechahora_ofensa'],
        'due_by'                 : single_ticket['due_by'], 
        'fr_due_by'              : single_ticket['fr_due_by'], 
        'created_at'             : single_ticket['created_at'],
        'updated_at'             : single_ticket['updated_at'],
        
        "id_cdu"                 : single_ticket["id_cdu"],
        'caso_de_uso'            : single_ticket['namecdu'],
        "cant_eventos"           : single_ticket["cant_eventos"],
        'category'               : single_ticket['category'], 
        'sub_category'           : single_ticket['sub_category'], 
        'item_category'          : single_ticket['item_category'], 
        'producto'               : single_ticket['producto'],
        'evaluacion_analista'    : single_ticket['evaluacin_analista'],   
        'customer'               : customer,
        'month'                  : month,     
        'timer_notificar'        : timer_notificar,   
        'timer_cerrar'           : timer_cerrar,
        'sla_crear'              : sla_crear,       
        'sla_cerrar'             : sla_cerrar,      
        'sla_notificar'          : sla_notificar,
        'ticket_fusionado'       : ticket_fusionado,
        'ticket_problematico'    : ticket_problematico,
    }

    return ticket_data

def update_closed_tickets(con_monthly, con_ticket, db_table, list_customers):
    contador        = 0
    forced_exit     = False
    limite_inferior = 0
    limite_superior = 1000

    while forced_exit == False:
        query   = f"SELECT id, updated_at FROM {db_table} WHERE status IN (5, 9) LIMIT {limite_inferior}, {limite_superior}"
        results = db.__get_data__(con_monthly, query, multi = True)

        if results == None or results == []:
            forced_exit = True
        
        else:

            list_queries = None
            list_datas   = None     
            for result in results:
                
                internal_query  = f"SELECT * FROM z_fs_ticket WHERE fs_id = {result['id']} AND updated_at > '{result['updated_at']}'"
                internal_result = db.__get_data__(con_ticket, internal_query, multi = False)
                
                if internal_result != None:
                    print(result['id'])
                    if list_queries == None or list_datas == None:
                        list_queries = []
                        list_datas   = []
                    
                    internal_result = construir_ticket_data(internal_result, list_customers)
                    internal_result = construir_update_and_data(internal_result, db_table)
                    contador       += 1

                    list_queries.append(internal_result['query'])
                    list_datas.append(internal_result['data'])

            if list_queries != None and list_datas != None:
                db.__insert_multi_data__(con_monthly, list_queries, list_datas)

            limite_inferior  = limite_superior
            limite_superior += 1000

    print(f"Tickets Actualizados: {contador}")

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

init_datetime = datetime_now()
print(f"\n{init_datetime} - Iniciando script sla_monitoreo.py")

con_ticket  = db.__try_open__(db.connection_params, dictionary = True)
con_monthly = db.__try_open__(db.connection_params_2, dictionary = True)

print("Comprobando IDs...")
ticket_last_id         = get_last_id_ticket(con_ticket)
monthly_report_last_id = get_last_id_monthly_report(con_monthly)
list_customers         = get_customer(con_monthly)

for table in monthly_report_last_id:
    
    # Actualizar los ticket cerrados modificados a posterior
    print("Actualizando tickets cerrados...")
    update_closed_tickets(con_monthly, con_ticket, table, list_customers)

    # Actualizar tickets viejos
    print('Buscando tickets pendientes...')
    tickets = get_ticket_pendiente_update(con_monthly, con_ticket, table)
    update_query = []
    update_data  = []
    
    if tickets != None:    
        
        print(f"ACTUALIZANDO {len(tickets)} Tickets")
        for ticket in tickets:
            
            ticket_data = construir_ticket_data(ticket, list_customers)
            temp_data   = construir_update_and_data(ticket_data, table)
        
            # Guardando listado de QUERYS y DATAS
            update_query.append(temp_data['query'])
            update_data.append(temp_data['data'])

        # actualizando datos
        db.__insert_multi_data__(con_monthly, update_query, update_data)
        del ticket_data
        del temp_data
    
    del tickets
    del update_query
    del update_data

    # Proceso para añadir datos nuevos
    print("Buscando tickets nuevos...")
    if ticket_last_id != None and ticket_last_id != monthly_report_last_id[table]:
        
        insert_query = []
        insert_data  = []
        tickets      = get_tickets(con_ticket, monthly_report_last_id[table])
        
        if tickets != None:
            print(f"INSERTANDO {len(tickets)} Tickets")
            for ticket in tickets: # Recorrido de tickets

                ticket_data = construir_ticket_data(ticket, list_customers)

                # Guardando listado de QUERYS y DATAS
                temp_data = construir_insert_and_data(ticket_data, table)
                insert_query.append(temp_data['query'])
                insert_data.append(temp_data['data'])

            # Insertando datos
            db.__insert_multi_data__(con_monthly, insert_query, insert_data)


con_ticket  = db.__try_close__(con_ticket)
con_monthly = db.__try_close__(con_monthly)

stop_datetime = datetime_now()
print(f"{stop_datetime} - Finalizado script sla_monitoreo.py\n")