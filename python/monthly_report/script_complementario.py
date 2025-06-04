#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

from datetime import datetime, timedelta
from time     import sleep

import connection_db     as db
import connection_xforce as xforce

#####################################
# SCRIPT                            #
#####################################


def set_date_range_per_month(automatized : bool = True, init_date = None):
    
    # Usando la fecha actual de base
    if init_date == None or automatized == True:
        init_date = datetime.now()

    # Calculando el stop_time
    init_date = datetime(
        year    = init_date.year,
        month   = init_date.month,
        day     = 1,
    )

    # Calculando el start_time
    limit_date = init_date - timedelta(days = 1)
    limit_date = datetime(
        year    = limit_date.year,
        month   = limit_date.month,
        day     = 1,
    )

    return {'start_time' : limit_date, 'stop_time' : init_date,}

def no_first_load(autoofense_conn, list_ip):
    
    escape_values  = str("%s, " * len(list_ip)).rstrip(", ")
    query_select   = 'SELECT MAX(normal_data.id) AS last_id, normal_data.ip_number, last_data.xf_value, last_data.xf_risk, last_data.update_time FROM ip_tracking AS normal_data LEFT JOIN ( SELECT id, ip_number, xf_value, xf_risk, update_time FROM ip_tracking) AS last_data ON normal_data.id = last_data.id' 
    query_where    = 'WHERE normal_data.ip_fuente NOT IN ("{fileHash}", "{fileSha256}", "{mwHash}", "{request}") AND (normal_data.ip_number NOT IN ("", "None") AND normal_data.xf_value NOT IN ("", "None") AND normal_data.xf_risk NOT IN ("", "None")) '
    query_where   += f'AND normal_data.ip_number NOT IN ({escape_values})'
    query_order    = 'GROUP BY normal_data.ip_number ORDER BY ip_number ASC'
    

    query      = f"{query_select} {query_where} {query_order}"
    output_var = db.__get_data__(autoofense_conn, query, list_ip, multi = True)
    
    return output_var

def first_load(autoofense_conn):
    
    query   = 'SELECT MAX(normal_data.id) AS last_id, normal_data.ip_number, last_data.xf_value, last_data.xf_risk, last_data.update_time FROM ip_tracking AS normal_data LEFT JOIN ( SELECT id, ip_number, xf_value, xf_risk, update_time FROM ip_tracking) AS last_data ON normal_data.id = last_data.id WHERE normal_data.ip_fuente NOT IN ("{fileHash}", "{fileSha256}", "{mwHash}", "{request}") AND (normal_data.ip_number NOT IN ("", "None") AND normal_data.xf_value NOT IN ("", "None") AND normal_data.xf_risk NOT IN ("", "None")) GROUP BY normal_data.ip_number ORDER BY ip_number ASC'
    
    output_var = db.__get_data__(autoofense_conn, query, multi = True)
    
    return output_var

def check_before_first_load(monthly_conn):
    
    query      = "SELECT ip FROM ip_risk LIMIT 1"
    output_var = db.__get_data__(monthly_conn, query, multi = False)

    return output_var

def insert_new_ips(monthly_conn, table, insert_list : list):
    
    print(f"Insertando {len(insert_list)} registros")

    list_query = []
    list_data  = []
    for data in insert_list:
        
        insert_data   = tuple(dict(data).values())
        escape_values = str("%s, " * len(insert_data)).rstrip(", ")
        columns       = ",".join(tuple(dict(data).keys()))
        insert_query  = f"INSERT INTO {table} ({columns}) VALUES ({escape_values})"
        
        list_query.append(insert_query)
        list_data.append(insert_data)
    
    db.__insert_multi_data__(monthly_conn, list_query, list_data)

def evertec_top_ip(monthly_conn, date_range, list_ip = None):

    if list_ip == None:
        output_var = []
    elif list_ip != None:
        output_var = list_ip

    # start_time, stop_time
    top_classification  = "SELECT classification, SUM(event_count) AS total_event FROM zdumps_evertec_cdu_ips_resume WHERE (start_date >= %s AND start_date < %s) GROUP BY classification ORDER BY total_event DESC LIMIT 3"
    temp_values         = (date_range['start_time'], date_range['stop_time'])
    top_classification  = db.__get_data__(monthly_conn, top_classification, temp_values, multi = True)
    
    if top_classification != None:
        temp_data = []
        for classification in top_classification:
            temp_data.append(classification['classification'])
        top_classification = temp_data
    
        for classification in top_classification:
            
            # cdu, start_time, stop_time
            top_destination_ip = "SELECT destination_ip, sum(event_count) AS total_event FROM zdumps_evertec_cdu_ips_especific WHERE classification = %s AND (start_date >= %s AND start_date < %s) group by destination_ip ORDER BY total_event DESC LIMIT 10"
            temp_values        = (classification, date_range['start_time'], date_range['stop_time'])
            top_destination_ip = db.__get_data__(monthly_conn, top_destination_ip, temp_values, multi = True)

            if top_destination_ip != None:
                
                temp_data = ""
                for ip in top_destination_ip:
                    temp_data += (f"'{ip['destination_ip']}', ")
                top_destination_ip = temp_data.rstrip(', ')

                # cdu, list_ips, start_time, stop_time
                top_origin_ip = f"SELECT t1.destination_ip, t1.source_ip, SUM(t1.event_count) AS total_event, CASE WHEN t2.ip IS NOT NULL THEN 'Red Excepcionada' ELSE '' END AS ip_habilitada FROM  zdumps_evertec_cdu_ips_especific AS t1 LEFT JOIN (SELECT ip FROM portal.company_ipblock WHERE validation = 0 ) AS t2 ON t1.source_ip = t2.ip WHERE t1.classification = '{classification}' AND t1.destination_ip IN ({top_destination_ip}) AND (start_date >= '{date_range['start_time']}' AND start_date < '{date_range['stop_time']}') GROUP BY t1.destination_ip, t1.source_ip ORDER BY FIELD(t1.destination_ip, {top_destination_ip}), t1.source_ip ASC"
                top_origin_ip = db.__get_data__(monthly_conn, top_origin_ip, multi = True)

                if top_origin_ip != None:
                    for origin_ip in top_origin_ip:
                        if 'Excepcionada' in origin_ip['ip_habilitada'] and origin_ip['source_ip'] not in output_var:
                            output_var.append(origin_ip['source_ip'])

    return output_var

def adaptive_security_top_ip(monthly_conn, date_range, list_ip = None):
    
    if list_ip == None:
        output_var = []
    elif list_ip != None:
        output_var = list_ip

    # start_time, stop_time
    top_origin_ip  = "SELECT zdump.source_ip, SUM(zdump.event_count) AS eventos, risk.locate_new, CONCAT(ROUND(SUM(zdump.event_count) * 100 / SUM(SUM(zdump.event_count)) OVER (), 2), \"%\") AS porcentaje FROM zdumps_as_cloudflare AS zdump LEFT JOIN ip_risk AS risk ON zdump.source_ip = risk.ip WHERE (start_date >= %s AND start_date < %s) AND zdump.firewall_matches_actions LIKE '%\"block\"%' GROUP BY zdump.source_ip ORDER BY eventos DESC, zdump.source_ip ASC LIMIT 10"
    temp_values    = (date_range['start_time'], date_range['stop_time'])
    top_origin_ip  = db.__get_data__(monthly_conn, top_origin_ip, temp_values, multi = True)
    
    if top_origin_ip != None:
        for origin_ip in top_origin_ip:
            if origin_ip['source_ip'] not in output_var:
                output_var.append(origin_ip['source_ip'])
    
    return output_var

def ips_without_country(monthly_conn, limit = 500):
    
    output_var = []

    query = f"SELECT * FROM ip_risk WHERE locate_new IS NULL ORDER BY updated_at DESC LIMIT {limit}"
    query = db.__get_data__(monthly_conn, query, multi = True)

    if query != None:
        for data in query:
            if data['ip'] not in output_var:
                output_var.append(data['ip'])

    return output_var

def monthly_report_get_ips(monthly_conn):
    
    query      = "SELECT ip FROM ip_risk"
    output_var = db.__get_data__(monthly_conn, query, multi = True)
    
    list_ip = []
    for ip_data in output_var:
        list_ip.append(ip_data['ip'])
    output_var = list_ip

    return output_var

def monthly_report_update_ips(monthly_conn, ip, update_dict):
    
    update_columns  = str(" = %s, ".join(list(dict(update_dict).keys()))) + " = %s"
    values          = list(dict(update_dict).values())
    values.append(ip)
    query           = f"UPDATE ip_risk SET {update_columns} WHERE (ip = %s)"

    db.__insert_data__(monthly_conn, query, values)
    
def discard_ip_from_list(monthly_conn, list_ip):

    output_var = []
    for data_ip in list_ip:
        query   = "SELECT * FROM ip_risk WHERE ip = %s"
        query   = db.__get_data__(monthly_conn, query, (data_ip, ))

        if query == None:
            output_var.append(data_ip)

    return output_var

def get_old_level_xforce(xforce_score):
    
    xf_risk_old = {
        0 : "LOW",
        1 : "LOW",
        2 : "LOW",
        3 : "MEDIUM",
        4 : "MEDIUM",
        5 : "MEDIUM",
        6 : "MEDIUM",
    }
    
    try:
        if xforce_score >= 7:
            xf_risk = "HIGH"
        else:
            xf_risk = xf_risk_old[xforce_score]
    except:
        xf_risk = "Undefined"
        
    return xf_risk

def print_by_percent(max_value, index_value, message, old_percent = None):
    
    percent_value = round((index_value * 100 / max_value))
    
    if old_percent == None:
        print(f"{message} {percent_value}%")
    else:
        if (percent_value != old_percent) or (max_value == index_value):
            print(f"{message} {percent_value}%")

    return percent_value

# Ejecucion del script
automatized      = True                 # False para aplicar fechas de abajo
stop_time        = datetime(2024, 2, 1) # Para el date_range de las querys, 
start_time       = datetime(2024, 1, 1) # Para el date_range de las querys, puede ser un None

update_before    = datetime.now() - timedelta(days = 30) # Las IP mas antiguas a 30 dias seran actualizadas

disable_segment_1   = False  # Desactiva Nuevas IPs
disable_segment_2   = False  # Desactiva Update IPs
disable_segment_3   = False  # Desactiva Consultas en xforce
disable_segment_aux = True   # Desctiva el limitador y actualiza todo, Mantener en True

# Abrir conexiones necesarias
print("\nInicio del Script") # Salto de linea e inicio de LOG
monthly_conn     = db.__try_open__(db.connection_params_2, dictionary = True)
autoofense_conn  = db.__try_open__(db.connection_params_3, dictionary = True)

# Comprobacion si existen datos guardados
check_table      = check_before_first_load(monthly_conn)

### BUSCAR REGISTROS NUEVOS ###
if disable_segment_1 == False:
    if check_table == None:
        
        print("No se encontro informacion previa, primera ejecución")
        list_ips    = first_load(autoofense_conn)
        insert_list = []
        for ip_data in list_ips:

            if ip_data['update_time'] == None:
                ip_data['update_time'] = datetime.now()

            insert_dict = {
                'ip'        : ip_data['ip_number'], 
                'score_new' : ip_data['xf_value'], 
                'risk_new'  : ip_data['xf_risk'], 
                'updated_at': ip_data['update_time'],
            }
            insert_list.append(insert_dict)

        del list_ips
        insert_new_ips(monthly_conn, 'ip_risk', insert_list)

    else:

        print("Buscando informacion nueva")
        list_ips = monthly_report_get_ips(monthly_conn)
        if list_ips != None:
            list_ips = no_first_load(autoofense_conn, list_ips)
        
        if list_ips != None:
            insert_list = []
            for ip_data in list_ips:

                if ip_data['update_time'] == None:
                    ip_data['update_time'] = datetime.now()

                insert_dict = {
                    'ip'        : ip_data['ip_number'], 
                    'score_new' : ip_data['xf_value'], 
                    'risk_new'  : ip_data['xf_risk'], 
                    'updated_at': ip_data['update_time'],
                }
                insert_list.append(insert_dict)
            
            del list_ips
            insert_new_ips(monthly_conn, 'ip_risk', insert_list)

### ACTUALIZAR REGISTROS CON AUTOOFENSA ###
if disable_segment_2 == False:
    if check_table == None:
        pass
    elif check_table != None:
        
        old_datas = f"SELECT ip, locate_new, score_new, risk_new, updated_at FROM ip_risk WHERE updated_at <= '{update_before}'"
        old_datas = db.__get_data__(monthly_conn, old_datas, multi = True)

        registro_total = len(old_datas)
        contador       = 0
        old_percent    = None
        for old_data in old_datas:
            contador += 1

            old_percent = print_by_percent(registro_total, contador, "Actualizando:", old_percent)

            values      = (old_data['ip'], old_data['updated_at'])
            new_data    = "SELECT ip_number, xf_value, xf_risk, update_time FROM ip_tracking WHERE ip_number = %s AND update_time > %s ORDER BY update_time DESC LIMIT 1"
            new_data    = db.__get_data__(autoofense_conn, new_data, values, multi = False)
            
            if new_data != None: # Aumentar el criterio para saber que actualizar

                if new_data['update_time'] > old_data['updated_at']:
                    
                    if new_data['xf_risk'] == None or new_data['xf_risk'] == "":
                        print(f"{old_data['ip']} | riesgo vacio")

                    # Pais de la IP no se esta actualizando
                    update_dict = {
                        'score_old' : old_data['score_new'], 
                        'risk_old'  : old_data['risk_new'], 
                        'score_new' : new_data['xf_value'], 
                        'risk_new'  : new_data['xf_risk'], 
                        'updated_at': new_data['update_time'],
                    }
                
                    monthly_report_update_ips(monthly_conn, old_data['ip'], update_dict)

### INGRESANDO REGISTROS DESCUBIERTOS ###
if disable_segment_3 == False:
    # Comprobando suficiente informacion necesaria
    print("Comprobando IPs no registradas")
    if automatized == True:
        date_range = set_date_range_per_month()
    elif automatized == False and (start_time == None or stop_time == None): # IF anticagadas
        date_range = set_date_range_per_month()
    elif automatized == False and (start_time == None and stop_time != None):
        date_range = set_date_range_per_month(init_date = stop_time)
    elif automatized == False and (start_time != None and stop_time != None):
        date_range = {'start_time' : start_time, 'stop_time' : stop_time,}

    list_ip = None # Limpieza inicial
    list_ip = evertec_top_ip(monthly_conn, date_range)
    list_ip = adaptive_security_top_ip(monthly_conn, date_range)
    list_ip = discard_ip_from_list(monthly_conn, list_ip) # Eliminacion de IPs ya registradas
    print(f"IPs sin registrar detectadas: {str(list_ip)}")

    insert_list = []
    if list_ip != None and list_ip != []:
        for ip_data in list_ip:
            json_data = xforce.request_ibm_xforce("get", f"/api/ipr/{ip_data}")
            
            try:
                ip = json_data['ip']
            except:
                ip = ip_data

            try:
                country = json_data['geo']['country']
            except:
                country = None

            try:
                score   = json_data['score']
            except:
                score   = None

            try:
                risk    = get_old_level_xforce(score)
            except:
                risk    = None

            insert_dict = {
                'ip'            : ip,
                'locate_new'    : country, 
                'score_new'     : score, 
                'risk_new'      : risk,
            }

            insert_list.append(insert_dict)

        insert_new_ips(monthly_conn, 'ip_risk', insert_list)

### CON CONSULTAS A LA API PARA OBTENER PAIS ###
if disable_segment_aux == False:

    list_ip = None # Limpieza inicial
    list_ip = ips_without_country(monthly_conn, limit = 10000)

    insert_list = []
    if list_ip != None and list_ip != []:
        
        internal_count = 0
        for ip_data in list_ip:
            internal_count += 1
            json_data = xforce.request_ibm_xforce("get", f"/api/ipr/{ip_data}")
            
            try:
                ip = json_data['ip']
            except:
                ip = ip_data

            try:
                country = json_data['geo']['country']
            except:
                country = None

            try:
                score   = json_data['score']
            except:
                score   = None

            try:
                risk    = get_old_level_xforce(score)
            except:
                risk    = None

            update_dict = {
                'locate_new'    : country, 
                'score_new'     : score, 
                'risk_new'      : risk,
                'updated_at'    : datetime.now()
            }
            print(f"{internal_count}/{len(list_ip)}   {ip}")
            monthly_report_update_ips(monthly_conn, ip, update_dict)
            sleep(0.2)
            


# Cerrar conexiones necesarias
monthly_conn    = db.__try_close__(monthly_conn)
autoofense_conn = db.__try_close__(autoofense_conn)

print("Fin del Script")