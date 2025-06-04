from datetime import datetime
from datetime import timedelta

import connection_db as db
import connection_qradar as qradar

# Variables de control
ejecucion       =   "crontab"   # opciones posibles manual/crontab
min_offense     =   0
max_offense     =   50000

# Variables para request
headers         = {'Authorization': 'Basic cG9ydGFsZXM6QWRhcHRpdmUuMTIz'}
url_base        = "https://172.16.17.10/api"

# Variables de insercion
min_offense     =   0
max_offense     =   0
insertDB        =   False
insertadas      =   0
no_insertadas   =   0

# Variables de ofensa
event_count     =   ""
rul_id          =   ""
type_log_id     =   ""
type_log        =   ""                    
log_id          =   ""
log_name        =   ""
offense_id      =   ""
start_time      =   ""
stop_time       =   ""
domain_id       =   ""
continuar       =   ""

# VERIFICA LA EXISTENCIA DEL ID EN DB, NO LOG_SOURCE Y RULE_ID
def check_offense(offense_id):
    # Conexion a la base de datos auto_offense
    sql     = f"SELECT * FROM qr_offenses WHERE offense_id = '{offense_id}'"
    result  = db.get_multi_data(sql)
    
    if not result:
        return False
    else:
        return True
    
# (TEMPORAL) CONSULTA LAS OFENSAS QUE PERSISTEN ABIERTAS EN DB    
def db_open_offense():
    #Conexion a la base de datos auto_offense
    sql     = "SELECT offense_id FROM qradar_portal.qr_offenses WHERE offense_state != 'CLOSED' AND start_time >= DATE_SUB(NOW(), INTERVAL 14 DAY) GROUP BY offense_id"
    result  = db.get_multi_data(sql)
    
    return result

# (TEMPORAL) CONSULTA LAS OFENSAS A PARTIR DE UN LISTADO DE IDs
def qrdar_open_offense(offenses_ids):
    response_ofensas = qradar.get_qradar("offenses_by_ids", offenses_ids)
    return response_ofensas

# (TEMPORAL) ELIMINA OFENSAS ABIERTAS
def db_delete_open_offense(open_ids):
    # Conexion a la base de datos auto_offense
    query = f"DELETE FROM qradar_portal.qr_offenses WHERE offense_id IN ({open_ids})"
    db.single_insert(query)
    
# (TEMPORAL) PROCESO PARA ACTUALIZAR LAS OFENSAS ABIERTAS
def update_open_offenses():
    print("OBTENIENDO OFENSAS ABIERTAS DE LOS ULTIMOS 15 DIAS")
    open_offenses = db_open_offense() # Obtencion de las ofensas abiertas

    if open_offenses != None and open_offenses != []:
        db_index     = 0
        db_max_index = len(open_offenses)
        open_ids     = []

        # Construyendo un listado de ids
        while db_index < db_max_index:
            open_ids.append(str(open_offenses[db_index][0]))
            db_index += 1
        open_ids = ",".join(open_ids)

        print("BORRANDO OFENSAS ABIERTAS DE LOS ULTIMOS 15 dias")
        db_delete_open_offense(open_ids)

# ===========================
# SCRIPT DE VERDAD
# ===========================

init_date = datetime.now()  # Fecha del inicio de ejecucion del script
update_open_offenses()      # Borra ofensas para su posterior reingreso a db

# Se usan distintas URL, manual limitado por id y crontab limitado por fecha
payload = {}
if ejecucion == "manual":
    url_ofensas = f"siem/offenses?fields=id%2C%20domain_id%2C%20start_time%2C%20close_time%2C%20status%2C%20event_count%2C%20log_sources%2C%20rules&filter=id%3E%3D%20{min_offense}%20AND%20id%3C%3D%20{max_offense}&sort=-id"
elif ejecucion == "crontab":
    limit_date  = datetime.now() - timedelta(days = 15) # Retrocede 15 dias a partir de la actualidad
    limit_date  = int(limit_date.timestamp() * 1000)    # Conversion de la fecha a un timestamp en milisegundos
    url_ofensas = f"siem/offenses?fields=id%2Cdomain_id%2Cstart_time%2Cclose_time%2Cstatus%2Cevent_count%2Clog_sources%2Crules&filter=start_time%20%3E%3D%20{limit_date}%20OR%20close_time%20is%20null&sort=-id"
    
response_ofensas_json = qradar.get_qradar("custom_get", url_ofensas, True)
print("Demora en recibir ofensas: ", (datetime.now() - init_date) / timedelta(seconds=1))

insertadas  = 0
index_reg   = 0
max_reg     = len(response_ofensas_json)

for ofensas in response_ofensas_json:
    index_reg += 1

    if ejecucion == "manual":
        print(f"{index_reg} / {max_reg}") # Guia visual de cuantas ofensas faltan por recorrer
        print(ofensas)                    # ofensa dentro del response_json


    ###ASIGNACION DE OEVENT_COUNT, ODATE, offense_id, Start_time
    try: 
        offense_id = ofensas['id']
    except: 
        print("Ofensa sin número")
            
    # Comprobacion, si la ya esta registrada en DB
    if check_offense(offense_id) == False:
        
        # Conversion del start_time en formato timestap, por uno mas sencillo de leer
        try: 
            start_time = datetime.fromtimestamp(ofensas['start_time'] / 1e3).strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        
        # Conversion del close_time en formato timestap, por uno mas sencillo de leer
        try:
            stop_time = datetime.fromtimestamp(ofensas['close_time'] / 1e3).strftime("%Y-%m-%d %H:%M:%S")
        except:
            # En caso de no poder dar formato, se usa una fecha default
            stop_time = "0000-00-00 00:00:00"
        
        try: 
            event_count = ofensas['event_count']
        except: 
            pass
        
        # DOMAIN ID es utilizado a posterior para obtener el "cliente" (DOMAIN NAME)
        try: 
            domain_id = ofensas['domain_id']
        except: 
            pass
        
        # Se encontraron 3 estados posibles CLOSED/OPEN/HIDDEN
        try: 
            offense_state = ofensas['status']
        except: 
            pass
        
        if ejecucion == "manual":
            print(f"start_time: {start_time}; stop_time: {stop_time}; event_count: {event_count}; domain_id: {domain_id}; offense_state: {offense_state}")

        #BUSCAR EL CLIENTE(domain_name), CORRESPONDIENTE AL DOMAIN ID
        response_customer_json = qradar.get_qradar("domain", domain_id)
        
        #ESTABLECER domain_name
        try: 
            domain_name = response_customer_json['name']            
        except: 
            print("domain_name no definido")
        
        # Recorrido de las reglas contenidas en la informacion de la ofensa
        list_query = []
        list_data  = []
        if 'rules' in list(ofensas.keys()): 
            for rul in ofensas['rules']:       
                # Recorrido de los log_sources contenidos en la informacion de la ofensa         
                if 'log_sources' in list(ofensas.keys()):
                    for log in ofensas['log_sources']: 
                        
                        rul_id          = rul['id']
                        type_log_id     = log['type_id']
                        type_log_name   = log['type_name']                    
                        log_id          = log['id']
                        log_name        = log['name']

                        if ejecucion == "manual":
                            print(f"offense_id: {offense_id}; start_time: {start_time}; event_count: {event_count}; domain_name: {domain_name}; type_log_id: {type_log_id}; type_log: {type_log}; log_id: {log_id}; log_name: {log_name}; rul_id: {rul_id}")

                        list_query.append("INSERT INTO qr_offenses (offense_id, domain, log_source_id, log_source_name, rule_id, event_count, type_log_id, type_log_name, offense_state, start_time, stop_time, create_time) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())")
                        list_data.append((offense_id, domain_name, log_id, log_name, rul_id, event_count, type_log_id, type_log_name, offense_state, start_time, stop_time))
                        insertadas +=1

        if list_query != []:
            db.multi_insert(list_query, list_data)

    else: 
        no_insertadas +=1

print(f"Ofensas insertadas       =   {insertadas}")
print(f"Ofensas no_insertadas    =   {no_insertadas}")
print("Script finalizado: ", (datetime.now() - init_date) / timedelta(seconds=1))