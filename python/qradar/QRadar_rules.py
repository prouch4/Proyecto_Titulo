# Consulta las reglas en QRadar y detecta los cambios
from datetime import datetime

import connection_qradar as qradar
import connection_db as db

# Inserta las reglas nuevas en db
def insert_query(insert_array):
    query  = ""

    columns         = ",".join(list(insert_array.keys()))
    values          = list(insert_array.values())
    escape_values   = str("%s, " * len(values)).rstrip(', ')

    query = f"INSERT INTO qr_rules ({columns}) VALUES ({escape_values})" # CREANDO QUERY

    return (query, escape_values)

# Remplaza las reglas actualizadas en db
def update_query(old_data, new_data):
    # Array para facilitar la comparacion
    old_data = {
        'id'                : old_data[0],
        'name'              : old_data[1],
        'enable'            : old_data[2],
        'origin'            : old_data[3],
        'owner'             : old_data[4],
        'type'              : old_data[5],
        'creation_date'     : old_data[6],
        'modification_date' : old_data[7],
    }

    update_text     = []
    update_values   = []
    
    # RECORRIDO DE LOS VALORES A REMPLAZAR
    for keys in new_data:
        if new_data[keys] != None:
            if new_data[keys] == old_data[keys]:
                pass # Sin cambios
            elif str(new_data[keys]) == str(old_data[keys]):
                pass # Sin cambios
            else:
                # Cambios detectados
                update_text.append(f"{keys} = %s")
                update_values.append(new_data[keys])
        else:
            pass

    if update_text != [] and update_values != []:
        print(f"{datetime.now()} Se detectaron cambios en la regla {new_data['id']}, Preparando actualizacion")
        
        update_text     = f"UPDATE qradar_portal.qr_rules SET {(', '.join(update_text))} WHERE (id = {new_data['id']})"
        update_values   = update_values
        return (update_text, update_values)

    else:
        return None

# Verifica si la regla esta ingresada
def get_old_rules():
    
    query  = f"SELECT * FROM qr_rules"
    result = db.get_single_data(query, dictionary = True)

    if result is None:
        return None
    else:
        return result

# Verifica si la regla esta ingresada
def check_rules(rule_id):
    
    query   = f"SELECT * FROM qr_rules WHERE id = {rule_id}"
    result  = db.get_multi_data(query)
    
    if result is None:
        return None
    else:
        return result

def check_rules_2(old_rules, rule_id):
    output_var = None

    for rule in old_rules:
        if rule['id'] == rule_id:
            output_var = tuple(output_var.values())
            break

    return output_var

# =========================
# EJECUCION DEL SCRIPT
# =========================

response = qradar.get_qradar("rules") # Obtencion de las reglas existentes en QRadar

if response != None:
    print("Reglas recibidas, revisando {0} reglas".format(len(response)))
    
    list_insert = []
    data_insert = []

    list_update = []
    data_update = []

    # Recorrido de las reglas
    for rule in response:

        # Correcion de los timestamp
        try:
            creation_date     = datetime.fromtimestamp(rule['creation_date'] / 1e3).strftime("%Y-%m-%d %H:%M:%S")
        except:
            creation_date     = None
        try:
            modification_date = datetime.fromtimestamp(rule['modification_date'] / 1e3).strftime("%Y-%m-%d %H:%M:%S")
        except:
            modification_date = None

        # Construccion del array con la informacion que se insertara
        insert_array = {
            'id'                : rule['id'],
            'name'              : rule['name'],
            'enable'            : rule['enabled'],
            'origin'            : rule['origin'],
            'owner'             : rule['owner'],
            'type'              : rule['type'],
            'creation_date'     : creation_date,
            'modification_date' : modification_date,
        }

        old_data = check_rules(insert_array['id'])

        if  old_data != None:
            query = update_query(old_data, insert_array)
            if query != None:
                data_update.append(query[1])
                list_update.append(query[0])

        else:
            query = insert_query(insert_array)
            data_insert.append(query[1])
            list_insert.append(query[0])

    # AQUI VAN LOS INSERT
    db.multi_insert(list_insert, data_insert)
    db.multi_insert(list_update, data_update)
