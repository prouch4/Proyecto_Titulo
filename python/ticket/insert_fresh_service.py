#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Determina cual es el ultimo ticket guardado en base de datos, en caso de no existir,
# busca el primer ticket en FRESH SERVICE para registrar todos los ticket que no se encuentren registrados
# en la base de datos, guardando el ticket original de FRESH SERVICE y la adaptacion al sistema antigüo de OTRS 

#####################################
# IMPORTS NECESARIOS                #
#####################################

import correction_for_db as patch
import get_fresh_service as get_fs

import connection_db as db2

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

def syncro_insert_fs_otrs(tickets, columns_db):
    tickets = tickets[list(tickets)[0]] 

    while len(tickets) > 0:
        ticket = tickets.pop(0)

        if patch.autotesting_checker(ticket) == False:

            if ticket["deleted"] == 0:
                # INSERT EN OTRS (COPIA)
                ticket_otrs = patch.modification_for_otrs(ticket)
                
                columns         = ", ".join(list(ticket_otrs.keys()))
                insert_values   = str('%s, ' * len(ticket_otrs)).rstrip(', ')
                query           = f'INSERT INTO ticket ({columns}) VALUES ({insert_values})'
                db2.single_insert_disable_keys(query, list(ticket_otrs.values()))

                del columns, insert_values, query
                del ticket_otrs

            # INSERT EN FS (LOCAL)
            ticket_fs = ticket
            ticket_fs["fs_id"] = ticket_fs.pop("id")
            
            # DESCARTADOS
            ticket_fs.pop("attachments")        # POR FUTURO INSERT
            ticket_fs.pop("description")        # REPETIDO DE DESCTRIPCION_TEXT
            ticket_fs.pop("description_text")   # PESA DEMASIADO

            # AQUI SE PUEDEN COMPARAR
            columns_db = columns_detector(ticket_fs, columns_db, "z_fs_ticket")

            columns         = ', '.join(list(ticket_fs.keys()))
            insert_values   = str('%s, ' * len(ticket_fs)).rstrip(', ')
            query           = f'INSERT INTO z_fs_ticket ({columns}) VALUES ({insert_values})'
            db2.single_insert_disable_keys(query, list(ticket_fs.values()))
            del columns, insert_values, query
            del ticket_fs
        
def first_and_last_ticket():
    
    # OBTENCION DEL ULTIMO TICKET GUARDADO O EL PRIMER TICKET DE FS
    query        = "SELECT cast(tn as signed) as tn FROM ticket WHERE fs_ticket = 'T' ORDER BY tn DESC LIMIT 1"
    first_ticket = db2.get_single_data(query)
    
    if first_ticket == None:
        try:
            first_ticket = get_fs.fresh_service_get("first_ticket")["tickets"][0]["id"]
        except:
            print("Imposible obtener el primer ticket")
            first_ticket = None
    else:
        first_ticket = (first_ticket[0] + 1)

    # OBTENCION DEL ULTIMO TICKET DE FS
    try:
        last_ticket = get_fs.fresh_service_get("last_ticket")["tickets"][0]["id"]
    except:
        print("Imposible obtener el ultimo ticket")
        last_ticket = None

    return {"first_ticket" : first_ticket , "last_ticket" : last_ticket}


columns_db  = get_table_columns("z_fs_ticket")
ticket_ids  = first_and_last_ticket()
index       = ticket_ids["first_ticket"]
final       = ticket_ids["last_ticket"]

while index <= final:
    print(f"{index} / {final}")
    # tickets = get_fs.fresh_service_get("ticket_specific", index)
    tickets = get_fs.fresh_service_get("ticket_specific_2", index)

    if isinstance(tickets, dict):
        # tickets = get_fs.custom_field_corrector(tickets)
        tickets = get_fs.dictionary_field_corrector(tickets)
        syncro_insert_fs_otrs(tickets, columns_db)
    
    else :
        print(tickets)
    index += 1

print("")
