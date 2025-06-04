#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

import connection_db as db2 # Archivo de prueba

#####################################
# SCRIPT                            #
#####################################

def translate_fs_columns(table, filter, id):
    query = f"SELECT otrs_id FROM {table} WHERE {filter} = %s"
    result = db2.get_single_data(query, (id,))

    if not result:
        return None
    else:
        return result[0]
    
def translate_multi_fs_columns(table, data_dict):

    # WHERE constructor
    filter = ""
    ids    = []

    for keys in data_dict:
        filter += f"{keys} = %s"
        ids.append(data_dict[keys])
        if keys != list(data_dict)[-1]:
            filter += " AND "

    query = f"SELECT otrs_id FROM {table} WHERE {filter}"
    result = db2.get_single_data(query, ids)

    if not result:
        return None
    else:
        return result[0]

def translate_service_id(ticket_id, data_dict, table = "z_service_dictionary", use_type = False):
    # FILTER constructor
    filters = []

    for keys in data_dict:
        if isinstance(data_dict[keys], str):
            value = data_dict[keys]
            filters.append(f"({keys} = '{value}')")
        elif isinstance(data_dict[keys], int): 
            value = data_dict[keys]
            filters.append(f"({keys} = {value} )")
        else:
            filters.append(f"({keys} is Null)")

    where = " AND ".join(filters)
    query = f"SELECT * FROM {table} WHERE {where}"

    try:
        result = db2.get_single_data(query)
    except:
        print(f"ERROR AL OBTENER RESULTADOS --- {ticket_id}")
        print(query)
        result = None

    if not result:
        return None
    else:
        if use_type == False:
            return result[1]
        else:
            return result[4]

def modification_for_otrs(ticket):
    ticket_dict = {}

    if "fs_id" in list(ticket.keys()):
        try:
            ticket_dict["tn"] = ticket["fs_id"]
        except:
            ticket_dict["tn"] = None
    elif "id" in list(ticket.keys()):
        try:
            ticket_dict["tn"] = ticket["id"]
        except:
            ticket_dict["tn"] = None
    
    try:
        ticket_dict["title"] = ticket["subject"]
    except:
        ticket_dict["title"] = None
    
    try:
        ticket_dict["queue_id"] = translate_fs_columns("z_queue_dictionary", "fs_id", ticket["group_id"])
    except:
        ticket_dict["queue_id"] = None


    # NEW TYPE
    try:
        temp_var = {
            "fs_category"       : ticket["category"],
            "fs_sub_category"   : ticket["sub_category"],
            "fs_item_category"  : ticket["item_category"],
            "fs_plataforma"     : ticket["plataforma"],
            "fs_producto"       : ticket["producto"],
            "fs_sla_policies"   : ticket["sla_policies_id"],
        }
        ticket_dict["type_id"] = translate_service_id(ticket_dict["tn"], temp_var, use_type=True)
        del temp_var
    except:
        ticket_dict["type_id"] = None

    # OLD TYPE
    if ticket_dict["type_id"] == None:
        print("USANDO type_id viejo, no se encontro en el nuevo")
        try:
            temp_var = {
                "fs_category"       : ticket["category"],
                "fs_sub_category"   : ticket["sub_category"],
                "fs_item_category"  : ticket["item_category"],
            }
            ticket_dict["type_id"] = translate_multi_fs_columns("z_type_dictionary", temp_var)
            del temp_var
        except:
            ticket_dict["type_id"] = None
    
    # GET SERVICE_ID
    try:
        temp_var = {
            "fs_category"       : ticket["category"],
            "fs_sub_category"   : ticket["sub_category"],
            "fs_item_category"  : ticket["item_category"],
            "fs_plataforma"     : ticket["plataforma"],
            "fs_producto"       : ticket["producto"],
            "fs_sla_policies"   : ticket["sla_policies_id"],
        }
        ticket_dict["service_id"] = translate_service_id(ticket_dict["tn"], temp_var)
        del temp_var
    except:
        ticket_dict["service_id"] = None
    
    try:
        ticket_dict["sla_id"] = translate_fs_columns("z_sla_dictionary", "fs_id", ticket["sla_policies_id"])
    except:
        ticket_dict["sla_id"] = None
    
    try:
        temp_var = {
            "fs_priority"   :   ticket["priority"],
            "fs_impact"     :   ticket["impact"],
            "fs_urgency"    :   ticket["urgency"],
        }
        ticket_dict["ticket_priority_id"] = translate_multi_fs_columns("z_priority_dictionary", temp_var)
        del temp_var
    except:
        ticket_dict["ticket_priority_id"] = None
    
    try:
        ticket_dict["ticket_state_id"] = translate_fs_columns("z_status_dictionary", "fs_id", ticket["status"])
    except:
        ticket_dict["ticket_state_id"] = None
    
    try:
        ticket_dict["customer_id"] = translate_fs_columns("z_customer_dictionary", "fs_id", ticket["department_id"])
    except:
        ticket_dict["customer_id"] = None
    
    try:
        ticket_dict["create_time"] = ticket["created_at"]
    except:
        ticket_dict["create_time"] = None
    
    try:
        ticket_dict["change_time"] = ticket["updated_at"]
    except:
        ticket_dict["change_time"] = None

    try:
        ticket_dict["user_id"]              = ticket["requester_id"]
        ticket_dict["responsible_user_id"]  = ticket["requester_id"]
        ticket_dict["create_by"]            = ticket["requester_id"]
        ticket_dict["change_by"]            = ticket["requester_id"]
    except:
        ticket_dict["user_id"]              = None
        ticket_dict["responsible_user_id"]  = None
        ticket_dict["create_by"]            = None
        ticket_dict["change_by"]            = None

    # CAMPOS TEMPORALMENTE TRUNCADOS
    ticket_dict["ticket_lock_id"]           = 1
    ticket_dict["customer_user_id"]         = ticket_dict["customer_id"]
    ticket_dict["timeout"]                  = 0
    ticket_dict["until_time"]               = 0
    ticket_dict["escalation_time"]          = 0
    ticket_dict["escalation_update_time"]   = 0
    ticket_dict["escalation_response_time"] = 0
    ticket_dict["escalation_solution_time"] = 0
    ticket_dict["archive_flag"]             = 0
    ticket_dict["fs_ticket"]                = "T"

    return ticket_dict

def autotesting_checker(ticket):
    try:
        if "Autotesting" == ticket["item_category"]:
            print(f"{ticket['id']} El ticket pertenece a Autotesting, Omitiendo...\n")
            return True
        else:
            
            return False
    except:
        print("Item_category Null recibido")
        return False