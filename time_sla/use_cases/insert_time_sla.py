import helpers.utils_helper as util

def insert_time_sla(db, api, cls_conn):
    """
    El código selecciona los tickets que no tienen registrado el tiempo de cierre, luego cuenta cuantos son y uno por uno va calculando el tiempo de cierre en base a los parámetros
    closed_at y fechahora_ofensa. El número obtenido en ese cálculo es insertado en la columna tiempo_de_cierre y se actualiza en la api de app a través del método PUT.
    Finalmente se revisa que se haya hecho el registro de manera correcta y se rellena una variable booleana llamada tiempo_de_cierre_actualizado para dejar constancia de que se hizo el registro.
    
    """
    query_close_time = "SELECT fs_id, fechahora_ofensa, closed_at, tiempo_de_cierre FROM otrs.z_fs_ticket WHERE fechahora_ofensa IS NOT NULL AND closed_at IS NOT NULL AND tiempo_de_cierre IS NULL AND deleted != 1;"
    tickets = db.__get_data__(cls_conn, query_close_time, data = None, multi = True)

    if not tickets:
        print("No se encontró ningún ticket.")

    else:
        cant_tickets = len(tickets)
        print("Cantidad de tickets a actualizar: ", cant_tickets)
        print("")
        count = 0
        countFail = 0

        for ticket in tickets:
            fs_id, _, _, _ = ticket 
            _, fechahora_ofensa, _, _ = ticket
            _, _, closed_at, _ = ticket
            print("Ticket: ", fs_id)

            # Cálculo de tiempo de cierre
            formato_fecha = "%Y-%m-%d %H:%M:%S"
            closed_at_str = closed_at.strftime(formato_fecha)
            fechahora_ofensa_str = fechahora_ofensa.strftime(formato_fecha)
            closetime = util.diferencia_en_minutos(closed_at_str, fechahora_ofensa_str)

            # Actualización de base de datos
            query_insert = "UPDATE otrs.z_fs_ticket SET tiempo_de_cierre = %s WHERE fs_id = %s"
            db.__insert_data__(cls_conn, query_insert, (closetime, fs_id))

            # Consulta para confirmar si se actualizó correctamente
            query_consulta = "SELECT fs_id, tiempo_de_cierre FROM otrs.z_fs_ticket WHERE fechahora_ofensa IS NOT NULL AND closed_at IS NOT NULL AND tiempo_de_cierre IS NOT NULL AND deleted != 1 AND fs_id = %s"
            result = db.__get_data__(cls_conn, query_consulta, (fs_id,))
            if result != None:
                print("Registro exitoso en la base de datos.")
                print("Avance del proceso: paso 1 de 2")
            else:
                print("No fue posible registar en la base de datos.")
                countFail += 1

            # Actualización de API
            _, tiempo_de_cierre = result
            update = api.fresh_service_put('ticket_specific_2', 'custom_fields', 'tiempo_de_cierre', tiempo_de_cierre, fs_id)

            # Consulta para confirmar si se actualizó correctamente, si el resultado es positivo se registra en la base de datos
            close_time = update.get('ticket', {}).get('custom_fields', {}).get('tiempo_de_cierre', None)
            query_update = "UPDATE otrs.z_fs_ticket SET tiempo_de_cierre_actualizado = true WHERE fs_id = %s"
            if close_time == tiempo_de_cierre:
                db.__insert_data__(cls_conn, query_update, (fs_id,))
                print("Ticket actualizado correctamente.")
                print("Avance del proceso: paso 2 de 2")
                print("")
                count += 1
            else:
                print("No fue posible actualizar el ticket.")
                countFail += 1

        print("Cantidad de tickets actualizados: ", count)
        print ("Cantidad de tickets sin actualizar: ", countFail)
