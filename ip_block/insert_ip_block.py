#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# Extrae las IP bloqueadas contenidas en un ticket a través de la API de 
# app, luego las inserta en la columna body_text en la tabla z_fs_ticket en la 
# BBDD OTRS, luego desde esa tabla separa la información en ip, source_ip e 
# info_risk y las inserta en la tabla company_ipblock en la BBDD PORTAL junto con 
# el ticket de ofensa, número de ticket y fechas de bloqueo e inserción, además extrae
# desde la tabla user, de la BBDD portal, el n de audi_user para insertarlo también en company_ipblock

#####################################
# IMPORTS NECESARIOS                #
#####################################

import os
import sys
import time
import connection.api.config as cf_api
import connection.db.config as cf_db
import connection.api.extract_fs as ext
import connection.db.functions as fun

ruta_connection = os.path.abspath(os.path.join('/', 'opt', 'python_process', 'python', 'ticket'))
sys.path.append(ruta_connection)
import connection_db as db2

ruta_get_fs = os.path.abspath(os.path.join('/', 'opt', 'python_process', 'python', 'ticket'))
sys.path.append(ruta_get_fs)
import get_fresh_service as get_fs

#####################################
# SCRIPT                            #
#####################################

def print_section_header(header):
    print("\n" + "="*60)
    print(f"===== {header} =====")
    print("="*60 + "\n")

start_time = time.time()
print_section_header(f"Inicio de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

if __name__ == "__main__":

    # Se abren las conexiones
    cls_conn = db2.__try_open__(cf_db.connection_params, dictionary=True)
    if cls_conn.check_conn:
        cursor = cls_conn.cursor

    # Obtener todos los fs_id
    query_ticket = "SELECT fs_id FROM otrs.z_fs_ticket WHERE closed_at IS NOT NULL AND item_category = 'Deploy Programado' AND body_text IS NULL ORDER BY id DESC;"
    tickets = db2.__get_data__(cls_conn, query_ticket, data=None, multi=True)

    if not tickets:
        print("No se encontró ningún ticket.")

    else: 
        # Procesar cada ticket en un ciclo
        for ticket in tickets:
            fs_id = ticket['fs_id'] if isinstance(ticket, dict) else ticket
            print("Nro ticket: ", fs_id)

            # Consulta a la API
            response = get_fs.fresh_service_get('conversations', fs_id)
            body_text = ext.obtener_body_text(fs_id, cf_api.domain, response)

            if body_text == None:
                print("Formato incorrecto en ticket: ", fs_id)
            else:
                print("Se encontró body_text en ticket: ", fs_id)

            # Insertar body_text en z_fs_ticket
            query = "UPDATE otrs.z_fs_ticket SET body_text = %s WHERE fs_id = %s"
            db2.__insert_data__(cls_conn, query, (body_text, fs_id))

            # Obtener datos actualizados de z_fs_ticket
            query_z_fs_ticket = "SELECT fs_id, closed_at, body_text FROM otrs.z_fs_ticket WHERE fs_id = %s"
            columnas_z_fs_ticket = ['fs_id', 'closed_at', 'body_text']
            info_z_fs_ticket = fun.obtener_info_tabla(cls_conn, query_z_fs_ticket, columnas_z_fs_ticket, data = (fs_id,))
            fs_id = info_z_fs_ticket.get('fs_id')
            closed_at = info_z_fs_ticket.get('closed_at')
            body_text = info_z_fs_ticket.get('body_text')

            if body_text != None:
                    print("Registro exitoso en z_fs_ticket")

            # Obtener datos de user
            query_user = "SELECT id as audi_user, audi_action FROM portal.user WHERE user_name = 'prueba_es'"
            columnas_user = ['audi_user']
            info_user = fun.obtener_info_tabla(cls_conn, query_user, columnas_user)
            audi_user = info_user.get('audi_user')

            # Insertar datos en company_ipblock
            fun.transferir_datos(fs_id, cls_conn, cursor, body_text, closed_at, audi_user)

    # Se cierra la conexión al final
    db2.__try_close__(cls_conn)

end_time = time.time()
execution_time = end_time - start_time
print_section_header(f"Fin de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
print(f"Tiempo total de ejecución: {execution_time:.2f} segundos")

