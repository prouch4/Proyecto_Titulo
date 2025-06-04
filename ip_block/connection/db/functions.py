#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# La función transferir_datos necesita información de la tabla z_fs_ticket, de las columnas fs_id, created_at y body_text
# y de la tabla user de la columna audi_user. La función separa la información contenida en body_text segun las "," y ";" ip, source_ips e info_risk
# luego rellena cada una de esas columnas respectivamente en la tabla company_ipblock, además de nro_ticket, block_date e insert_date
# con la información de fs_id, closed_at y la fecha actual respectivamente. Finalmente la información obtenida de la tabla user se inserta en
# user_insert, user_update, audi_user, audi_date se rellena con la fecha actual y audi_action con una "I" ya que es una inserción.

# La función obtener_info_tabla realiza una consulta según la query y columnas que se le indique, luego se asegura de que la información
# retornada este en forma de diccionario para poder ser procesada posteriormente.

#####################################
# IMPORTS NECESARIOS                #
#####################################

import os
import sys
import re

ruta_connection = os.path.abspath(os.path.join('/', 'opt', 'python_process', 'python', 'ticket'))
sys.path.append(ruta_connection)
import connection_db as db2

#####################################
# SCRIPT                            #
#####################################

def transferir_datos(fs_id, cls_conn, cursor, body_text, closed_at, audi_user):
    
    # Procesar valores vacíos
    def procesar_valor(valor):
        valor = valor.strip()  # Eliminar espacios en blanco
        return valor if valor != '' else None
    
    def extraer_parte_numerica(cadena):
    # Usar re.search para encontrar la primera ocurrencia de uno o más dígitos
        match = re.search(r'\d+', cadena)
        if match:
            return match.group()  # Retorna el grupo de dígitos encontrado
        else:
            return None

    # Convertir bodytext a cadena si no lo es
    if not isinstance(body_text, str):
        body_text = str(body_text)

    def insertar_company_ipblock (query, data):

        # Verificar si la IP ya está en la tabla
        query_verificar = "SELECT COUNT(*) FROM portal.company_ipblock WHERE ip = %s"
        resultado = db2.__get_data__(cls_conn, query_verificar, (ip,), multi=True)

        # Si no está duplicado, proceder a insertar
        if resultado and isinstance(resultado, list) and resultado[0].get('COUNT(*)', -1) == 0:
            print(f"No hay IP duplicada: {ip}, se procederá a insertar.")
            data_list = list(data)
            db2.__insert_data__(cls_conn, query, data_list)

        else:
            print(f"IP duplicada: {ip}. No se insertará este registro.")



    # Separar bodytext por puntos y coma para obtener los diferentes conjuntos
    conjuntos = body_text.split(';')
    
    # Para cada conjunto, separarlo por comas e insertar los valores en la tabla destino
    for conjunto in conjuntos:
        columnas = conjunto.split(',')
        
        if len(columnas) == 4:
            ip, info_risk, nticket, source_ips = map(procesar_valor, columnas)  # Procesar cada valor
            ticket_ofensa = extraer_parte_numerica(nticket)
            query_ipblock = "INSERT INTO portal.company_ipblock (ticket_ofensa, nro_ticket, ip, source_ips, info_risk, block_date, insert_date, update_date, user_insert, user_update, audi_user, audi_date, audi_action) VALUES (%s, %s, %s, %s, %s, %s, NOW(),NOW(), %s, %s, %s, NOW(), 'I')"
            insertar_company_ipblock(query_ipblock, (ticket_ofensa, fs_id, ip, source_ips, info_risk, closed_at, audi_user, audi_user, audi_user))

def obtener_info_tabla(cls_conn, query, columns, data = None, multi = False):

    if columns is None or not isinstance(columns, list) or len(columns) == 0:
        print("Error: El parámetro 'columns' no es válido. Debe ser una lista no vacía.")
        return {col: None for col in columns}  # Retornar un diccionario con None para cada columna

    # Ejecutar la consulta
    resultado = db2.__get_data__(cls_conn, query, data=data, multi=multi)

    # Verificar si se obtuvo un resultado
    if resultado is None:
        print("No se encontraron resultados o el resultado fue None.")
        return {col: None for col in columns}  # Retorna un diccionario con None para cada columna

    # Si la conexión está en modo diccionario, 'resultado' será un diccionario
    if isinstance(resultado, dict):
        return {col: resultado.get(col, None) for col in columns}

    # Si el resultado es una tupla (resultado por posición), lo convertimos a un diccionario
    elif isinstance(resultado, tuple):
        if len(resultado) == len(columns):
            return dict(zip(columns, resultado))
        else:
            print("Error: El número de columnas no coincide con el número de resultados.")
            return {col: None for col in columns}  # Retorna un diccionario con None si hay desajuste

    else:
        print("Error: Tipo de resultado no reconocido.")
        return {col: None for col in columns}