#####################################
# IMPORTS NECESARIOS                #
#####################################

import mysql.connector
from time import sleep

#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################
# Este script contiene funciones para realizar transacciones en una base de datos MySQL.

def __try_open__(connection_params: dict, dictionary: bool):
    """
    Establece una conexión a una base de datos MySQL con los parámetros dados.

    Args:
        connection_params (dict): Un diccionario con los parámetros de conexión para MySQL.
        dictionary (bool): Indica si los resultados de las consultas se devolverán como diccionarios.

    Returns:
        cls_conn: Una clase que contiene la conexión, el cursor y un indicador de estado de la conexión.

    Funcionalidad:
        - Intenta conectarse a la base de datos hasta un máximo de 3 veces en caso de error.
        - Devuelve un objeto con la conexión establecida o None si la conexión falla después de 3 intentos.
    """

    class cls_conn:
        connection = None
        cursor = None
        check_conn = False

    try_count = 0  # Contador de repeticiones
    while cls_conn.check_conn == False:
        try_count += 1

        try:
            cls_conn.connection = mysql.connector.connect(**connection_params)
            cls_conn.cursor = cls_conn.connection.cursor(dictionary=dictionary)
            cls_conn.check_conn = True

        except mysql.connector.Error as sql_error:
            print(sql_error, f"... Intento N°{try_count}")

            cls_conn.check_conn = False
            # if sql_error.errno == 2003:
            #     check_conn  = False

        if try_count >= 3:  # Forzar salida al tercer intento
            break
        else:
            sleep(3)

    return cls_conn


def __try_close__(class_connection):
    """
    Cierra la conexión y el cursor a una base de datos MySQL si la conexión está activa.

    Args:
        class_connection: La clase que contiene la conexión y el cursor.

    Returns:
        class_connection: La clase con la conexión cerrada.

    Funcionalidad:
        - Cierra el cursor y la conexión si están abiertos.
    """
    if class_connection.check_conn == True:
        class_connection.cursor.close()
        class_connection.connection.close()

    return class_connection


def __on_off_foreign_key__(class_connection, enable):
    """
    Activa o desactiva la verificación de claves foráneas en una base de datos MySQL.

    Args:
        class_connection: La clase que contiene la conexión y el cursor.
        enable (int): 1 para activar las verificaciones de claves foráneas, 0 para desactivarlas.

    Funcionalidad:
        - Ejecuta el comando SQL `SET FOREIGN_KEY_CHECKS` para activar o desactivar las verificaciones de claves foráneas.
        - Confirma la operación.
    """
    class_connection.cursor.execute(
        f"SET FOREIGN_KEY_CHECKS = {enable}"
    )  # DISABLE FOREIGN KEYS
    class_connection.connection.commit()


def __get_data__(class_connection, query, data=None, multi: bool = False):
    """
    Ejecuta una consulta SQL en una base de datos MySQL y devuelve los resultados.

    Args:
        class_connection: La clase que contiene la conexión y el cursor.
        query (str): La consulta SQL a ejecutar.
        data (tuple, optional): Los datos que se deben pasar a la consulta.
        multi (bool, optional): Indica si se deben devolver múltiples filas o una sola fila. Por defecto, es False.

    Returns:
        result: El resultado de la consulta, ya sea una fila, múltiples filas o None en caso de error o sin resultados.

    Funcionalidad:
        - Ejecuta la consulta con o sin parámetros.
        - Devuelve el primer resultado (si multi=False) o todos los resultados (si multi=True).
        - Imprime un mensaje de error si ocurre un error durante la consulta.
    """
    try:
        if data == None:
            class_connection.cursor.execute(query)
        else:
            class_connection.cursor.execute(query, data)

        if multi == False:
            result = class_connection.cursor.fetchone()
        elif multi == True:
            result = class_connection.cursor.fetchall()
    except mysql.connector.Error as error:
        # Si se produce un error, deshacer la transacción
        print(f"Error al consultar la base de datos: {error}")
        result = None

    if not result:
        result = None

    return result


def __insert_data__(class_connection, query, data=None):
    """
    Inserta datos en una base de datos MySQL y maneja transacciones.

    Args:
        class_connection: La clase que contiene la conexión y el cursor.
        query (str): La consulta SQL de inserción a ejecutar.
        data (tuple, optional): Los datos que se deben pasar a la consulta.

    Funcionalidad:
        - Inicia una transacción antes de ejecutar la consulta.
        - Ejecuta la consulta de inserción con o sin parámetros.
        - Confirma la transacción si la inserción fue exitosa.
        - Si ocurre un error, realiza un rollback de la transacción y lo imprime.
    """
    try:
        # Iniciar una transacción
        class_connection.cursor.execute("START TRANSACTION")

        # Realizar las operaciones necesarias en la base de datos
        if data == None:
            class_connection.cursor.execute(query)
        else:
            class_connection.cursor.execute(query, data)

        # Si todo ha ido bien, confirmar la transacción
        class_connection.cursor.execute("COMMIT")
    except mysql.connector.Error as error:

        # Si se produce un error, deshacer la transacción
        print("Error al crear en la base de datos: {0}".format(error))
        class_connection.cursor.execute("ROLLBACK")


def __insert_multi_data__(class_connection, list_query, list_data=None):
    """
    Inserta múltiples conjuntos de datos en una base de datos MySQL y maneja transacciones.

    Args:
        class_connection: La clase que contiene la conexión y el cursor.
        list_query (list): Lista de consultas SQL de inserción a ejecutar.
        list_data (list of tuples, optional): Lista de datos que se deben pasar a cada consulta.

    Funcionalidad:
        - Inicia una transacción antes de ejecutar las consultas.
        - Ejecuta cada consulta de inserción de la lista con o sin parámetros correspondientes.
        - Confirma la transacción si todas las inserciones fueron exitosas.
        - Si ocurre un error, realiza un rollback de la transacción y lo imprime.
    """
    try:
        # Iniciar una transacción
        class_connection.cursor.execute("START TRANSACTION")

        # Realizar las operaciones necesarias en la base de datos
        index = 0
        for query in list_query:

            if list_data == None:
                class_connection.cursor.execute(query)
            else:
                class_connection.cursor.execute(query, list_data[index])
                index += 1

        # Si todo ha ido bien, confirmar la transacción
        class_connection.cursor.execute("COMMIT")
    except mysql.connector.Error as error:

        # Si se produce un error, deshacer la transacción
        print("Error al crear en la base de datos: {0}".format(error))
        class_connection.cursor.execute("ROLLBACK")

