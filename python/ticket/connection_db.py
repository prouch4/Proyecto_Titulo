#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

#####################################
# IMPORTS NECESARIOS                #
#####################################

import mysql.connector
from time import sleep
from dotenv import dotenv_values

#####################################
# SCRIPT                            #
#####################################

env_vars = dotenv_values("/opt/python_process/.env")
connection_params = {
    "host"      : env_vars["DB_HOST"],
    "user"      : env_vars["DB_USER"],
    "password"  : env_vars["DB_PASSWORD"],
    "database"  : env_vars["DB_NAME_1"],
}

def __try_open__(connection_params : dict, dictionary : bool):
    class cls_conn:
        connection  = None
        cursor      = None
        check_conn  = False

    try_count  = 0 # Contador de repeticiones
    while cls_conn.check_conn == False:
        try_count += 1
        
        try:
            cls_conn.connection  = mysql.connector.connect(**connection_params)
            cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
            cls_conn.check_conn  = True

        except mysql.connector.Error as sql_error:
            print(sql_error, f"... Intento N°{try_count}")
            
            cls_conn.check_conn  = False
            # if sql_error.errno == 2003:
            #     check_conn  = False
            

        if try_count >= 3: # Forzar salida al tercer intento
            break
        else:
            sleep(3)

    return cls_conn

def __try_close__(class_connection):
    if class_connection.check_conn == True:
        class_connection.cursor.close()
        class_connection.connection.close()

    return class_connection

def __on_off_foreign_key__(class_connection, enable):
    class_connection.cursor.execute(f"SET FOREIGN_KEY_CHECKS = {enable}") # DISABLE FOREIGN KEYS
    class_connection.connection.commit()

def __get_data__(class_connection, query, data = None, multi : bool = False):
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

def __insert_data__(class_connection, query, data = None):
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

def __insert_multi_data__(class_connection, list_query, list_data = None):
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

# SOLO ES UNA DEMO, HACE ABSOLUTAMENTE NADA
def try_base(dictionary = False):

    def try_open(connection_params, dictionary):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                

            if try_count >= 3: # Forzar salida al tercer intento
                break
            else:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection

    class_connection = try_open(connection_params, dictionary)

    # AQUI VA LA QUERY
    if class_connection.check_conn:
        pass
        print('inserte QUERY')
    
    class_connection = try_close(class_connection)

# Ejecuta querys que solo retornan un resultado
def get_single_data(query, data = None, dictionary = False):

    def try_open(connection_params, dictionary = False):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                
            if try_count >= 3 and cls_conn.check_conn == False: # Forzar salida al tercer intento
                break
            elif try_count < 3 and cls_conn.check_conn == False:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection
    
    def get_data(class_connection, query, data = None):
        try:
            if data == None:
                class_connection.cursor.execute(query)
            else:
                class_connection.cursor.execute(query, data)
            
            result = class_connection.cursor.fetchone()
        except mysql.connector.Error as error:        
            # Si se produce un error, deshacer la transacción
            print(f"Error al consultar la base de datos: {error}")
            result = None

        if not result:
            result = None

        return result
    
    class_connection = try_open(connection_params, dictionary)

    # AQUI VA LA QUERY
    if class_connection.check_conn:
        result = get_data(class_connection, query, data)
    
    class_connection = try_close(class_connection)

    return result

# Ejecuta querys que retornan multiples resultados
def get_multi_data(query, data = None, dictionary = False): # Retorna multiples resultados
    
    def try_open(connection_params, dictionary):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                

            if try_count >= 3 and cls_conn.check_conn == False: # Forzar salida al tercer intento
                break
            elif try_count < 3 and cls_conn.check_conn == False:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection

    def get_data(class_connection, query, data = None):
        try:
            if data == None:
                class_connection.cursor.execute(query)
            else:
                class_connection.cursor.execute(query, data)
        
            result = class_connection.cursor.fetchall()
        except mysql.connector.Error as error:
            # Si se produce un error, deshacer la transacción
            print(f"Error al consultar la base de datos: {error}")

            result = None

        if not result:
            result = None

        return result

    class_connection = try_open(connection_params, dictionary)

    # AQUI VA LA QUERY
    if class_connection.check_conn:
        result = get_data(class_connection, query, data)
    
    class_connection = try_close(class_connection)

    return result

def single_insert(query, values = None, dictionary = False):

    def try_open(connection_params, dictionary):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                

            if try_count >= 3 and cls_conn.check_conn == False: # Forzar salida al tercer intento
                break
            elif try_count < 3 and cls_conn.check_conn == False:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection

    def execute_query(class_connection, query, values):
        try:
            # Iniciar una transacción  
            class_connection.cursor.execute("START TRANSACTION")

            # Realizar las operaciones necesarias en la base de datos
            if values == None:
                class_connection.cursor.execute(query)
            else:
                class_connection.cursor.execute(query, values)

            # Si todo ha ido bien, confirmar la transacción
            class_connection.cursor.execute("COMMIT")
        except mysql.connector.Error as error:

            # Si se produce un error, deshacer la transacción
            print("Error al crear en la base de datos: {0}".format(error))
            class_connection.cursor.execute("ROLLBACK")

    class_connection = try_open(connection_params, dictionary)

    # AQUI VA LA QUERY
    if class_connection.check_conn:
        execute_query(class_connection, query, values)
    
    class_connection = try_close(class_connection)

def multi_insert(list_query, list_data = None, dictionary = False):

    def try_open(connection_params, dictionary):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                

            if try_count >= 3 and cls_conn.check_conn == False: # Forzar salida al tercer intento
                break
            elif try_count < 3 and cls_conn.check_conn == False:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection

    def execute_query(class_connection, list_query, list_data):
        try:
            # Iniciar una transacción  
            class_connection.cursor.execute("START TRANSACTION")
            
            # Realizar las operaciones necesarias en la base de datos
            index = 0
            for query in list_query:

                # Ejecucion de la querys
                if list_data == None:        
                    class_connection.cursor.execute(query)
                else:
                    class_connection.cursor.execute(query, list_data[index])
                    index += 1

            # Si todo ha ido bien, confirmar la transacción
            class_connection.cursor.execute("COMMIT")
        except mysql.connector.Error as error:
            # Si se produce un error, deshacer la transacción
            print(f"Error al insertar la base de datos: {error}")
            class_connection.cursor.execute("ROLLBACK")



    class_connection = try_open(connection_params, dictionary)

    # AQUI VA LA QUERY
    if class_connection.check_conn:
        execute_query(class_connection, list_query, list_data)
    
    class_connection = try_close(class_connection)

def single_insert_disable_keys(query, values = None, dictionary = False):

    def try_open(connection_params, dictionary):
        class cls_conn:
            connection  = None
            cursor      = None
            check_conn  = False

        try_count  = 0 # Contador de repeticiones
        while cls_conn.check_conn == False:
            try_count += 1
            
            try:
                cls_conn.connection  = mysql.connector.connect(**connection_params)
                cls_conn.cursor      = cls_conn.connection.cursor(dictionary = dictionary)
                cls_conn.check_conn  = True

            except mysql.connector.Error as sql_error:
                print(sql_error, f"... Intento N°{try_count}")
                
                cls_conn.check_conn  = False # Errores frecuentes (2003)
                

            if try_count >= 3 and cls_conn.check_conn == False: # Forzar salida al tercer intento
                break
            elif try_count < 3 and cls_conn.check_conn == False:
                sleep(3)

        return cls_conn

    def try_close(class_connection):
        if class_connection.check_conn == True:
            class_connection.cursor.close()
            class_connection.connection.close()

        return class_connection

    def execute_query(class_connection, query, values):
        try:
            # Iniciar una transacción  
            class_connection.cursor.execute("START TRANSACTION")

            # Realizar las operaciones necesarias en la base de datos
            if values == None:
                class_connection.cursor.execute(query)
            else:
                class_connection.cursor.execute(query, values)

            # Si todo ha ido bien, confirmar la transacción
            class_connection.cursor.execute("COMMIT")
        except mysql.connector.Error as error:

            # Si se produce un error, deshacer la transacción
            print("Error al crear en la base de datos: {0}".format(error))
            class_connection.cursor.execute("ROLLBACK")

    def on_off_foreign_key(class_connection, enable = 1):
        class_connection.cursor.execute(f"SET FOREIGN_KEY_CHECKS = {enable}") # DISABLE FOREIGN KEYS
        class_connection.connection.commit()
    

    class_connection = try_open(connection_params, dictionary)
    if class_connection.check_conn:
        on_off_foreign_key(class_connection, 0)
        execute_query(class_connection, query, values) # AQUI VA LA QUERY
        on_off_foreign_key(class_connection, 1)
    class_connection = try_close(class_connection)
