# import mysql.connector

# # Conectarse a la base de datos
# conn = mysql.connector.connect(
#     host="localhost",  user="usuario",
#     password="contraseña",  database="basededatos"
# )

# # Crear un cursor para realizar operaciones en la base de datos
# cursor = conn.cursor()
# try:
#     # Iniciar una transacción  
#     cursor.execute("START TRANSACTION")
#     # Realizar las operaciones necesarias en la base de datos
#     cursor.execute("UPDATE tabla SET columna='valor' WHERE id=1")
#     cursor.execute("DELETE FROM tabla WHERE id=2")
#     # Si todo ha ido bien, confirmar la transacción
#     cursor.execute("COMMIT")
# except mysql.connector.Error as error:
#     # Si se produce un error, deshacer la transacción
#     print("Error al actualizar la base de datos: {}".format(error))
#     cursor.execute("ROLLBACK")

# # Cerrar la conexión a la base de datos
# cursor.close()
# conn.close()

# # ===============================
# # otro metodo de insert en base de datos
# # ===============================
# import pymysql

# try:   
#     # Establecer conexión a la base de datos MySQL
#     connection = pymysql.connect(
#         host='localhost',
#         user='usuario',
#         password='contraseña',
#         db='basededatos',
#         charset='utf8mb4',
#         cursorclass=pymysql.cursors.DictCursor
#     )
    
#     # Realizar operaciones con la base de datos
#     with connection.cursor() as cursor:
#         # Insertar un nuevo registro
#         sql = "INSERT INTO tabla (`campo1`, campo2, `campo3`) VALUES (%s, %s, %s)"
#         cursor.execute(sql, ('valor1', 'valor2', 'valor3'))
#         # Confirmar cambios en la base de datos
#     connection.commit()
# except pymysql.err.OperationalError as e:
#     # Manejar excepción en caso de caída de la conexión
#     print(f"Ocurrió un error de conexión: {e}")
# finally:
#     # Cerrar conexión a la base de datos
#     connection.close()