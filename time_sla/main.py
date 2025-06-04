import connection.db.transaction_functions as db
import connection.db.config as cf_db
import connection.api.functions as api
import time
import use_cases.insert_time_sla as uc


def print_section_header(header):
    print("\n" + "="*60)
    print(f"===== {header} =====")
    print("="*60 + "\n")

start_time = time.time()
print_section_header(f"Inicio de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

# Consulta a la base de datos
connection_params = cf_db.connection_params_time_sla
cls_conn = db.__try_open__(connection_params, dictionary=bool)
uc.insert_time_sla(db, api, cls_conn)

db.__try_close__(cls_conn)

end_time = time.time()
execution_time = end_time - start_time
print_section_header(f"Fin de ejecución: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
print(f"Tiempo total de ejecución: {execution_time:.2f} segundos")




