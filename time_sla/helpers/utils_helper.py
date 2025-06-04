from datetime import datetime
import math


def diferencia_en_minutos(closed_at: str, fecha_ofensa: str) -> int:
    """
    Calcula la diferencia en minutos entre dos fechas proporcionadas en formato de cadena.

    Args:
        closed_at (str): La primera fecha en formato 'YYYY-MM-DD HH:MM:SS'.
        fecha_ofensa (str): La segunda fecha en formato 'YYYY-MM-DD HH:MM:SS'.

    Returns:
        int: La diferencia en minutos entre las dos fechas. Siempre devuelve un valor positivo.

    Funcionalidad:
        - Convierte las cadenas de fecha a objetos datetime usando el formato especificado.
        - Calcula la diferencia en minutos entre las dos fechas.
        - Asegura que el resultado sea un valor positivo, independientemente del orden de las fechas.
    """
    # Definir el formato en que se espera que se presenten las fechas en las cadenas de texto
    formato_fecha = "%Y-%m-%d %H:%M:%S"

    # Convertir la primera cadena de fecha en un objeto datetime usando el formato especificado
    datetime1 = datetime.strptime(closed_at, formato_fecha)

    # Convertir la segunda cadena de fecha en un objeto datetime usando el mismo formato
    datetime2 = datetime.strptime(fecha_ofensa, formato_fecha)

    # Calcular la diferencia de tiempo entre las dos fechas en minutos
    # .total_seconds() obtiene la diferencia en segundos y se divide por 60 para obtener minutos
    diferencia = (datetime1 - datetime2).total_seconds() / 60

    # Aproximación
    #def round_up_half(diferencia):
    #    aprox = math.ceil(diferencia - 0.5)
    #    return aprox


    # Devolver la diferencia en valor absoluto (siempre positiva) y convertirla a entero
    return abs(int(diferencia))
    #return abs(int(aprox))


# Ejemplo de uso
#fecha1 = '2024-10-01 10:56:47'
#fecha2 = '2024-10-01 10:58:16'
#resultado = diferencia_en_minutos(fecha1, fecha2)
#print(f'La diferencia en minutos es: {resultado}')