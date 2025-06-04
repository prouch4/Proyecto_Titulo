import unittest
import utils_helper as util

# Test Unitario
class TestDiferenciaEnMinutos(unittest.TestCase):

    def test_diferencia_positiva(self):
        """
        Prueba que la diferencia entre dos fechas cuando la primera es mayor que la segunda
        sea correcta.
        """
        fecha1 = "2024-10-01 12:57:19"
        fecha2 = "2024-10-01 12:52:41"
        # Diferencia de 4 minutos
        resultado = util.diferencia_en_minutos(fecha1, fecha2)
        self.assertEqual(resultado, 4)

    def test_diferencia_negativa(self):
        """
        Prueba que la función funcione correctamente si la primera fecha es menor que la segunda.
        """
        fecha1 = "2024-10-01 12:52:41"
        fecha2 = "2024-10-01 12:57:19"
        # La diferencia es la misma, 4 minutos, aunque el orden esté invertido
        resultado = util.diferencia_en_minutos(fecha1, fecha2)
        self.assertEqual(resultado, 4)

    def test_misma_fecha(self):
        """
        Prueba que si las dos fechas son iguales, la diferencia sea 0.
        """
        fecha1 = "2024-10-01 12:52:41"
        fecha2 = "2024-10-01 12:52:41"
        # La diferencia entre dos fechas iguales debe ser 0 minutos
        resultado = util.diferencia_en_minutos(fecha1, fecha2)
        self.assertEqual(resultado, 0)


if __name__ == "__main__":
    unittest.main()
