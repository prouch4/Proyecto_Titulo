import unittest
import api.functions as api
import api.config as cf_api
import requests



class Test(unittest.TestCase):

    def test_fs_put(self):
        tiempo_de_cierre =  5
        fs_id = 143172

        resultado = api.fresh_service_put('ticket_specific', 'custom_fields', 'tiempo_de_cierre', tiempo_de_cierre, fs_id)

        close_time = resultado.get('ticket', {}).get('custom_fields', {}).get('tiempo_de_cierre', None)
        self.assertTrue(close_time == "5")

if __name__ == "__main__":
    unittest.main()
