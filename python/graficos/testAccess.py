import requests
from dotenv import dotenv_values


def accessQradar(): 
    env_vars = dotenv_values("/opt/python_process/.env")
    url_base = env_vars["QRADAR_DOMAIN"]

    #CODIGO GENERAL
    url_ofensas = "{0}/siem/offenses?filter=status%3DOPEN&sort=-last_updated_time".format(url_base)
    payload = {}
    headers = {
        'Authorization': env_vars["QRADAR_TOKEN"]
    }
    response_ofensas = requests.request("GET", url_ofensas, headers=headers, data=payload, verify=False)
    print(response_ofensas)
    response_ofensas_json = response_ofensas.json()
    #print(response_ofensas_json)
    return True

accessQradar()