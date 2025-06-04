#####################################
# DESCRIPCIÓN DEL SCRIPT            #
#####################################

# La función obtener_body_text busca en la respuesta de una consulta a la API de app, obteniendo el campo body_text del ticket especificado.
# Luego, extrae la información importante de body_text, elimina los corchetes y la frase "IP PARA PORTAL".
# Si el body_text no viene en el formato [IP PARA PORTAL] [información], retorna None.

#####################################
# IMPORTS NECESARIOS                #
#####################################

import re

#####################################
# SCRIPT                            #
#####################################

# Extracción de info de body_text, eliminación de corchetes y "IP PARA PORTAL"
def obtener_body_text(ticket, domain, response):

    body_text = None

    # Verificar si 'ticket' está en la respuesta
    if 'ticket' in response:
        ticket_info = response['ticket']

        # Verificar si 'conversations' está en 'ticket'
        if 'conversations' in ticket_info:
            conversations = ticket_info['conversations']

            # Recorrer las conversaciones y buscar el último 'body_text'
            for conversation in conversations:
                if 'body_text' in conversation:
                    body_text = conversation['body_text']
                    
                    # Comprobar si el body_text está en el formato esperado
                    if body_text.startswith('[IP PARA PORTAL]'):
                        # Eliminar todos los corchetes y la frase "IP PARA PORTAL" del body_text
                        body_text = re.sub(r'[\[\]]', '', body_text)
                        body_text = body_text.replace('IP PARA PORTAL', '').strip()
        
                        # Una vez encontrado el primer body_text válido, romper el ciclo
                        break
                    else:
                        body_text = None

    # Si no se encontró body_text relevante o el formato no es el esperado
    if body_text is None:
        return None

    return body_text if body_text else None