from requests import request, exceptions
from bs4      import BeautifulSoup

def __try_custom_request__(url):
    
    output_var = {
        "response_detail" : None,
        "with_https"      : None,
    }

    try:
        response = request(method = "GET", url = url, timeout = 0.5)
        response.raise_for_status()
        
        if response.status_code == 200 or response.status_code == 201:
            # print(response.headers)
            if "IIS" in response.text:
                placeholder_text                = response.text[response.text.find("<title>"):response.text.find("</title>")].replace("<title>", "")
                output_var["response_detail"]   = f"Muestra página {placeholder_text} (Internet Information Services)"
            
            else:
                output_var["response_detail"]   = "Página desplegada"
            
            if str(url).replace("http:", "https:") in str(response.url):
                output_var["with_https"]        = True
            
    except exceptions.HTTPError as http_error:
        # print("Error HTTP")
        output_var["response_detail"] = str(http_error)
    
    except exceptions.Timeout as timeout_error:
        # print("Timeout Error")
        output_var["response_detail"] = "ERR_CONNECTION_TIMED_OUT"

    except exceptions.ConnectionError as conn_error:
        # print("Error Conexion")
        output_var["response_detail"] = str(conn_error)

    except exceptions.RequestException as requests_error:
        # print("Request Error")
        output_var["response_detail"] = str(requests_error)

    if "HTTPS" in str(output_var["response_detail"]) or "https" in str(output_var["response_detail"]):
        output_var["with_https"]      = True
    elif output_var["with_https"] == None:
        output_var["with_https"]      = False

    return output_var

def __try_request__(url):
    
    output_var = {
        "response_detail" : None,
        "with_https"      : None,
    }

    try:
        response = request(method = "GET", url = url, timeout = 3)
        response.raise_for_status()
        
        if response.status_code == 200 or response.status_code == 201:
            print(response.url)
            print(response.status_code)
            print(response.history)
            print(response.headers)
            print(response.text)
            
    except exceptions.HTTPError as http_error:
        # print("Error HTTP")
        output_var["response_detail"] = str(http_error)
    
    except exceptions.Timeout as timeout_error:
        # print("Timeout Error")
        output_var["response_detail"] = "ERR_CONNECTION_TIMED_OUT"

    except exceptions.ConnectionError as conn_error:
        # print("Error Conexion")
        output_var["response_detail"] = str(conn_error)

    except exceptions.RequestException as requests_error:
        # print("Request Error")
        output_var["response_detail"] = str(requests_error)

    if "HTTPS" in str(output_var["response_detail"]) or "https" in str(output_var["response_detail"]):
        output_var["with_https"]      = True
    elif output_var["with_https"] == None:
        output_var["with_https"]      = False

    print(output_var)
    return output_var

def get_text(file_path):

    output_var = None

    file_text  = open(file_path)
    output_var = file_text.read()
    file_text.close()

    return output_var

def login_detector(html_response, html_parser = None):
    
    def login_in_class_from_body(html_response):
        
        with_login = None

        try:
            body_html = html_response.body
            body_html = body_html.attrs
            body_html = body_html['class']

            comun_class_name = ['login', 'login-page']

            with_login = False
            for class_body in body_html:
                for class_name in comun_class_name:
                    if str(class_name).lower() == str(class_body).lower():
                        with_login = True
                        break

                if with_login == True:
                    break
        except:
            print("¿Body_tag no encontrado?")
        
        return with_login

    def login_in_form(html_response):
        
        print(html_response.find())
        pass

    print("Login en body:",login_in_class_from_body(html_response))
    login_in_form(html_response)

    pass

sample_1 =  "giteit.udp.cl"
sample_2 =  "conecta.udp.cl"


# html_text = get_text("/opt/python_process/Samples/test_login_sample.html")
html_text = get_text("/opt/python_process/Samples/test_login_sample2.html")
html_text = BeautifulSoup(html_text, "html.parser")

html_response = html_text
html_form = html_response.find_all('form')

key_terms = ['oauth-login'] # Usar con ID / class

for single_form in html_form:
    for content_form in single_form.children:
        if content_form.name != None:
            attrs_form = content_form.attrs
            print(attrs_form)
            # if 'id' in list(attrs_form):
            #     for term in key_terms:
            #         if attrs_form['id'] == term:
# login_detector(html_text)