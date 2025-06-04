import os,sys
# import conexion2 as c

def check_ejecution(): 
    cliente = sys.argv[0]
    stream = os.popen(f'ps -aux|grep autooffense.py|wc -l')
    output = stream.read()
    cantidad = int(output) - 2
    
    if cantidad <= 5:            
        print("Check_ejecution: ", cantidad, "True")
        return True
    else:
        print("Check_ejecution: ", cantidad, "False")
        return False

def check_ejecution_cliente(domain): 
    cantidad = 0
    #cliente =   sys.argv[1]
    cron    =   '/bin/bash -c'
    python  =   '/usr/bin/python3'
    script  =   '/opt/test_autoofensa/autooffenseCliente.py'    
    #ps -aux|grep "/bin/bash -c /usr/bin/python3 /opt/test_autoofensa/autooffenseCliente.py 2"    
    
    stream = os.popen(f'ps -aux|grep "{cron} {python} {script} {domain}"|wc -l')
    output = stream.read()
    cantidad = int(output) - 2
    
    if cantidad <= 2:            
        print("Check_ejecution True: ", cantidad, "True")
        return True
    else:
        print("Check_ejecution False: ", cantidad, "False")
        return False
      

# Domain_id: 1 Banco BICE (Muerto)
# Domain_id: 2 Adaptive Security
# Domain_id: 3 BDO
# Domain_id: 4 CAS
# Domain_id: 5 AAN
# Domain_id: 6 EMSA
# Domain_id: 7 UDLA
# Domain_id: 8 AIEP (Muerto) 
# Domain_id: 9 UNAB (Muerto)
# Domain_id: 10 SIN (Muerto)
# Domain_id: 11 AFP Capital
# Domain_id: 12 SBPAY
# Domain_id: 13 Evertec
# Domain_id: 14 Prisa (Muerto)
# Domain_id: 15 SCONT
# Domain_id: 16 COPEC

#[2, 3, 4, 5, 6, 7, 11, 12, 13, 15, 16]

"""
#Código inicial de Ricardo
import os,sys
cliente = sys.argv[0]
stream = os.popen(f'ps -aux|grep "revision.py SBPAY"|wc -l')
output = stream.read()
cantidad = int(output) - 2
print(cantidad)
"""