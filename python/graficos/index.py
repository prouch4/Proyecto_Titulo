
import matplotlib.pyplot as plt
import sys
import time
print( "Esta frase se emite inmediatamente.")
#time.sleep(5)

plt.figure(figsize=(10,6))

#pais1
x= [2016,2017,2018,2019,2020,2021]
y = [45,44,47,48,47,48]

#pais2
x2= [2016,2017,2018,2019,2020,2021]
y2 = [40,41,42,43,45,47]

plt.plot(x,y, marker='o', linestyle = '--',color = 'g', label = 'Argentina')
plt.plot(x2,y2, marker='d', linestyle = '-',color = 'r', label = 'Chile')

plt.xlabel('Años')
plt.ylabel('Poblacion (M)')
plt.title('Años vs Población')
plt.yticks([41,45,48,51])
plt.legend(loc = 'lower right')
plt.savefig('/var/www/html/portalclientes/python/graficos/lines5.png')

#plt.show()
#time.sleep(5)
print("Esta frase se emite a los cinco segundos.")
sys.stdout.flush()
sys.exit(0)