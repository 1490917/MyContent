import multiprocessing
from multiprocessing import Pool
import sys
import re
import time

#Autors
#Daniel Calvo Ramos 1494116
#Angel Garcia Calleja 1490917

#Función que se ejecuta antes de hacer MAPREDUCE que lee el fichero
#Comprobamos que la extensión sea válida
def comprobarExtension(arg):
    if not arg.endswith('.txt'):
        print("EXTENSION NO VÁLIDA")
        control=-1               #Controlador por si error
    else:
        control=1
    return control

#Función que lee una linea del fichero (cada proceso lee 1)
def leerFichero(linea):
    return linea

#Fase MAP (funciones de Splitting y Mapping juntas)
def Map(lineas):
    palabras=Splitting(lineas)
    mapa=Mapping(palabras)
    return mapa

#Función donde separamos todas las filas, convertimos todos a minúscula y eliminamos otros caracteres que no sean letras
#Al final acabamos teniendo una lista grande que contiene sublistas. Estas sublistas son cada línea del fichero
def Splitting(lineas):
    linea_min = lineas.lower()                          #Transformamos tod0 a minúsculas
    linea_final = re.sub("[^\w\s'-]", '', linea_min)
    linea_final2 = re.sub("[0-9_]", '', linea_final)
    p = linea_final2.split()
    return p

#Función donde agrupamos las palabras en una tupla con un 1.
#Al final obtenemos una lista de tuplas de cada palabra tal que (palabra,1)
def Mapping(palabras):
    mapa=list()
    for palabra in palabras:
        tupla = (palabra,1)
        mapa.append(tupla)
    return mapa

""" Pasos para que el Shuffling en procesos paralelizados funcione (lento pero funcione)
    def Preparar(mapa2,d):
        for linea in mapa2:
            linea.append(d)
        return mapa2
    
    def Shuffling(mapaPreparado):
        for tupla in mapaPreparado[0:-1]:
            if tupla[0] not in mapaPreparado[-1]:
                mapaPreparado[-1][tupla[0]]=[1]
            else:
                mapaPreparado[-1][tupla[0]]+=[1]
"""

#Función que agrupa todas las tuplas con la misma clave en un diccionario con valor tal que [1,1,1,etc]
def Shuffling2(lista,x):
    for linea in lista:
        for tupla in linea:
            if tupla[0] not in x:
                x[tupla[0]]=[1]
            else:
                x[tupla[0]]+=[1]
    return x

#Función que transforma el valor de las tuplas de palabras [1,1,1] a un número
def Reducing(tupla):
    listaAux = list(tupla)
    listaAux[1] = len(listaAux[1])
    tupla = tuple(listaAux)
    return tupla

#################### MAIN ########################
if __name__ == '__main__':
    #A lo largo del código tenemos distintos controles de tiempo para comprobar si algo va lento
    tic=time.perf_counter()
    nProcesos=multiprocessing.cpu_count()                       #Número de procesos a ejecutar en paralelo
    #nProcesos=4
    nFicheros=len(sys.argv)-1                                   #Variable que nos indica el número de ficheros pasados por parámetro -1 (le restamos el archivo TextCounter.py)
    numFichero=1
    if nFicheros < 1:
        print("NO SE HA INTRODUCIDO NINGÚN PARÁMETRO")
    else:
        for numFichero in range(nFicheros):                     #Bucle en el cual leeremos todos los ficheros
            control= comprobarExtension(sys.argv[numFichero + 1])
            if not control == -1:                               #Para ver si el fichero no da error en extension
                p = Pool(nProcesos)
                try:
                    with open(sys.argv[numFichero + 1], "r", encoding="utf8") as filename:  #Abrimos fichero
                        lineas=p.map(leerFichero,filename)
                except:
                    print("ARCHIVO NO ENCONTRADO")
                    control=-2
                if not control == -2:                               #Para ver si el fichero no da error al leer
                    filename.close()                                #Lo cerramos al acabar
                    diccionario = dict()
                    mapa=p.map(Map,lineas)                          #Fase MAP (Splitting + Mapping) paralelo
                    diccionario = Shuffling2(mapa, diccionario)     #Fase Shuffling NO paralelo :/
                    listaItems=diccionario.items()                  #Convertimos el diccionario a lista para hacer en paralelo
                    listaDeTuplas=p.map(Reducing, listaItems)       #Fase Reducing paralelo
                    p.close()
                    p.join()
                    print('\n')
                    print(sys.argv[numFichero + 1], ":")
                    for i in range(len(listaDeTuplas)):
                        print(listaDeTuplas[i][0],":", listaDeTuplas[i][1])
    toc=time.perf_counter()
    #print(f"Tiempo transcurrido completo: {toc - tic:0.5f} s")  #Control de tiempo total