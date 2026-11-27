# Map Reduce Carolina Mesa Martínez

## Paso 1. Libros en hdfs

* Después de crear la carpeta e6/libros , mostramos su contenido para poder calcular el tamaño en bloques de 128 MB
    * En este caso cada archivo ocuparía un único bloque , ya que no superan ese tamaño
    * Al calcular 1 bloque por fichero , generarémos 3 tarefas de map
```
[xuedua076@cdh61-login5 ~]$  hdfs dfs -ls e6/libros
Found 3 items
-rw-r--r--   3 xuedua076 xunta     772386 2026-11-26 17:49 e6/libros/pg1342.txt
-rw-r--r--   3 xuedua076 xunta    2225777 2026-11-26 17:49 e6/libros/pg2000.txt
-rw-r--r--   3 xuedua076 xunta    1276315 2026-11-26 17:49 e6/libros/pg2701.txt
```

## Paso 2. Wordcount con hadoop

### Pregunta 1
* Los contadores a analizar son :
    * Launched map tasks : 3 , número de tareas de map reduce que se inciaron para procesar los archivos de esa carpeta
    * Launched reduce tasks : 48 , número de tareas que se ejecutaron para agrupar palabras y sumar
    * Map input records : 75303 , líneas de texto procesadas por los mapper
    * Map output records : 735917 , número total de palabras emitidas por los mappers
    * Reduce output records : 79998 , palabras generadas por recucer por cada palabra distinta

```
 Job Counters
                Launched map tasks=3
                Launched reduce tasks=48
...

 Map-Reduce Framework
                Map input records=75303
                Map output records=735917
                ...
                Reduce output records=79998

```

* Al comparar los resultados, observamos que coinciden las líneas de texto contadas pero no el número total de palabras
    * Esto último se debe a que wc -w cuenta las palabras según los separadores que reconoce y wordcount(stringtokenizer) separa cada línea en tokens.

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cat e6/libros/* | wc -l
75303
...
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cat e6/libros/* | wc -w
732920
```

### Pregunta 2

* Mostramos las 20 palabras más frecuentes , donde observamos diferentes problemas:
    * Se mezclan palabras en español y en inglés 
    * Aparecen mayúsculas y minúsculas , por lo cual la misma palabra no se cuenta de la misma manera (Por ejemplo , 'I ' e 'i')

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cat e6/palabras/part-* | sort -t$'\t' -k2 -nr | head -20
que     19546
the     18539
de      18195
a       16197
y       15976
of      10652
la      10331
and     9508
to      8878
el      8009
en      7941
no      6513
in      5891
se      4751
los     4701
that    4161
con     4119
por     3763
his     3624
I       3487
```

* Mostramos que aparecen 173 claves diferentes , esto debido a la diferencia de mayúsculas , signos de puntuación (whale-ship) , variantes de las propias palabras(whale frente a whales) 

```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cat e6/palabras/part-* | grep -i "^whale" | wc -l
173
```

### Pregunta 3
* La salida muestra que tenemos 49 en total y 48 part-r-
    * los part-r- guardan parte de las palabras con su contador mediante reduce
    * _SUCCESS : hadoop lo crea para indicar que ha terminado correctamemte
* Existen 48 archivos por las 48 tareas de reduce que obserbávamos antes (Launched reduce tasks=48)
    * Se utiliza la configuración por defecto , lo cual establece un part por reduce

```
Found 49 items
-rw-r--r--   3 xuedua076 xunta          0 2026-11-26 17:58 e6/palabras/_SUCCESS
-rw-r--r--   3 xuedua076 xunta      19039 2026-11-26 17:58 e6/palabras/part-r-00000
...
-rw-r--r--   3 xuedua076 xunta      18221 2026-11-26 17:58 e6/palabras/part-r-00047
```


### Pregunta 4

* Al volver a ejecutar el comando 'yarn jar $EX wordcount e6/libros e6/palabras' , hadoop nos devuelve un error 
    * Map reduce no permite ejecutar yarn con una carpeta de salida que ya existe, evitando que se sobrescriban los resultados
```
[xuedua076@cdh61-login5 ~]$ yarn jar $EX wordcount e6/libros e6/palabras
WARNING: YARN_OPTS has been replaced by HADOOP_OPTS. Using value of YARN_OPTS.
org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://nameservice1/user/xuedua076/e6/palabras already exists
```


## Paso 3. Compras por barra 

### Pruebas en local
* Comprobamos localmente el funcionamienro del mapperreduce
* El resultado suma 999 porque una de las líneas correponde a la cabecera
```
[xuedua076@cdh61-login2 E6_material]$ head -1000 compras.csv | python mapper_compras.py | sort | python reducer_suma.py
Escenario       293
Norte   253
Praia   348
VIP     105
```

### Pregunta 1
* Observamos que la suma total de los resultados de compras de cada barra y las compras totales generadas coinciden (90000)
```
[xuedua076@cdh61-login2 E6_material]$ python xera_compras.py --saida compras.csv
compras.csv: 90000 compras

...

yarn jar $ST -files mapper_compras.py,reducer_suma.py \
 -mapper "python mapper_compras.py" -reducer "python reducer_suma.py" \
 -input e6/compras -output e6/barras

[xuedua076@cdh61-login2 E6_material]$ hdfs dfs -cat e6/barras/part-*
VIP     9108
Norte   22646
Escenario       26896
Praia   31350
```

### Pregunta 2

* La condición indica que descartamos las líneas que no tengan 7 campos y si la fila a analizar contiene 'compra_id'
    * Si no tuviéramos esa línea de código analizaríamos las cabeceras como compras o líneas mal organizadas o con datos nulos


### Pregunta 3
* Todas las líneas llegan seguidas ya que antes de procesarlas hadoop las agrupa y ordena por clave
    * La ordenación se realiza durante la fase de shuffle and sort

### Pregunta 4
* Aunque tenemos 49 archivos part en nuestro e6/barras , nos encontramos con solo 4 archivos con datos  
    * Esto se ve ya que hadoop lanzó 48 tareas reduce ,por lo cual se generaron 48 part-r , pero solo exisren 4 claves diferentes , una por barra .
    * Se agrupan las claves y se mandan a un reduce , quedando el resto vacíos.
```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -ls e6/barras
Found 49 items
-rw-r--r--   3 xuedua076 xunta          0 2026-11-26 18:29 e6/barras/_SUCCESS
-rw-r--r--   3 xuedua076 xunta          0 2026-11-26 18:29 e6/barras/part-00000
...
-rw-r--r--   3 xuedua076 xunta          0 2026-11-26 18:29 e6/barras/part-00047

...

[xuedua076@cdh61-login5 ~]$ hdfs dfs -ls e6/barras/part-* | awk '$5 > 0' | wc -l
4
```


## Paso 4 . Modificar mapper

### Pregunta 1
* Copiamos el contenido de mapper_compras en nuestro mapper_importes
    * Se ha modificado el docstring para tener una descripción más adecuada
    * Se ha añadido una variable para guardar el importe de cada barra ( importe = campos[6])
    * Para mostrar el resultado correcto , se ha modificado para visualizar tanto la barra y el importe (print("%s\t%s" % (barra,importe)))
```
[xuedua076@cdh61-login5 ~]$ cp E6_material/mapper_compras.py E6_material/mapper_importes.py

```

```
[xuedua076@cdh61-login5 ~]$ cat E6_material/mapper_importes.py
# -*- coding: utf-8 -*-
"""E6, exemplo: mapper que emite (barra, importe) por cada compra.

Entrada (CSV):  compra_id,data_hora,barra,pulseira,produto,unidades,importe
Saída:          barra<TAB>importe

Funciona con Python 2.7 (o do clúster) e con Python 3.
"""
from __future__ import print_function
import sys

for linha in sys.stdin:
    campos = linha.strip().split(",")
    if len(campos) != 7 or campos[0] == "compra_id":     # cabeceira ou liña rara
        continue
    barra = campos[2]
    importe = campos[6]
    print("%s\t%s" % (barra,importe))

```

### Pregunta 2

* Para calcular el importe total de cada barra ejecutamso el siguiente comando , con los nuevos archivos y las rutas indicadas


```
yarn jar $ST -files E6_material/mapper_importes.py,E6_material/reducer_suma.py \
 -mapper "python mapper_importes.py" -reducer "python reducer_suma.py" \
 -input e6/compras -output e6/importes
hdfs dfs -cat e6/importes/part-*
```
 * Y mostramos el importe total de cada barra (total general : 614.361,50)
```
[xuedua076@cdh61-login5 ~]$ hdfs dfs -cat e6/importes/part-*
VIP     62176.00
Norte   155114.50
Escenario       184230.50
Praia   212840.50
```

### Pregunta 3

* El reducer sirve para los dos usos porque siempre suma los valores de cada clave, independientemente de que sean unos o importes

### Pregunta 4
* Es importante a la hora de tratar importes , ya que necesitamos evitar errores de redondeo al sumar cantidades. Por eso , reduce utiliza decimal en lugar de float.

```
[xuedua076@cdh61-login5 ~]$ python -c "print(0.1 + 0.2)"
0.3
```
* En este ejemplo observamos que al indicar cuantos decimales queremos que se muestre no conserva el valor exacto.
```
[xuedua076@cdh61-login5 ~]$ python -c "print(format(0.1 + 0.2, '.20f'))"
0.30000000000000004441
```
```
[xuedua076@cdh61-login5 ~]$ python -c "from decimal import Decimal; print(Decimal('0.1') + Decimal('0.2'))"
0.3
```