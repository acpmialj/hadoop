# HDFS y MapReduce
En este repositorio hay tres notebooks Jupyter, con diferentes propósitos

## Parquet
Para su ejecución directa (sin contenedores). Experimenta con un fichero parquet no-HDFS: carga, escritura, esquema, compresión. El fichero de datos está obtenido de https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page, 2022, Febrero, Yellow Taxi Trip Records. Dicho fichero está almacenado en ~/Downloads. 

La ejecución del notebook requiere tener instalado el paquete **pyarrow**, además de Jupyter. 

## Python_mapred
También para su ejecución directa. Se experimenta con funciones *mapper* y *reducer* escritas en Python. 

## MapReduce
Para su ejecución en un contenedor Docker. 

**NOTA**: es importante que la máquina (física o virtual) en la que se ejecuta Docker tenga asignada bastante RAM. Con 2 GB puede haber problemas de ejecución (tareas MapReduce enviadas a YARN que nunca se procesan). Con 8 GB no ocurre este problema. 

Lanzamos un contenedor que incluye los procesos necesarios para

1. **Yarn**
2. **HDFS**

También incluye **Jupyter Notebook**. Tiene todos los puertos necesarios abiertos

* 50070 Para el NameNode de HDFS
* 8088 Para el ResourceManager de YARN
* 8888 Para Jupyter

Ejecutar un contenedor con hadoop

```shell
$ mkdir hadoop
$ cd hadoop
$ docker run --rm -it -v "$PWD":/workspace --name hadoop -p 8888:8888 -p 50070:50070 -p 8088:8088 jdvelasq/hadoop:2.10.1
```

El resultado

```shell
---------------------< stack >---------------------
 apache/ubuntu  20.04
    jupyterlab  3.2.9
        hadoop  2.10.1
---------------------------------------------------
 Hadoop NameNode at: 
    http://127.0.0.1:50070/
 Yarn ResourceManager at: 
     http://127.0.0.1:8088/
---------------------------------------------------
```

Comprobamos que funcionan ambos enlaces. Todo correcto. Además, estamos dentro de un "shell" del contenedor. 

Podemos lanzar un notebook de Jupyter. Usará el puerto 8888, que está mapeado al del host. 

```shell
# jupyter notebook 
[I 10:01:19.592 NotebookApp] Serving notebooks from local directory: /workspace
[I 10:01:19.592 NotebookApp] Jupyter Notebook 6.4.11 is running at:
[I 10:01:19.592 NotebookApp] http://c23feec501e2:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp]  or http://127.0.0.1:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

Abrimos el enlace "http://127.0.0.1:8888" y accedemos a un directorio de notebooks. Creamos uno nuevo (botón new..." de tipo Python3. Ya podemos empezar a introducir comandos. 

También podemos copiar el fichero que está en repositorio e ir ejecutándolo. 

Los pasos que se dan son:

* Escribir código python para "mapper" y "reducer", haciéndolos ejecutables. Comprobamos que funcionan localmente (fuera de Hadoop)
* Crear tres ficheros de texto con la entrada a procesar
* Copiar esos ficheros de texto a HDFS
* Crear un script que borre el fichero HDFS de salida (si existía) y lance la aplicación MapReduce
* Comprobar que la aplicación ha terminado, y ha creado un fichero HDFS output
* Visualizar output
* Repetir los últimos pasos usando una función "mapper", otra "combiner" y otra "reducer".


