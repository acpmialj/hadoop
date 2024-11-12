# Ficheros Parquet y Avro, HDFS y MapReduce
En este repositorio hay varios notebooks Jupyter, con diferentes propósitos. Para ejecutar cualquiera de ellos se recomienda lanzar un contenedor con Python, Hadoop (HDFS, MapReduce, YARN) y Jupyter. 

Los objetivos de aprendizade de estos notebooks son:

* Entender el formato de ficheros Parquet
* Entender el formato de ficheros Avro
* Entender el modelo de procesamiento de MapReduce
* Entender cómo trabajar con ficheros HDFS, y "verlos" desde la interfaz WebUI
* Entender cómo funciona YARN, viendo los trabajos en ejecución y los realizados en la interfaz WebUI.
* Entender la interacción de MapReduce con HDFS y YARN

## Puesta en marcha de un contenedor Hadoop Core + Jupyter
**NOTA**: es importante que la máquina (física o virtual) en la que se ejecuta Docker tenga asignada bastante RAM. Con 2 GB puede haber problemas de ejecución (tareas MapReduce enviadas a YARN que nunca se procesan). Con 8 GB no ocurre este problema. 

Lanzamos un contenedor que incluye los procesos necesarios para **Hadoop** y **Jupyter**. El contenedor usa los siguientes puertos:

* 50070 Para el NameNode de HDFS -- el uso de este puerto puede dar problemas en hosts windows. Hacer un mapeo diferente al propuesto al ejecutar el contenedor, por ejemplo "-p 10070:50070"
* 8088 Para el ResourceManager de YARN
* 8888 Para Jupyter

Para ejecutar el contenedor:

```shell
$ git clone https://github.com/acpmialj/hadoop.git # Para construir los notebooks desde cero: mkdir hadoop 
$ cd hadoop
$ docker run --rm -it -v "$PWD":/workspace --name hadoop -p 8888:8888 -p 50070:50070 -p 8088:8088 jdvelasq/hadoop:2.10.1
```

El resultado: se lanza el contenedor con los servicios de HDFS y Yarn en marcha. Se nos facilitan los enlaces de acceso al NameNode de HDFS, y al ResourceManager de Yarn. Además, en el terminal pasaremos a estar dentro de un "shell" que se ejecuta en el contenedor. 

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
Comprobamos que funcionan ambos enlaces. Recordemos que si salimos del shell (con Ctrl-D o "exit") el contenedor será destruido. 

Jupyter no está aún en marcha. Lo lanzamos desde el shell del contenedor hadoop (jupyter-notebook o jupyter-lab). Jupyter usará el puerto 8888, que está mapeado al mismo puerto del host. 

```shell
# jupyter notebook 
[I 10:01:19.592 NotebookApp] Serving notebooks from local directory: /workspace
[I 10:01:19.592 NotebookApp] Jupyter Notebook 6.4.11 is running at:
[I 10:01:19.592 NotebookApp] http://c23feec501e2:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp]  or http://127.0.0.1:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

Abrimos el enlace que empieza por "http://127.0.0.1:8888/..." y accedemos a un directorio de notebooks. Podemos crear nuevos (botón new..." de tipo Python3 o usar los ya disponibles (los descargados desde GitHub, que se encuentran en el directorio actual del host, mapeado al directorio "/workspace" del contenedor). 

## Uso de ficheros en formato Parquet
Experimenta con un fichero parquet no-HDFS: carga, escritura, esquema, compresión. El fichero de datos está obtenido de https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page, 2022, Febrero, Yellow Taxi Trip Records. Dicho fichero está almacenado en el directorio de trabajo. 

La ejecución de los comandos de este notebook requiere tener instalados los paquetes **pyarrow** y **pandas**. 

Este código se podría ejecutar directamente en el anfitrión, no necesita ninguna característica especial del contenedor (no usa ningún módulo de Hadoop). 

## Uso de ficheros en formato Avro
Experimenta con un fichero Avro no-HDFS: carga, esquema, consulta SQL. El fichero de datos está obtenido de https://github.com/tensorflow/io/raw/master/docs/tutorials/avro/train.avro. En el mismo sitio está el fichero de esquema, !curl -OL https://github.com/tensorflow/io/raw/master/docs/tutorials/avro/train.avsc. Ambos ficheros se descargan con curl y quedan almacenados en el directorio de trabajo. 

La ejecución de los comandos de este notebook requiere tener instalados los paquetes **avro**, **pandas** y **pandasql**. 

Este código se podría ejecutar directamente en el anfitrión, no necesita ninguna característica especial del contenedor (no usa ningún módulo de Hadoop). 

1. Se abre un fichero Avro y se vuelcan sus contenidos (en formato JSON). Internamente los datos son binarios.
2. Se obtiene el esquema (en formato JSON) de los datos almacenados en el fichero. Se puede comparar con el obtenido vía curl.
3. Se leen otra vez los datos del fichero, y se transforman en un dataframe de Pandas.
4. Se transforma el dataframe, eliminando la primera columna. Se realizan dos consultas SQL sobre el dataframe resultante. 

## Python_mapred
Se experimenta con funciones *mapper* y *reducer* escritas en Python, mostrando un ejemplo típico: el conteo de palabras. Este código se podría ejecutar directamente en el anfitrión, no necesita ninguna característica especial del contenedor (no usa ningún módulo de Hadoop). 

## MapReduce 
Se practica con ficheros HDFS y el marco MapReduce con Hadoop Streaming (mapper y reducer son programas externos, no funciones)

Los pasos a dar son:

* Escribir código python para "mapper" y "reducer", haciéndolos ejecutables. Comprobamos que funcionan localmente (fuera de Hadoop)
* Crear tres ficheros de texto con la entrada a procesar
* Copiar esos ficheros de texto a HDFS
* Crear un script que borre el fichero HDFS de salida (si existía) y lance la aplicación MapReduce
* Comprobar que la aplicación ha terminado, y ha creado un fichero HDFS output
* Visualizar output
* Repetir los últimos pasos usando una función "mapper", otra "combiner" y otra "reducer".

Echaremos un vistazo a las interfaces web del NameNode HDFS (http://127.0.0.1:50070/) y del ResourceManager de YARN (http://127.0.0.1:8088/) para ver el efecto del ejemplo en el sistema HDFS y en el gestor de recursos. 

## Mrjob
Se practica con MapReduce usando el paquete Python **mrjob**, que permite especificar trabajos MR (preproceso, mapper, reducer) enteramente en Python. Los pasos que se dan son muy similares a los del notebook anterior. 

