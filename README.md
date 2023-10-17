# Ficheros Parquet, HDFS y MapReduce
En este repositorio hay varios notebooks Jupyter, con diferentes propósitos. Para ejecutar cualquiera de ellos se recomienda lanzar un contenedor con Python, Hadoop (HDFS, MapReduce, YARN) y Jupyter. 

Los objetivos de aprendizade de estos notebooks son:

* Entender el formato de ficheros Parquet
* Entender el modelo de procesamiento de MapReduce
* Entender cómo trabajar con ficheros HDFS, y "verlos" desde la interfaz WebUI
* Entender cómo funciona YARN, viendo los trabajos en ejecución y los realizados en la interfaz WebUI.
* Entender la interacción de MapReduce con HDFS y YARN

## Puesta en marcha de un contenedor Hadoop Core + Jupyter
**NOTA**: es importante que la máquina (física o virtual) en la que se ejecuta Docker tenga asignada bastante RAM. Con 2 GB puede haber problemas de ejecución (tareas MapReduce enviadas a YARN que nunca se procesan). Con 8 GB no ocurre este problema. 

Lanzamos un contenedor que incluye los procesos necesarios para **Hadoop** y **Jupyter**. El contenedor usa los siguientes puertos:

* 50070 Para el NameNode de HDFS
* 8088 Para el ResourceManager de YARN
* 8888 Para Jupyter

Para ejecutar el contenedor:

```shell
$ mkdir hadoop
$ cd hadoop
$ docker run --rm -it -v "$PWD":/workspace --name hadoop -p 8888:8888 -p 50070:50070 -p 8088:8088 jdvelasq/hadoop:2.10.1
```

El resultado:

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

Comprobamos que funcionan ambos enlaces. Además, estamos dentro de un "shell" del contenedor. Al salir del shell (con Ctrl-D o "exit") el contenedor desaparece. 

Desde el shell del contenedor, lanzamos Jupyter (jupyter-notebook o jupyter-lab). Usará el puerto 8888, que está mapeado al del host. 

```shell
# jupyter notebook 
[I 10:01:19.592 NotebookApp] Serving notebooks from local directory: /workspace
[I 10:01:19.592 NotebookApp] Jupyter Notebook 6.4.11 is running at:
[I 10:01:19.592 NotebookApp] http://c23feec501e2:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp]  or http://127.0.0.1:8888/?token=0039b4185fcdac1d9a5bb0b1c13927158c64eb7b6c1a5810
[I 10:01:19.592 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
```

Abrimos el enlace "http://127.0.0.1:8888" y accedemos a un directorio de notebooks. Creamos uno nuevo (botón new..." de tipo Python3. Ya podemos empezar a introducir comandos. 

Con este entorno en marcha, podemos trabajar con los notebooks de este repositorio. Podemos ir escribiendo cada celda paso a paso (copiar-pegar) o descargar los notebooks del repositorio y trabajar sobre ellos. Nótese que el contenedor puede acceder, vía /workspace, a los ficheros que están en la carpeta desde la cual se ha lanzado el contenedor. 

## Parquet
Experimenta con un fichero parquet no-HDFS: carga, escritura, esquema, compresión. El fichero de datos está obtenido de https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page, 2022, Febrero, Yellow Taxi Trip Records. Dicho fichero está almacenado en el directorio de trabajo. 

La ejecución de los comandos de este notebook requiere tener instalados los paquetes **pyarrow** y **pandas**. 

Este cçodigo se podría ejecutar directamente en el anfitrión, no necesita ninguna característica especial del contenedor (no usa ningún módulo de Hadoop). 

## Python_mapred
Se experimenta con funciones *mapper* y *reducer* escritas en Python, mostrando un ejemplo típico: el conteo de palabras. Este cçodigo se podría ejecutar directamente en el anfitrión, no necesita ninguna característica especial del contenedor (no usa ningún módulo de Hadoop). 

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

