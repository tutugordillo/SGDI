#! /bin/bash

HADOOP_DIR=/usr/local/hadoop-2.7.1 # Directorio donde está Hadoop
CLASS_NAME=ServidorWeb # Nombre de la clase principal

# Limpiar compilaciones anteriores
rm -rf *.class

# Compilar la clase que contiene el Map y Reduce (opcionalmente Combiner)
javac ${CLASS_NAME}.java -cp ${HADOOP_DIR}/share/hadoop/common/hadoop-common-2.7.1.jar:${HADOOP_DIR}/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar:${HADOOP_DIR}/share/hadoop/common/lib/commons-cli-1.2.jar:${HADOOP_DIR}/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.1.jar

# Crear un fichero JAR con todos las clases
jar cf ${CLASS_NAME}.jar *.class

# Borrar el directorio de salida para evitar errores al lanzar la tarea
rm -rf salida/

# Lanzar la tarea MapReduce 
$HADOOP_DIR/bin/hadoop jar ${CLASS_NAME}.jar ${CLASS_NAME}
