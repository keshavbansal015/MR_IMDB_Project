#!/bin/bash

# Load Hadoop and Java modules
# module load hadoop
# module load openjdk

# Define the Hadoop installation directory
# SW=/path/to/hadoop
# export HADOOP_HOME=$SW/hadoop-3.3.5
# PATH="$HADOOP_HOME/bin:$PATH"

# Define the directory where your Java source files are located
SRC_DIR=

# Define the name of your output JAR file
JAR_FILE=IMDbMapReduce.jar

# Clean previous builds
rm -rf classes
rm -f $JAR_FILE

# Compile Java source files
mkdir -p classes
javac -d classes -cp classes:`hadoop classpath` $SRC_DIR/*.java

# Package the compiled classes into a JAR
jar cf $JAR_FILE -C classes .

# Set permissions
chmod 750 $JAR_FILE

echo "Compilation completed."
