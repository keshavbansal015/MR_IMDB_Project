#!/bin/bash

#SBATCH --job-name="IMDbMapReduceJob"
#SBATCH --output="IMDbMapReduceJob.out"
#SBATCH --partition=compute
#SBATCH --nodes=3
#SBATCH --mem=64G
#SBATCH --time=48:00:00

# Load Hadoop and Java modules
module load hadoop
module load openjdk

# Hadoop configuration and environment setup
SW=/path/to/hadoop
export HADOOP_HOME=$SW/hadoop-3.2.2
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# Define the directory for your JAR file and input/output
JAR_DIR=/path/to/jar
INPUT_PATH=/path/to/input
OUTPUT_PATH=/path/to/output

# Clean the output directory in HDFS
hdfs dfs -rm -r $OUTPUT_PATH

# Run the MapReduce job
hadoop jar $JAR_DIR/IMDbMapReduce.jar IMDbJoinDriver $INPUT_PATH/title_basics $INPUT_PATH/title_actors $INPUT_PATH/title_crew $OUTPUT_PATH

echo "MapReduce job completed."
