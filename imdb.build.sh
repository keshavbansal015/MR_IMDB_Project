#!/bin/bash

#SBATCH -A uot191
#SBATCH --job-name="MR_IMDB_Project"
#SBATCH --output="imdb.distr.out"
#SBATCH --partition=compute
## allocate 3 nodes for the Hadoop cluster: 3 datanodes, from which 1 is namenode
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=5G
#SBATCH --export=ALL
#SBATCH --time=10

module reset
module unload cpu/0.17.3b 
module load cpu/0.15.4
module load hadoop
module load openjdk

SW=/expanse/lustre/projects/uot191/fegaras 
export HADOOP_HOME-$SW/hadoop-3.2.2
PATH-"$HADOOP_HOME/bin:$PATH"

rm -rf imdb.jar
rm -rf classes
mkdir -p classes


javac -d classes -cp classes:'hadoop classpath' src/*.java
jar cf imdb.jar -C classes .

chmod -R 750 classes
chmod -R 750 imdb.jar
echo "Compilation completed."