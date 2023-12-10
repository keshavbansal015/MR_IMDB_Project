#!/bin/bash
#SBATCH -A uot191
#SBATCH --job-name="MR_IMDB_Project"
#SBATCH --output="IMDbJoinDriver.build.out"
#SBATCH --partition=shared
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=16G
#SBATCH --export=ALL
#SBATCH --time=10:00:00

module reset
module unload cpu/0.17.3b 
module load cpu/0.15.4
module load hadoop
module load openjdk

SW=/expanse/lustre/projects/uot182/fegaras 
export HADOOP_HOME=$SW/hadoop-3.2.2
PATH="$HADOOP_HOME/bin:$PATH"

rm -rf IMDbJoinDriver.jar
rm -rf classes
mkdir -p classes


javac -d classes -cp classes:`hadoop classpath` src/IMDbJoinDriver.java
jar cf IMDbJoinDriver.jar -C classes .

chmod -R 750 classes
chmod -R 750 IMDbJoinDriver.jar
echo "Compilation completed."