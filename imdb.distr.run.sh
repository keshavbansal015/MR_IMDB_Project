#!/bin/bash
#SBATCH -A uot191
#SBATCH --job-name="MR_IMDB_Project"
#SBATCH --output="IMDbJoinDriver.distr.out"
#SBATCH --partition=compute
## allocate 3 nodes for the Hadoop cluster: 3 datanodes, from which 1 is namenode
#SBATCH --nodes=3
#SBATCH --ntasks-per-node=1
#SBATCH --mem=16G
#SBATCH --export=ALL
#SBATCH --time=10:00:00

rm ~/.ssh/known_hosts
export HADOOP_CONF_DIR=/home/$USER/expanse

module reset
module unload cpu/0.17.3b 
module load cpu/0.15.4
module load hadoop
module load openjdk

SW=/expanse/lustre/projects/uot182/fegaras
export HADOOP_HOME=$SW/hadoop-3.2.2
export MYHADOOP_HOME=$SW/myhadoop
PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$MYHADOOP_HOME/bin:$PATH"

myhadoop-configure.sh -s /scratch/$USER/job_$SLURM_JOBID
cp $HADOOP_CONF_DIR/slaves $HADOOP_CONF_DIR/workers

start-all.sh

hdfs dfs -rm -r /user/$USER/*
hdfs dfs -mkdir -p /user/$USER
hdfs dfs -mkdir -p /user/$USER/input
echo "Directory created"

hdfs dfs -put ~/MR_IMDB_Project/input/* /user/$USER/input/
# hdfs dfs -mkdir -p /user/$USER/reducer1
##echo "Transferred to directory"

hadoop jar IMDbJoinDriver.jar IMDbJoinDriver /user/$USER/input/title.basics.tsv /user/$USER/input/imdb00-title-actors.csv /user/$USER/input/title.crew.tsv /user/$USER/output
rm -rf output-distr
mkdir output-distr
hdfs dfs -get /user/$USER/output/* output-distr/
# hdfs dfs -get /user/$USER/mapper1/* output-distr/

stop-all.sh
myhadoop-cleanup.sh
