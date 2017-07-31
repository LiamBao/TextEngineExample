LIB="../trunk/lib"
TE_LIB="../trunk/dist/te.jar"
CORE_LIB="../../CICCore/trunk/dist/ciccore.jar"
TECONF="../conf"
HADOOPCONF="../hadoop_conf"

for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:$TE_LIB:$CORE_LIB:$TECONF:$HADOOPCONF

touch $1_copy_to_dfs_start
java -Xmx512m -cp $CLASSPATH com.cic.textengine.partitionprocessor.PartitionImporter "$TECONF/config.txt" source "/home/textd/data/TextRepo_history/$1" dest TextRepo > "../log/dfsimport_$1.log" 2> "../log/dfsimport_$1.err"
touch $1_copy_to_dfs_finish
