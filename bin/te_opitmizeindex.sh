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
touch optimize_$1_start
java -Xmx512m -cp $CLASSPATH com.cic.textengine.TEDaemon ../conf/config.txt optimizeindex ~/data/IndexRepo_history/$1
rm optimize_$1_start
touch optimize_$1_finish

