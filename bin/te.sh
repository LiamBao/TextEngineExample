LIB="../trunk/lib"
TE_LIB="../trunk/dist/te.jar"
CORE_LIB="../../CICCore/trunk/dist/ciccore.jar"
HADOOPCONF="../hadoop_conf"

for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:$TE_LIB:$CORE_LIB:$TECONF:$HADOOPCONF

CONF=$1
OPTION=$2
java -Xmx512m -cp $CLASSPATH com.cic.textengine.TEDaemon $CONF $OPTION $3 $4 $5

