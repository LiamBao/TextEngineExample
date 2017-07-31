LIB="../trunk/lib"
TE_LIB="../trunk/dist/te.jar"
CORE_LIB="../../CICCore/trunk/dist/ciccore.jar"
TECONF="../conf"
HADOOPCONF="../hadoop_conf"
TARGET_INDEX="/home/textd/data/IndexRepo_2007"
SRC_INDEX="/home/textd/data/IndexRepo_history/"

for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:$TE_LIB:$CORE_LIB:$TECONF:$HADOOPCONF
java -Xmx512m -cp $CLASSPATH com.cic.textengine.IndexMerger $TARGET_INDEX $1 $2 $3 $4 $5 $6 $7 $8 $9 

