#To delete all documents from index which match the given query 
#If the query contains white space, it shall be replaced by "_"
#e.g.
# ./te_indexdelete.sh "itemid:[970000000_TO_999999999]"

LIB="../trunk/lib"
TE_LIB="../trunk/dist/te.jar"
CORE_LIB="../../CICCore/trunk/dist/ciccore.jar"
TECONF="../conf"
HADOOPCONF="../hadoop_conf"
TARGET_INDEX="/home/textd/data/IndexRepo"
SRC_INDEX="/home/textd/data/IndexRepo_history/"

for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:$TE_LIB:$CORE_LIB:$TECONF:$HADOOPCONF
java -Xmx1024m -cp $CLASSPATH com.cic.textengine.IndexDeleter $TARGET_INDEX $1 

