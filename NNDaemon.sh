# prepare classpath for ItemImporter

LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

java -cp $CLASSPATH com.cic.textengine.repository.namenode.daemon.NNDaemon $1 $2
