# prepare classpath for ItemImporter

LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

java -cp $CLASSPATH com.cic.textengine.repository.datanode.daemon.DNDaemon $1 $2
