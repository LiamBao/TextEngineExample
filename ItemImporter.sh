# prepare classpath for ItemImporter
FLAG_FILE="./run.flag"
LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

if [ -a $FLAG_FILE ]; then
        echo "ItemImporter is running, can't start another process..."
        exit
fi
touch $FLAG_FILE

#java -Xmx7168m -cp $CLASSPATH com.cic.textengine.repository.ItemImporter $1 $2
java -Xmx10240m -cp $CLASSPATH com.cic.textengine.repository.ItemImporter $1 $2

rm $FLAG_FILE
