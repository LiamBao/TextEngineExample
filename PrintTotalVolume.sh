# prepare classpath for ItemImporter
cd `dirname $0`
LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

java -cp $CLASSPATH com.cic.textengine.diagnose.PrintTotalVolume
