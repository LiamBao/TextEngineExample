# prepare classpath for DecodeKey

LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

java -Xmx1800m -cp $CLASSPATH com.cic.textengine.datadelivery.DataConsolidate $1 $2
