# prepare classpath for Unconsolidate

LIB="./lib"
CLASSPATH=build/classes:properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done
CLASSPATH=$CLASSPATH:build/dist/TextEngine.jar

java -Xmx1800m -cp $CLASSPATH com.cic.textengine.diagnose.ReaderDataFromIE $1 $2 $3 $4 $5
