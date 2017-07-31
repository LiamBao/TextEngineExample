# prepare classpath for ItemImporter
JAVA_HOME=/usr/local/jdk/
TE_HOME=/usr/local/TextEngine
DAEMON_HOME=/usr/bin/
TE_USER=root

# for multi instances adapt those lines.
TMP_DIR=/var/tmp
PID_FILE=/var/run/jsvc_te.pid
TE_BASE=$TE_HOME
TE_OPTS=" -Xmx3096m "

LIB=$TE_HOME/lib
CLASSPATH=$TE_HOME/build/classes:$TE_HOME/properties
for f in `find $LIB -type f -name "*.jar"`
do
  CLASSPATH=$CLASSPATH:$f
done

case "$1" in
  start)
    #
    # Start Text Engine Name Node Daemon
    #
    $DAEMON_HOME/jsvc \
    -user $TE_USER \
    -home $JAVA_HOME \
    -Djava.io.tmpdir=$TMP_DIR \
    -wait 10 \
    -pidfile $PID_FILE \
    -outfile $TE_HOME/log/TE_DNDaemon.out \
    -errfile '&1' \
    $TE_OPTS \
    -cp $CLASSPATH \
    com.cic.textengine.repository.datanode.daemon.DNDaemon
    #
    # To get a verbose JVM
    #-verbose \
    # To get a debug of jsvc.
    #-debug \
    exit $?
    ;;

  stop)
    #
    # Stop Tomcat
    #
    $DAEMON_HOME/jsvc \
    -stop \
    -pidfile $PID_FILE \
    com.cic.textengine.repository.datanode.daemon.DNDaemon
    exit $?
    ;;

  *)
    echo "Usage TE_DNDaemon.sh start/stop"
    exit 1;;
esac
