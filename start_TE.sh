#!/bin/sh
#****************************************
        John.Li
        1.0
        2012-06-06
#****************************************

POST=/home/te_opr/TextEngine/data/post
SERA=/home/te_opr/TextEngine/data/sera
GML=/home/te_opr/TextEngine
###TEST=/home/te_opr/TextEngine/data/test
###CANSHU=`expr 4 * 60 * 60 * 1000`
echo `date`>>/home/te_opr/TextEngine/TElog.log
MQ_TIME=`java -jar /home/te_opr/TextEngine/getDate.jar`
cd ${POST}
ls -rS --sort=time TEItem_FF*>/home/te_opr/TextEngine/wjlb.ini
if [ -f /home/te_opr/TextEngine/wjlb.ini ];then
        head 1 /home/te_opr/TextEngine/wjlb.ini|while read a1
        do
                echo ${a9}
		FILE=`java -jar /home/te_opr/TextEngine/getDate.jar ${a9}`
		
		if [ `expr $MQ_TIME - $FILE` -gt `expr 4 \* 60 \* 60 \* 1000` ];then
			echo "==============post start==================">>/home/te_opr/TextEngine/TElog.log
			sh ./StopItemImporter.sh 192.168.2.11
 			sleep 60
			nohup sh ./ItemImporter.sh ~/TE_item_remote/ &
			rm -f /home/te_opr/TextEngine/wjlb.ini
			exit
		else
			echo "Less Than Four hours"
		fi 
		
        done
else
        echo "No TE*xml documents"
fi


cd ${SERA}
ls -rS --sort=time TEItem_FF*>/home/te_opr/TextEngine/wjlb.ini
if [ -f /home/te_opr/TextEngine/wjlb.ini ];then
        head 1 /home/te_opr/TextEngine/wjlb.ini|while read a1
        do
                echo ${a9}
                FILE=`java -jar /home/te_opr/TextEngine/getDate.jar ${a9}`

                if [ `expr $MQ_TIME - $FILE` -gt `expr 4 \* 60 \* 60 \* 1000` ];then
			echo "==============sera start==================">>/home/te_opr/TextEngine/TElog.log
                        sh ./StopItemImporter.sh 192.168.2.11
                        sleep 60
                        nohup sh ./ItemImporter.sh ~/TE_item_remote/ &
                else
                        echo "Less Than Four hours"
                fi

        done
else
        echo "No TE*xml documents"
fi
rm -f /home/te_opr/TextEngine/wjlb.ini
