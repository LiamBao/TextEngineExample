#!/bin/sh
##****************************************
##	John.Li
##	3.0
##	2012-08-28
##****************************************

TE_log_file=/home/te_opr/TELog
TE_file=/home/te_opr/TextEngine
date_time=`date +%Y-%m-%d\ %T`


tail -300 ${TE_log_file}/TE_ItemImporter.log|grep "Error to initiate the IWM workflow">${TE_file}/error_log.ini
###tail -50 ${TE_log_file}/aaa.log|grep "Error to initiate the IWM workflow">${TE_file}/error_log.ini

if [ -s error_log.ini ];then
	echo ${date_time}"========start===error==dispose=====">>${TE_file}/TElog.log
	sh ./StopItemImporter.sh 192.168.2.11
	sleep 30
###	ps -ux|grep ItemImporter|wc -l>${TE_file}/sum.ini
###	SUM=sum.ini
###	if [ ${SUM} -gt 1 ];then
###		exit
###	else
###		if [ -f rum.flag ];then
		
			rm -rf run.flag
			sh ./ItemImporter.sh ~/TE_item_remote/ &
###		else
###			sh ./ItemImporter.sh ~/TE_item_remote/ &
###		fi
###	fi
else
	echo ${date_time}">>>>>No Error<<<<">>${TE_file}/TElog.log
	exit
fi
