TEXTREPO_BASE=/home/textd/data/TextRepo_history
INDEXREPO_BASE=/home/textd/data/IndexRepo_history
ITEM_BASE=/home/textd/data/itemxml
LOG_BASE=/home/textd/develop/TextEngine/log
CONF=/home/textd/develop/TextEngine/conf/config.txt


OP_WRITE=write
OP_MERGE=mergeblock
OP_INDEX=index
OP_OPTIMIZEINDEX=optimizeindex

if [ $# -ne 2 ]
then
  echo "Usage -$0 MONTH INIT_ID"
  exit 1
fi

month=$1
initid=$2

item_path=$ITEM_BASE/$month
textRepo=$TEXTREPO_BASE/$month
indexRepo=$INDEXREPO_BASE/$month

touch ftp_"$month"_start
./getitem_ftp.sh $month >/dev/null
rm ftp_"$month"_start

touch write_"$month"_start
./te.sh $CONF $OP_WRITE $item_path $textRepo $initid > $LOG_BASE/"$OP_WRITE"_$month.log
rm write_"$month"_start 

touch merge_"$month"_start
./te.sh $CONF $OP_MERGE $textRepo > $LOG_BASE/"$OP_MERGE"_$month.log
rm merge_"$month"_start

touch index_"$month"_start
./te.sh $CONF $OP_INDEX $textRepo $indexRepo >$LOG_BASE/"$OP_INDEX"_$month.log 
rm index_"$month"_start

touch dfscopy_"$month"_start
./textrepoimport.sh $month
rm dfscopy_"$month"_start
touch dfscopy_"$month"_finish
