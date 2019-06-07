FORUM_NAME=$1
FORUM_ARCHIVE_FILE=$FORUM_NAME.stackexchange.com.7z

mkdir -p ~/temp/rozpakowane
cd ~/temp

ls -al  ~/temp/$FORUM_ARCHIVE_FILE
if [[ $? -ne 0 ]]; then
  echo Pobieram plik $FORUM_ARCHIVE_FILE
  wget https://archive.org/download/stackexchange/$FORUM_ARCHIVE_FILE
  if [[ $? -eq 0 ]]; then
   echo Plik https://archive.org/download/stackexchange/$FORUM_ARCHIVE_FILE pobrany
  else
   echo " wget nie wyszedl"
   exit 1
  fi
else
 echo "plik $FORUM_ARCHIVE_FILE juz istnieje; nie pobieram"
fi

rm -rf ~/temp/rozpakowane/*.xml
if [[ $? -eq 0 ]]; then
 echo "poprzednie pliki (rozpakowane) usuniete"
else
 echo "usuwanie nie udalo sie"
 exit 2
fi

#7za e $FORUM_ARCHIVE_FILE -o"rozpakowane/$FORUM_NAME"
#if [[ $? -eq 0 ]]; then
# echo "rozpakowanie pliku $FORUM_ARCHIVE_FILE zakończone"
#else
# echo "rozpakowanie nie udalo sie"
# exit 3
#fi


ls ~/temp/rozpakowane/$FORUM_NAME/*.xml | while read filename; do
  
  XML_FILE=$(basename $filename)
  DIR_NAME=$(echo $XML_FILE | cut -d"." -f1)
  hdfs dfs -rm -skipTrash hdfs:/user/hive/xml/$FORUM_NAME/$DIR_NAME/$XML_FILE
  hdfs dfs -mkdir hdfs:/user/hive/xml/$FORUM_NAME/ 
  hdfs dfs -mkdir hdfs:/user/hive/xml/$FORUM_NAME/$DIR_NAME/
  hdfs dfs -put ~/temp/rozpakowane/$FORUM_NAME/$XML_FILE hdfs:/user/hive/xml/$FORUM_NAME/$DIR_NAME/$XML_FILE 

done 
exit 0

for FILE in ~/temp/rozpakowane/$FORUM_NAME/*.xml; do
 echo kopiuje $XML_FILE
 XML_FILE=$(basename $FILE)
 DIR_NAME=$(echo $XML_FILE | cut -d"." -f1) 
 echo DIRNAME=$DIR_NAME
 echo XML_FILE=$XML_FILE
 hdfs dfs -rm -skipTrash hdfs:/user/hive/xml/$DIR_NAME/$XML_FILE
 hdfs dfs -put ~/rozpakowane/$FORUM_NAME/$XML_FILE hdfs:/user/hive/xml/$DIR_NAME/$XML_FILE
done 

# hdfs dfs -put rozpakowane/android.stackexchange.com.7z/Users.xml hdfs:/user/hive/xml/Users/Users.xml
#echo "jestem w"
#pwd

#hive -f load_all_files.hql
#if [[ $? -eq 0 ]]; then
## echo " wczytanie do hive zakonczone sukcesem"
#else
# echo " wczytanie do hive nie udalo sie"
# exit 4
#fi

