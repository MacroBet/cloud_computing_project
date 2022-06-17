git pull
hadoop fs -rm -r output
hadoop fs -rm -r output_2
mvn clean package
cd target
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Main mini_ratings.txt output
hadoop fs -cat output/part-r-00000
hadoop fs -cat output_2/part-r-00000