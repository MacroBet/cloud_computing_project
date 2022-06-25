git pull
hadoop fs -rm -r output
hadoop fs -rm -r output_2
hadoop fs -rm -r output_3
hadoop fs -rm -r output_3.1
hadoop fs -rm -r output_3.2
mvn clean package
cd target
hadoop jar bloomfilter-1.0-SNAPSHOT.jar it.unipi.hadoop.Main title.ratings.txt output 100000 0.01 
hadoop fs -cat output/part-r-00000
hadoop fs -cat output_3.1/part-r-00000
hadoop fs -cat output_3.1/part-r-00001
hadoop fs -cat output_3.2/part-r-00000
hadoop fs -cat output_3.2/part-r-00001
