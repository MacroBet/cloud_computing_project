package it.unipi.hadoop;

import org.apache.hadoop.fs.FileSystem;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class BloomFilterUtility {

  public static int get_size(int n, Double p) {
    return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
  }
    
     
  public static int get_hash_count(int size, int n) {
    return (int) ((size / n) * Math.log(2));
  }
  
  public static Double countFalsePositiveRate() {
    
    Double fp = 0.0;
    try {
      
      Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_3/part-r-00000");// Location of file in HDFS
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line;
    
      line = br.readLine();
      
      while (line != null) {
        String[] currencies = line.split("\t");
        fp += Double.parseDouble(currencies[1]);
        line = br.readLine();
      }
   

    } catch (Exception e) { e.printStackTrace(); }

    return fp;

  }
}
