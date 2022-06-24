package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.BloomFilter;

public class TestMapper3 extends Mapper<Object, Text, Text,Text> {
    private HashMap<Text, BloomFilter> bloomFilter_param = new HashMap<Text, BloomFilter>();
    private Map<String, String> combiner = new HashMap<String, String>(); 

    public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_2/part-r-00000");// Location of file in HDFS
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(pt));
            boolean hasNext;
            do {
  
              Text key = new Text();
              BloomFilter bf = new BloomFilter();
              hasNext = reader.next(key, bf);
              bloomFilter_param.put(key, bf);
  
            } while(hasNext);
  
        } catch (Exception e) { e.getStackTrace(); }
    }
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
         
          String ratingRaw = itr.nextToken().toString();
          int rating = Integer.parseInt(ratingRaw.split("\t")[0]);
          String id = ratingRaw.split("\t")[1];
          //combiner.put(String.valueOf(rating), id);      

          context.write(new Text(String.valueOf(rating)), new Text(id));

          /*if( rating >1 || rating < 4)
            
            context.write(new Text("group1"), new Text(param));

          else if(rating > 3  || rating < 8)
            context.write(new Text("group2"), new Text(param));

          else if(rating > 7  || rating < 11)
            context.write(new Text("group3"), new Text(param));*/
        }
    }
    /* 
    public void cleanup(Context context) throws IOException, InterruptedException {
       
        Iterator<Map.Entry<String, ArrayList<Integer>>> temp = combiner.entrySet().iterator();
      
        while(temp.hasNext()) {
            Map.Entry<String, ArrayList<Integer>> entry = temp.next();
            String keyVal = entry.getKey();
            Double falsePositive = (double) entry.getValue().get(0);
            Double NonFalsePositive = (double) entry.getValue().get(1);
            Double FPrate = falsePositive/(falsePositive+NonFalsePositive);
      
            context.write(new Text(keyVal), new Text(String.valueOf(FPrate)));
        }
          
      }*/
}
