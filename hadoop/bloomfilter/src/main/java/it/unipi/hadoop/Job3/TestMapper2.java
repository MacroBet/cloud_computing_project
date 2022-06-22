package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.StringTokenizer;

import javax.lang.model.util.ElementScanner6;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.BloomFilter;

public class TestMapper2  extends Mapper<Object, Text, Text,Text> {
  
  private HashMap<Text, BloomFilter> bloomFilter_param = new HashMap<Text, BloomFilter>();
  private Map<String, ArrayList<Integer>> combiner = new HashMap<String, ArrayList<Integer>>(); 

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
          
          if(bloomFilter_param.get(new Text(String.valueOf(rating))).check(id)) {
         
            if(combiner.containsKey(String.valueOf(rating))) { 

              int FPsum = combiner.get(String.valueOf(rating)).get(0) + 1;
              int NFPsum = combiner.get(String.valueOf(rating)).get(1);
              ArrayList<Integer> val = new ArrayList<Integer>();
              val.add(FPsum);
              val.add(NFPsum);
              combiner.put(String.valueOf(rating), val);      

            } else {
                                                                     
              ArrayList<Integer> val = new ArrayList<Integer>();    
              val.add(1);
              val.add(0);
              combiner.put(String.valueOf(rating), val);
          
          }

          } else {
            if(combiner.containsKey(String.valueOf(rating))) { 

              int FPsum = combiner.get(String.valueOf(rating)).get(0);
              int NFPsum = combiner.get(String.valueOf(rating)).get(1) + 1;
              ArrayList<Integer> val = new ArrayList<Integer>();
              val.add(FPsum);
              val.add(NFPsum);
              combiner.put(String.valueOf(rating), val);      

            } else {
            
              ArrayList<Integer> val = new ArrayList<Integer>();
              val.add(0);
              val.add(1);
              combiner.put(String.valueOf(rating), val);

          }
        
        }
      }
    }
  

  /* 
       context.write(new Text(String.valueOf(rating)), new Text("1"));
          else
            context.write(new Text(String.valueOf(rating)), new Text("0"));*/


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
        
      }

}
    