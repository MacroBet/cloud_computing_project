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

public class TestMapper  extends Mapper<Object, Text, BloomFilter, Text> {

    private HashMap<String, BloomFilter> bloomFilter_param = new HashMap<String, BloomFilter>();
    //private Map<String, Integer> bloomFP = new HashMap<String, Integer>();
    private String res = new String();

    public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_2/part-r-00000");// Location of file in HDFS
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), Reader.file(pt));
            boolean hasNext;
            do {

              Text key = new Text();
              BloomFilter bf = new BloomFilter();
              hasNext = reader.next(key, bf);
              bloomFilter_param.put(key.toString(), bf);

            } while(hasNext);

        } catch (Exception e) { e.getStackTrace(); }
    }
 

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        Integer rating;
        while (itr.hasMoreTokens()) {
         
          String ratingRaw = itr.nextToken().toString();
          String movieId = ratingRaw.split("\t")[0];
          rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
          res = (Integer.toString(rating)) + "\t" + movieId;
          context.write(bloomFilter_param.get(rating.toString()), new Text(res));
          /* 
          
          for (Map.Entry<Text, BloomFilter> entry : bloomFilter_param.entrySet()) {
            if((entry.getKey() == new Text(rating.toString())) && entry.getValue().check(movieId))

              if(bloomFP.containsKey(rating.toString()))
             
                bloomFP.put(rating.toString(), bloomFP.get(rating.toString())+1);      
           
              else 

                bloomFP.put(rating.toString(), 1);
            
          }*/
        
        }
        
      }
      /* 
      public void cleanup(Context context) throws IOException, InterruptedException {
        Iterator<Map.Entry<String, Integer>> temp = bloomFP.entrySet().iterator();
  
        while(temp.hasNext()) {
            Map.Entry<String, Integer> entry = temp.next();
            String keyVal = entry.getKey();
            Integer falsePositive = entry.getValue();
          
            
        }
       
    }*/


    
}
