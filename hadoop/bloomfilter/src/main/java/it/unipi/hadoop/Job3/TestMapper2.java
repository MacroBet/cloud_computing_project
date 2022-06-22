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
          
          if(bloomFilter_param.get(new Text(String.valueOf(rating))).check(id))

            context.write(new Text(String.valueOf(rating)), new Text("1"));
          else
            context.write(new Text(String.valueOf(rating)), new Text("0"));
         
          }
        
        }
        
      }
    