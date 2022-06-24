package it.unipi.hadoop.Job3;


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

public class TestReducer3 extends Reducer<Text, Text, Text, Text> {
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
    
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    Double falsePositive = 0.0;
    int n = 0;
    for (Text val : values) {
        if(bloomFilter_param.get(new Text(String.valueOf(key))).check(val.toString()))
            falsePositive++;
        n++;
    }

    context.write(key, new Text(String.valueOf(falsePositive/n)));
  }
    
}
