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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

public class TestReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  private HashMap<Text, BloomFilter> bloomFilter_param = new HashMap<Text, BloomFilter>();
  private Map<String, ArrayList<String>> bloomFP = new HashMap<String, ArrayList<String>>();
 
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

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        
      int falsePositive = 0;
      int n = 0;
      for (DoubleWritable val : values) {
          if(bloomFilter_param.get(key).check(val.toString()))
            falsePositive++;
          n++;
        }
          
      context.write(key, new DoubleWritable(falsePositive/n)); 

 
      }

    
}
