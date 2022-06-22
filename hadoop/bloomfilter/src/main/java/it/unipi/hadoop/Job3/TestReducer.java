package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

public class TestReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
      double falsePositive = 0.0;
      int n = 0;
      for (Text val : values) {

          falsePositive += Double.parseDouble(val.toString());
          n++;
        }
          
      context.write(key, new DoubleWritable(falsePositive/n)); 

 
      }

    
}
