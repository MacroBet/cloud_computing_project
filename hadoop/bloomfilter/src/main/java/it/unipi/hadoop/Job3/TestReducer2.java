package it.unipi.hadoop.Job3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import it.unipi.hadoop.BloomFilter;

public class TestReducer2 extends Reducer<Text, Text, Text, ArrayList<String>> {

    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
      ArrayList<String> valRate = new ArrayList<String>();
      for (Text val : values) {
          valRate.add(val.toString());
        }
          
      context.write(key, valRate); 

 
      }

    
}
