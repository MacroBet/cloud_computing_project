package it.unipi.hadoop.Job3;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import it.unipi.hadoop.BloomFilter;

public class TestReducer2 extends Reducer<Text, Text, Text, DoubleWritable> {

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    Double falsePositive = 0.0;
    int n = 0;
    for (Text val : values) {
      if(val.toString().equals("1")) 
        falsePositive += Double.parseDouble(val.toString());
      n++;
    }

    context.write(key, new DoubleWritable(falsePositive/n));
  }
}
