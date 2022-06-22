package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

import it.unipi.hadoop.BloomFilter;

public class TestReducer1 extends Reducer<Text, Text, Text, ArrayWritable> {

  public void reduce(Text key, List<Text> values, Context context) throws IOException, InterruptedException {

    String[] valRate = new String[Iterables.size(values)];
    for (int i = 0; i < Iterables.size(values); i++) {
      valRate[i] = values.get(i).toString();
    }

    context.write(key, new ArrayWritable(valRate));

  }
}
