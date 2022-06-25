package it.unipi.hadoop;


import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.Job1.*;
import it.unipi.hadoop.Job2.BloomFiltersMapper;
import it.unipi.hadoop.Job2.BloomFiltersReducer;
import it.unipi.hadoop.Job3.TestCombiner2;
import it.unipi.hadoop.Job3.TestCombiner3;
import it.unipi.hadoop.Job3.TestMapper1;
import it.unipi.hadoop.Job3.TestMapper2;
import it.unipi.hadoop.Job3.TestMapper21;
import it.unipi.hadoop.Job3.TestMapper3;
import it.unipi.hadoop.Job3.TestReducer1;
import it.unipi.hadoop.Job3.TestReducer2;
import it.unipi.hadoop.Job3.TestReducer3;

public class Test {

public static HashMap<Text, BloomFilter> bloomFilter_param = new HashMap<Text, BloomFilter>();
  private static long startTime;
  private static long stopTime;
  private static int N_SPLIT = 0;

  public static void main(String[] args) throws Exception {
    
    Configuration conf1 = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: bloom filter <in> [<in>...] <out>");
      System.exit(2);
    }

    startTime= System.currentTimeMillis();
    boolean succes = true;
    String outputTempDir = args[1] + "_3";
   // for(int i = 1; i < 11; i++ ) {
    //  System.out.println("TEST su BLOOM FILTER" + i);
    succes = Job3(args, 6, outputTempDir);
    if(!succes)
        System.exit(0);
    //}

    
    stopTime = System.currentTimeMillis();
    System.out.println("TEMPO DI ESECUZIONE JOB3:" + TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime)+ "sec");
    
    System.out.println("FALSE POSITIVE RATE:" + (BloomFilterUtility.countFalsePositiveRate()/10));
     
    System.exit(0);

 
  }

  private static boolean Job3(String[] args, int i, String dir) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{

    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "bloom test");
    job3.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job3, new Path(args[0]));
    job3.getConfiguration().setInt("mapreduce.input.rating.totest", i);
    job3.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", (N_SPLIT*3));
    job3.setJarByClass(Main.class);
    job3.setMapperClass(TestMapper1.class);
    job3.setReducerClass(TestReducer1.class);

    job3.setMapOutputKeyClass(Text.class);
    job3.setMapOutputValueClass(Text.class); 
    
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job3, new Path(dir+ "."+i));
    Boolean countSuccess3 = job3.waitForCompletion(true);
    return countSuccess3;
    
    
    
  }
    
}
