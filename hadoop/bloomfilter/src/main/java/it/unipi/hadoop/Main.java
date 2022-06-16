package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import it.unipi.hadoop.Job1.*;
import it.unipi.hadoop.Job2.BloomFiltersMapper;
import it.unipi.hadoop.Job2.BloomFiltersReducer;



public class Main {

  public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: bloom filter <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job1 = Job.getInstance(conf1, "tokenizer of data");
    job1.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job1, new Path(args[0]));
    job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job1.setJarByClass(Main.class);
    job1.setMapperClass(RatingMapper.class);
    //job1.setCombinerClass(CreateBloomFilterReducer.class);
    job1.setReducerClass(CreateParametersReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(IntWritable.class); // set output values for mapper
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
   
    FileOutputFormat.setOutputPath(job1,
        new Path(otherArgs[otherArgs.length - 1]));
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) {
      System.exit(0);
    }
    
  
    System.exit(0);
    

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "bloom filter creator");
    job2.setInputFormatClass(NLineInputFormat.class);
    NLineInputFormat.addInputPath(job2, new Path(args[0]));
    job2.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 600000);

    job2.setJarByClass(Main.class);
    job2.setMapperClass(BloomFiltersMapper.class);
    //job2.setCombinerClass(CreateBloomFilterReducer.class);
    job2.setReducerClass(BloomFiltersReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(BloomFilter.class); // set output values for mapper
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1] + "_2"));
    Boolean countSuccess1 = job2.waitForCompletion(true);
    if(!countSuccess1) {
      System.exit(0);
    }



    //Configuration conf3 = new Configuration();
    //Job job3 = Job.getInstance(conf3, "testing bloom filter");
    
  }
}
