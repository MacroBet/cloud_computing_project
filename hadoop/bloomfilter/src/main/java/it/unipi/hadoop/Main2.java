package it.unipi.hadoop;


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
import it.unipi.hadoop.Job2_1.BloomFilterCreator;


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
    job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);

    job1.setJarByClass(Main.class);
    job1.setMapperClass(BloomFilterCreator.RatingMapper.class);
    // job1.setCombinerClass(Job1Combiner.class);
    job1.setReducerClass(BloomFilterCreator.CreateBloomFilterReducer.class);

    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class); // set output values for mapper
    
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(BloomFilter.class);
   
    
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1] + "_2"));
    job1.setOutputFormatClass(SequenceFileOutputFormat.class);
    Boolean countSuccess = job1.waitForCompletion(true);
    if(!countSuccess) {
      System.exit(0);
    }
   
    System.exit(0);

    
  }
}

