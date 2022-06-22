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


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        Integer rating;
        while (itr.hasMoreTokens()) {
         
          String ratingRaw = itr.nextToken().toString();
          String movieId = ratingRaw.split("\t")[0];
          rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
          for(int i = 1; i < 11; i++)
            if(i != rating)

              context.write(new Text(String.valueOf(i)), new Text(movieId));
         

        }
        
      }
    }