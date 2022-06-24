package it.unipi.hadoop.Job3;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import it.unipi.hadoop.BloomFilter;

public class TestMapper3 extends Mapper<Object, Text, Text,Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
         
          String ratingRaw = itr.nextToken().toString();
          int rating = Integer.parseInt(ratingRaw.split("\t")[0]);
          String id = ratingRaw.split("\t")[1];
          String param = String.valueOf(rating) + id;
          context.write(new Text(String.valueOf(rating)), new Text(id));

          /*if( rating >1 || rating < 4)
            
            context.write(new Text("group1"), new Text(param));

          else if(rating > 3  || rating < 8)
            context.write(new Text("group2"), new Text(param));

          else if(rating > 7  || rating < 11)
            context.write(new Text("group3"), new Text(param));*/
        }
    }
}
