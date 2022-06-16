package it.unipi.hadoop.Job2;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.fs.FileSystem;


import java.io.InputStreamReader;

public class BloomFiltersMapper extends Mapper<Object, Text, Text, Text> {
    private Text word = new Text();
    private HashMap<Integer, ArrayList<Integer>> bloomFilter_param = new HashMap<Integer, ArrayList<Integer>>();

    public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output/part-r-00000");// Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;

            
            line = br.readLine();
            
            //rating  m k
            while (line != null) {
              ArrayList<Integer> parameters = new ArrayList<Integer>();
              String[] currencies = line.split("\t");
              parameters.add(Integer.parseInt(currencies[1]));
              parameters.add(Integer.parseInt(currencies[2]));

              bloomFilter_param.put(Integer.parseInt(currencies[0]), parameters);


              line = br.readLine();
            }

        } catch (Exception e) { e.getStackTrace(); }
    }

    
}
