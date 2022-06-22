package it.unipi.hadoop.Job2;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.FileSystem;
import it.unipi.hadoop.BloomFilter;
import java.io.InputStreamReader;

public class BloomFiltersMapper extends Mapper<Object, Text, Text, BloomFilter> {

    private HashMap<Integer, BloomFilter> bloomFilter_param = new HashMap<Integer, BloomFilter>();
    private Map<Integer, BloomFilter> combiner = new HashMap<Integer, BloomFilter>(); 
    public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output/part-r-00000");// Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;

            line = br.readLine();
            
            while (line != null) {
              String[] currencies = line.split("\t");
              BloomFilter bloomFilter = new BloomFilter(Integer.parseInt(currencies[1]),Integer.parseInt(currencies[2]));
              bloomFilter_param.put(Integer.parseInt(currencies[0]), bloomFilter);
              line = br.readLine();
            }

        } catch (Exception e) { e.getStackTrace(); }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    
      StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
      
      while (itr.hasMoreTokens()) {
        String ratingRaw = itr.nextToken().toString();
        Integer rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
        String movieId = ratingRaw.split("\t")[0];

        if(combiner.containsKey(rating)) { 
          combiner.get(rating).add(movieId); 
          combiner.put(rating, combiner.get(rating));       
        }
        else {
          bloomFilter_param.get(rating).add(movieId);  
          combiner.put(rating, bloomFilter_param.get(rating)); 
      
        }
      
      }

    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      Iterator<Map.Entry<Integer, BloomFilter>> temp = combiner.entrySet().iterator();

      while(temp.hasNext()) {
          Map.Entry<Integer, BloomFilter> entry = temp.next();
          String keyVal = entry.getKey()+"";
          BloomFilter bloomFilter = entry.getValue();

          context.write(new Text(keyVal), bloomFilter);
      }
  }
    
}
