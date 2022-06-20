package it.unipi.hadoop.Job3;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.unipi.hadoop.BloomFilter;

public class TestMapper  extends Mapper<Object, Text, Text, BloomFilter> {
    private Text word = new Text();
    private ArrayList<BloomFilter> bloomFilter_param = new ArrayList<BloomFilter> ();

    public void setup(Context context) throws IOException, InterruptedException {
      
      try {
            Path pt = new Path("hdfs://hadoop-namenode:9820/user/hadoop/output_2/part-r-00000");// Location of file in HDFS
            SequenceFile.Reader reader = new SequenceFile.Reader(new Configuration(), SequenceFile.Reader.file(pt));

            boolean hasNext;
            do {

              Text key = new Text();
              BloomFilter bf = new BloomFilter();
              hasNext = reader.next(key, bf);
              bloomFilter_param.add(bf);

            } while(hasNext);

        } catch (Exception e) { e.getStackTrace(); }
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        int falsePositive;
         
        while (itr.hasMoreTokens()) {
          falsePositive = 0;
          String ratingRaw = itr.nextToken().toString();
          Integer rating = Math.round(Float.parseFloat(ratingRaw.split("\t")[1]));
          String movieId = ratingRaw.split("\t")[0];

          for (int i = 0; i < bloomFilter_param.size(); i++) {
            if(i != (rating-1) && bloomFilter_param.get(i).check(movieId))  
              
              falsePositive++;   
                       
          }
         
          context.write(new Text("sum"), new IntWritable(falsePositive/9));   //rating  bloomfilter
        }
  
      }


    
}
