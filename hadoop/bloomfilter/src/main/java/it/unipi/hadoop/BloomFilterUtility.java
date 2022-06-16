package it.unipi.hadoop;

public class BloomFilterUtility {

    public static int get_size(int n, float p) {
        return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
      }
    
      public static int get_hash_count(int size, int n) {
        return (int) ((size / n) * Math.log(2));
      }
    
}
