package it.unipi.hadoop;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import java.util.BitSet;
import org.apache.hadoop.util.hash.MurmurHash;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BloomFilter implements Writable{
  private VIntWritable size;
  private VIntWritable hashCount;
  private BytesWritable bytes;
  private BitSet bitset;

  private int get_size(int n, float p) {
    return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
  }

  private int get_hash_count(int size, int n) {
    return (int) ((size / n) * Math.log(2));
  }

  
  public BloomFilter() {
    bytes = new BytesWritable();
    size = new VIntWritable();
    hashCount = new VIntWritable();
  }

  BloomFilter(int n) {
    int size = get_size(n, (float) 0.01);
    int hashCount= get_hash_count(size, n);
    this.size = new VIntWritable(size);
    this.hashCount = new VIntWritable(hashCount);
    this.bitset = new BitSet(size);
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  public BloomFilter(BloomFilter b) {

    this.size = new VIntWritable(b.get_size());
    this.hashCount = new VIntWritable(b.get_hash_count());
    this.bitset = b.get_bitset();
    this.bytes = b.bytes;
    
  }

  // BloomFilter(int size, int hashCount, String bitSet) {
  //   this.size = size;
  //   this.hashCount = hashCount;
  //   // short a = Short.parseShort(bitSet, 2);
  //   // ByteBuffer bytes = ByteBuffer.allocate((int) Math.round(size / 8 +
  //   // 0.5)).putShort(a);
  //   // byte[] array = bytes.array();
  //   this.bitSet = fromString(bitSet);
  // }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    bytes.write(dataOutput);
    size.write(dataOutput);
    hashCount.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    bytes.readFields(dataInput);
    size.readFields(dataInput);
    hashCount.readFields(dataInput);
    this.bitset= BitSet.valueOf(bytes.getBytes());
  }


  public BloomFilter(int size, int hashCount) {
    this.size = new VIntWritable(size);
    this.hashCount = new VIntWritable(hashCount);
    this.bitset = new BitSet(size);
    this.bytes = new BytesWritable(bitset.toByteArray());
  }

  public void or(BloomFilter bloomFilter){
    this.bitset.or(bloomFilter.get_bitset());
  }

  private long getUnsignedInt(int x) {
    return x & 0x00000000ffffffffL;
  }

  public void add(String s) {
    int hashCount= this.hashCount.get();
    int size = this.size.get();
    for (int i = 0; i < hashCount; i++) {

      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash.getInstance().hash(id.getBytes(), i)) % size);
      // if (digest < 0) {
      // digest += this.size;
      // }
      this.bitset.set(digest);
    }
  }

  public boolean check(String s) {
    int hashCount= this.hashCount.get();
    BitSet bitset = BitSet.valueOf(this.bytes.getBytes());
    int size = this.size.get();
    for (int i = 0; i < hashCount; i++) {
      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash.getInstance().hash(id.getBytes(), i)) % size);
      // System.out.println("DIGEST PRESENTE: " + digest);
      if (!bitset.get(digest)) {
        // System.out.println("DIGEST ASSENTE: " + digest);
        return false;
      }
    }
    return true;
  }

  // @Override
  // public String toString() {
  //   return this.size + "\t" + this.hashCount + "\t" + toString(this.bitSet);

  // }

  // private static BitSet fromString(final String str_bitset) {
  //   BitSet bitSet = new BitSet(str_bitset.length());
  //   // System.out.println(str_bitset);

  //   for (int i = 0; i < str_bitset.toCharArray().length; i++) {
  //     bitSet.set(str_bitset.toCharArray().length - 1 - i, ((str_bitset.charAt(i)) == '1' ? true : false));
  //   }
  //   return bitSet;
  // }

  // private static String toString(BitSet bs) {
  //   String strlong = "";

  //   for (int i = 0; i < bs.toLongArray().length; i++) {
  //     int n_fill = 64 - Long.toString(bs.toLongArray()[bs.toLongArray().length - 1 - i], 2).length();
  //     String fill_bits = new String(new char[n_fill]).replace('\0', '0');
  //     strlong = strlong + fill_bits + Long.toString(bs.toLongArray()[bs.toLongArray().length - 1 - i], 2);
  //   }
  //   System.out.println("STRINGA BIT LUNGA = " + strlong.length());
  //   return strlong;
  // }

  public int get_hash_count() {return this.hashCount.get();}

  public int get_size() {return this.size.get();}

  public BitSet get_bitset() {return this.bitset;}

}