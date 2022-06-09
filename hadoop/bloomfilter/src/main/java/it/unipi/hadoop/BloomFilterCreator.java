package it.unipi.hadoop;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.BitSet;
import org.apache.commons.codec.digest.MurmurHash3;

class BloomFilterCreator {
  private int size;
  private int hashCount;
  private BitSet bitSet;

  private int get_size(int n, float p) {
    return (int) (-(n * Math.log(p)) / Math.pow((Math.log(2)), 2));
  }

  private int get_hash_count(int size, int n) {
    return (int) ((size / n) * Math.log(2));
  }

  BloomFilterCreator(int n) {
    this.size = get_size(n, (float) 0.01);
    this.hashCount = get_hash_count(this.size, n);
    this.bitSet = new BitSet(this.size);
  }

  BloomFilterCreator(int size, int hashCount, String bitSet) {
    this.size = size;
    this.hashCount = hashCount;
    // short a = Short.parseShort(bitSet, 2);
    // ByteBuffer bytes = ByteBuffer.allocate((int) Math.round(size / 8 +
    // 0.5)).putShort(a);
    // byte[] array = bytes.array();
    this.bitSet = fromString(bitSet);
  }

  private long getUnsignedInt(int x) {
    return x & 0x00000000ffffffffL;
  }

  public void add(String s) {
    for (int i = 0; i < this.hashCount; i++) {

      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash3.hash32(Long.valueOf(id).longValue(), i)) % this.size);
      // if (digest < 0) {
      // digest += this.size;
      // }
      this.bitSet.set(digest);
    }
  }

  public boolean check(String s) {
    for (int i = 0; i < this.hashCount; i++) {
      String id = s.replace("t", "0");
      int digest = (int) (getUnsignedInt(MurmurHash3.hash32(Long.valueOf(id).longValue(), i)) % this.size);
      // System.out.println("DIGEST PRESENTE: " + digest);
      if (!this.bitSet.get(digest)) {
        // System.out.println("DIGEST ASSENTE: " + digest);
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    return this.size + "\t" + this.hashCount + "\t" + toString(this.bitSet);

  }

  private static BitSet fromString(final String str_bitset) {
    BitSet bitSet = new BitSet(str_bitset.length());
    // System.out.println(str_bitset);

    for (int i = 0; i < str_bitset.toCharArray().length; i++) {
      bitSet.set(str_bitset.toCharArray().length - 1 - i, ((str_bitset.charAt(i)) == '1' ? true : false));
    }
    return bitSet;
  }

  private static String toString(BitSet bs) {
    String strlong = "";

    for (int i = 0; i < bs.toLongArray().length; i++) {
      int n_fill = 64 - Long.toString(bs.toLongArray()[bs.toLongArray().length - 1 - i], 2).length();
      String fill_bits = new String(new char[n_fill]).replace('\0', '0');
      strlong = strlong + fill_bits + Long.toString(bs.toLongArray()[bs.toLongArray().length - 1 - i], 2);
    }
    System.out.println("STRINGA BIT LUNGA = " + strlong.length());
    return strlong;
  }
}