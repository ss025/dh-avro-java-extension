package in.dailyhunt.avro.data;

import java.util.HashMap;


public class LruSet extends HashMap<String, LruValue> {

  private static final int DEFAULT_INITIAL_CAPACITY = 1 << 4;
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  public LruSet() {
    super(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
  }

  public LruSet(int initialCapacity) {
    super(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  /**
   * Insert key with default value of LrValue Object. Default value of lastAccess in LrValue is System.currentTimeMillis() and for count = 1
   *
   * @param key in map/set
   * @return LrValue with default values .
   */
  public LruValue addWithDefault(String key) {
    return super.put(key, LruValue.withDefault());
  }

}
