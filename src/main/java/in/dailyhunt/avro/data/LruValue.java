package in.dailyhunt.avro.data;

import java.util.StringJoiner;

@SuppressWarnings("unused")
public class LruValue {
  private long lastAccess ;
  private long count ;

  public LruValue() {
  }

  public LruValue(long lastAccess, long count) {
    this.lastAccess = lastAccess;
    this.count = count;
  }

  public long getLastAccess() {
    return lastAccess;
  }

  public void setLastAccess(long lastAccess) {
    this.lastAccess = lastAccess;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", LruValue.class.getSimpleName() + "[", "]")
        .add("lastAccess=" + lastAccess)
        .add("count=" + count)
        .toString();
  }

  public void update() {
    this.lastAccess = System.currentTimeMillis();
    this.count++;
  }

  public static LruValue withDefault(){
    return new LruValue(System.currentTimeMillis(),1);
  }


}
