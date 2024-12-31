
package py.third.rocksdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;

public interface RocksDbKvSerializer {
  public int size();

  default void serialize(byte[] bytes) throws IOException {
    Validate.isTrue(bytes.length == size());
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    serialize(buffer);
  }

  public void serialize(ByteBuffer buffer) throws IOException;

  default boolean deserialize(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return deserialize(buffer);
  }

  public boolean deserialize(ByteBuffer buffer) throws IOException;

}
