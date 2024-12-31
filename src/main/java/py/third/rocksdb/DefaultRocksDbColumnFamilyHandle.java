
package py.third.rocksdb;

import java.util.Arrays;
import org.rocksdb.RocksDB;

public class DefaultRocksDbColumnFamilyHandle extends RocksDbColumnFamilyHandle {
  public DefaultRocksDbColumnFamilyHandle(RocksDbOptionConfiguration rocksDbOptionConfiguration) {
    super(rocksDbOptionConfiguration);
  }

  public DefaultRocksDbColumnFamilyHandle(String dbPath) {
    super(dbPath);
  }

  @Override
  protected boolean needCacheRecordsNumInMemory() {
    return false;
  }

  @Override
  protected String packColumnFamilyName() {
    return Arrays.toString(RocksDB.DEFAULT_COLUMN_FAMILY);
  }
}
