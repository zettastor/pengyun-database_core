/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.third.rocksdb;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TemplateRocksDbColumnFamilyHandle<K extends RocksDbKvSerializer,
    V extends RocksDbKvSerializer> extends RocksDbColumnFamilyHandle {
  private static final Logger logger = LoggerFactory
      .getLogger(TemplateRocksDbColumnFamilyHandle.class);

  public TemplateRocksDbColumnFamilyHandle(RocksDbOptionConfiguration rocksDbOptionConfiguration) {
    super(rocksDbOptionConfiguration);
  }

  public TemplateRocksDbColumnFamilyHandle(
      RocksDbOptionConfiguration rocksDbOptionConfiguration, String dbPathInfo) {
    super(rocksDbOptionConfiguration, dbPathInfo);
  }

  public boolean put(K key, V value) throws IOException, KvStoreException {
    byte[] keyBytes = new byte[key.size()];
    byte[] valueBytes = new byte[value.size()];
    try {
      key.serialize(keyBytes);
      value.serialize(valueBytes);
    } catch (IOException ex) {
      logger.error("serialize some data failed for put into rocks db", ex);
      throw ex;
    }
    logger.debug("rocks({}) put record key {}, value {}", this.getColumnFamilyName(), keyBytes,
        valueBytes);
    return super.put(keyBytes, valueBytes);
  }

  public boolean get(K key, V value) throws IOException, KvStoreException {
    byte[] keyBytes = new byte[key.size()];
    try {
      key.serialize(keyBytes);
    } catch (IOException ex) {
      logger.error("serialize key failed for get some data form rocks db", ex);
      throw ex;
    }
    byte[] valueBytes = super.get(keyBytes);
    if (null == valueBytes) {
      return false;
    }

    try {
      value.deserialize(valueBytes);
    } catch (IOException ex) {
      logger.error("deserialize value failed for get some data form rocks db", ex);
      throw ex;
    }
    return true;
  }

  public void delete(K key) throws IOException, KvStoreException {
    byte[] keyBytes = new byte[key.size()];
    try {
      key.serialize(keyBytes);
    } catch (IOException ex) {
      logger.error("serialize key failed for delete some data form rocks db", ex);
      throw ex;
    }

    super.delete(keyBytes);
    return;
  }

  public void delete(K key, int bucketIndex) throws IOException, KvStoreException {
    throw new IOException("not support exception");
  }

  public void rangeDelete(K startKey, K endKey) throws IOException, KvStoreException {
    byte[] startKeyBytes = new byte[startKey.size()];
    byte[] endKeyBytes = new byte[endKey.size()];
    try {
      startKey.serialize(startKeyBytes);
      endKey.serialize(endKeyBytes);
    } catch (IOException ex) {
      logger.error("serialize key failed for delete some data form rocks db", ex);
      throw ex;
    }
    super.rangeDelete(startKeyBytes, endKeyBytes);
    return;
  }

  public boolean exist(K key) throws IOException, KvStoreException {
    byte[] keyBytes = new byte[key.size()];
    try {
      key.serialize(keyBytes);
    } catch (IOException ex) {
      logger.error("serialize key failed for delete some data form rocks db", ex);
      throw ex;
    }

    return super.exist(keyBytes);
  }
}
