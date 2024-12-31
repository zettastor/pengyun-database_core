/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
