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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RestoreOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.third.rocksdb.KvStoreException.KvStatus;

public class RocksDbBaseHelper {
  private static final Logger logger = LoggerFactory.getLogger(RocksDbBaseHelper.class);

  private static final Map<String, RocksDbBase> rocksDBBaseMap = new ConcurrentHashMap<>();

  public static boolean openDatabaseAndCreateIfMissing(DBOptions dbOptions, String path)
      throws KvStoreException {
    try {
      dbOptions.setCreateIfMissing(true);
      open(dbOptions, path);
      return true;
    } catch (KvStoreException ex) {
      logger.error("open or create rocks db instance failed, path is {}", path, ex);
      throw ex;
    }
  }

  public static boolean createDatabaseWithNotExist(String path,
      List<ColumnFamilyDescriptor> descriptors,
      List<ColumnFamilyHandle> handleList) {
    synchronized (rocksDBBaseMap) {
      if (null != rocksDBBaseMap.get(path)) {
        return false;
      }

      try {
        RocksDbBase dbBase = new RocksDbBase();
        DBOptions options = new DBOptions().setErrorIfExists(true)
            .setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        dbBase.create(options, path, descriptors, handleList);
        rocksDBBaseMap.put(path, dbBase);
      } catch (RocksDBException ex) {
        logger.error("open or create rocks db instance failed, it may already exist. path is {}",
            path, ex);
        return false;
      }
    }
    return true;
  }

  public static boolean destroy(String path) {
    try {
      synchronized (rocksDBBaseMap) {
        RocksDbBase db = rocksDBBaseMap.remove(path);
        if (null != db) {
          db.close();
        }
      }
      RocksDB.destroyDB(path, new Options().setForceConsistencyChecks(true));
      logger.info("rocks db has been destroy, path is {}", path);
    } catch (RocksDBException ex) {
      logger.error("destroy rocks db {} failed", path, ex);
      return false;
    }
    return true;
  }

  public static RocksDbBase open(DBOptions options, String path) throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      try {
        if (rocksDBBaseMap.containsKey(path)) {
          dbBase = rocksDBBaseMap.get(path);
          dbBase.innerOpen(options, path, null);
          return dbBase;
        }

        dbBase = new RocksDbBase();
        if (null != dbBase.innerOpen(options, path, null)) {
          rocksDBBaseMap.put(path, dbBase);
        }
      } catch (RocksDBException ex) {
        logger.warn("open rocks db failed", ex);
        throw KvRocksDbExceptionFactory.build(ex);
      }
    }
    return dbBase;
  }

  public static RocksDbBase open(DBOptions options, String path,
      ColumnFamilyOptions columnFamilyOptions)
      throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      try {
        if (rocksDBBaseMap.containsKey(path)) {
          dbBase = rocksDBBaseMap.get(path);
          dbBase.innerOpen(options, path, columnFamilyOptions);
          return dbBase;
        }

        dbBase = new RocksDbBase();
        if (null != dbBase.innerOpen(options, path, columnFamilyOptions)) {
          rocksDBBaseMap.put(path, dbBase);
        }
      } catch (RocksDBException ex) {
        logger.warn("open rocks db failed", ex);
        throw KvRocksDbExceptionFactory.build(ex);
      }
    }
    return dbBase;
  }

  public static RocksDbBase open(DBOptions options, String path,
      List<ColumnFamilyDescriptor> columnFamilyDescriptors,
      List<ColumnFamilyHandle> columnFamilyHandleList)
      throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      try {
        if (rocksDBBaseMap.containsKey(path)) {
          dbBase = rocksDBBaseMap.get(path);
          dbBase.innerOpen(options, path, columnFamilyDescriptors, columnFamilyHandleList);
          return dbBase;
        }

        dbBase = new RocksDbBase();
        if (null != dbBase
            .innerOpen(options, path, columnFamilyDescriptors, columnFamilyHandleList)) {
          rocksDBBaseMap.put(path, dbBase);
        } else {
          logger.error("rocks db open failed, but not get any exception");
        }
      } catch (RocksDBException ex) {
        logger.warn("open rocks db failed", ex);
        throw KvRocksDbExceptionFactory.build(ex);
      }
    }

    return dbBase;
  }

  public static List<byte[]> listColumnFamilies(String path) throws KvStoreException {
    DBOptions ops = new DBOptions().setCreateIfMissing(false);
    return listColumnFamilies(ops, path);
  }

  public static List<byte[]> listColumnFamilies(DBOptions ops, String path)
      throws KvStoreException {
    List<byte[]> columnList = new ArrayList<>();
    try {
      RocksDbBase dbBase = open(ops, path);
      if (null == dbBase) {
        return columnList;
      }

      synchronized (dbBase) {
        for (ColumnFamilyDescriptor descriptor : dbBase.getColumnFamilyDescriptors()) {
          columnList.add(descriptor.columnFamilyName());
        }
      }
    } catch (KvStoreException ex) {
      if (ex.getStatus() != KvStatus.NotFound) {
        throw ex;
      }
    }
    return columnList;
  }

  public static boolean hasColumnFamily(String path, String column) throws KvStoreException {
    DBOptions ops = new DBOptions().setCreateIfMissing(false);
    List<byte[]> list = listColumnFamilies(ops, path);

    for (byte[] itr :
        list) {
      if (Arrays.equals(column.getBytes(), itr)) {
        return true;
      }
    }
    return false;
  }

  public static void dropColumnFamily(byte[] family, String path) throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      dbBase = rocksDBBaseMap.get(path);
      if (null == dbBase) {
        throw KvRocksDbExceptionFactory
            .build(KvStatus.HasNotOpen, "the rocks db instance has not opened");
      }
    }

    synchronized (dbBase) {
      try {
        for (int i = 0; i < dbBase.getColumnFamilyDescriptors().size(); i++) {
          if (Arrays
              .equals(dbBase.getColumnFamilyDescriptors().get(i).columnFamilyName(), family)) {
            dbBase.dropColumnFamily(dbBase.getColumnFamilyHandles().get(i));
            return;
          }
        }
      } catch (RocksDBException ex) {
        logger.error("list rocks db {} column families failed", path, ex);
        throw KvRocksDbExceptionFactory.build(ex);
      }

    }

    throw new KvStoreException(KvStatus.NotFound, "the column family has not found");
  }

  public static void close(String path) throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      dbBase = rocksDBBaseMap.get(path);
      if (null == dbBase) {
        throw KvRocksDbExceptionFactory
            .build(KvStatus.HasNotOpen, "the rocks db instance has not opened");
      }
    }

    dbBase.close();
  }

  public static boolean backup(String srcPath, String dstPath) throws KvStoreException {
    RocksDbBase dbBase;
    synchronized (rocksDBBaseMap) {
      dbBase = rocksDBBaseMap.get(srcPath);
      if (null == dbBase) {
        throw KvRocksDbExceptionFactory
            .build(KvStatus.HasNotOpen, "the rocks db instance has not opened");
      }
    }

    try {
      dbBase.backup(dstPath);
    } catch (RocksDBException ex) {
      logger.error("backup rocks db {} to {} failed", srcPath, dstPath, ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
    return true;
  }

  public static boolean restore(String bakPath, String dstPath) throws KvStoreException {
    try {
      BackupableDBOptions bkOptions = new BackupableDBOptions(bakPath);
      BackupEngine engine = BackupEngine.open(Env.getDefault(), bkOptions);
      engine.restoreDbFromBackup(1, dstPath, dstPath, new RestoreOptions(false));
    } catch (RocksDBException ex) {
      logger.error("restore rocks db {} from a backup {} failed ", dstPath, bakPath, ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
    return true;
  }
}
