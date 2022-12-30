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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.Status;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;

public abstract class RocksDbColumnFamilyHandle {
  private static final Logger logger = LoggerFactory.getLogger(RocksDbColumnFamilyHandle.class);
  private final AtomicInteger currentNums = new AtomicInteger(0);

  private final AtomicInteger records = new AtomicInteger(0);

  private final AtomicBoolean connected = new AtomicBoolean(false);
  protected WriteOptions writeOptions;

  private RocksDbBase dbBase;
  private DBOptions options;
  private String dbPath;
  private String columnFamilyName;
  private ColumnFamilyHandle columnFamilyHandle;
  private RocksDbOptionConfiguration rocksDbOptionConfiguration;
  private boolean useSpdk = false;
  private String spdkConfName;
  private String spdkBdevName;
  private int spdkCacheSize;

  private boolean sync;

  private int numsOnceSync;

  public RocksDbColumnFamilyHandle(String dbPath) {
    this.dbPath = dbPath;
    this.sync = false;
    this.numsOnceSync = 0;

    this.rocksDbOptionConfiguration = new RocksDbOptionConfiguration(dbPath);
  }

  public RocksDbColumnFamilyHandle(RocksDbOptionConfiguration rocksDbOptionConfiguration) {
    this.rocksDbOptionConfiguration = rocksDbOptionConfiguration;
    dbPath = rocksDbOptionConfiguration.getDbRootPath();
    sync = rocksDbOptionConfiguration.isSyncFlag();
    numsOnceSync = rocksDbOptionConfiguration.getMaxNumCacheAsync();
  }

  public RocksDbColumnFamilyHandle(RocksDbOptionConfiguration rocksDbOptionConfiguration,
      String dbPathInfo) {
    this.rocksDbOptionConfiguration = rocksDbOptionConfiguration;
    dbPath = dbPathInfo;
    sync = rocksDbOptionConfiguration.isSyncFlag();
    numsOnceSync = rocksDbOptionConfiguration.getMaxNumCacheAsync();
  }

  protected abstract boolean needCacheRecordsNumInMemory();

  protected abstract String packColumnFamilyName();

  private boolean checkColumnFamilyName() {
    return packColumnFamilyName().equals(this.getColumnFamilyName());
  }

  private void checkOpen() {
    if (!connected.get()) {
      String errMsg = "rocks db is not open, path is" + dbPath;
      logger.error(errMsg);
      throw new IllegalStateException(errMsg);
    }
  }

  public void deleteColumnFamily() throws KvStoreException {
    checkOpen();
    RocksDbBaseHelper.dropColumnFamily(this.columnFamilyName.getBytes(), dbPath);
    columnFamilyHandle = null;
    records.set(0);
    connected.set(false);
    return;
  }

  public synchronized boolean open(boolean createIfMissing) throws IOException, KvStoreException {
    if (connected.get()) {
      if (checkColumnFamilyName()) {
        return true;
      }
      logger.error("someone try to connect column family with unknown family name");
      return false;
    }

    this.setColumnFamilyName(packColumnFamilyName());

    final ColumnFamilyOptions cfOpts = rocksDbOptionConfiguration.generateColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
        new ColumnFamilyDescriptor(this.columnFamilyName.getBytes(), cfOpts)
    );
    options = rocksDbOptionConfiguration.generateDbOptions();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(createIfMissing);
    Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    options.setStatistics(statistics);
    options.setMaxSubcompactions(3);
    options.setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL);
    List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();
    dbBase = openRocksdb(options, dbPath, cfDescriptors, columnFamilyHandleList);

    if (columnFamilyHandleList.size() <= 0) {
      logger
          .error("open column name is {}, list rocks db {} column families:", this.columnFamilyName,
              dbPath);
      List<byte[]> columns = RocksDbBaseHelper.listColumnFamilies(options, dbPath);
      int i = 0;
      for (byte[] column : columns) {
        logger.warn("column {}'s name is {}", i++, new String(column));
      }

      List<ColumnFamilyDescriptor> descriptors = dbBase.getColumnFamilyDescriptors();
      logger.warn("list dbBase {} column families:", this.dbBase);
      for (ColumnFamilyDescriptor descriptor : descriptors) {
        logger.warn("column {}'s name is {}", new String(descriptor.columnFamilyName()));
      }
    }
    columnFamilyHandle = columnFamilyHandleList.get(0);

    if (null == writeOptions) {
      writeOptions = new WriteOptions().setSync(sync);
    }

    connected.set(true);
    int recordsNum = 0;
    if (needCacheRecordsNumInMemory()) {
      recordsNum = getRowNum();
      records.set(recordsNum);
    }

    logger.warn("rocks db column family {} is initialize, {} records in db path {}",
        this.columnFamilyName, recordsNum, dbPath);
    return true;
  }

  protected RocksDbBase openRocksdb(DBOptions options, String dbPath,
      List<ColumnFamilyDescriptor> cfDescriptors, List<ColumnFamilyHandle> columnFamilyHandleList)
      throws KvStoreException {
    return RocksDbBaseHelper.open(options, dbPath, cfDescriptors, columnFamilyHandleList);
  }

  public synchronized void closeColumnFamily() {
    logger.warn("rocks db column family {} has been close", this.columnFamilyName);
    columnFamilyHandle = null;

    if (writeOptions != null) {
      writeOptions.close();
      writeOptions = null;
    }

    if (options != null) {
      options.close();
      options = null;
    }

    connected.set(false);
    records.set(0);
  }

  protected RocksDB getRocksDb() {
    return dbBase.getRocksdb();
  }

  public String getColumnFamilyName() {
    return this.columnFamilyName;
  }

  private void setColumnFamilyName(String columnFamilyName) {
    this.columnFamilyName = columnFamilyName;
  }

  public ColumnFamilyHandle getColumnFamilyHandle() {
    return columnFamilyHandle;
  }

  public boolean isOpened() {
    return connected.get();
  }

  public int recordNums() throws IOException {
    if (needCacheRecordsNumInMemory()) {
      return records.get();
    } else {
      return getRowNum();
    }
  }

  public boolean put(byte[] key, byte[] value) throws KvStoreException {
    checkOpen();
    int times = 1;
    while (true) {
      try {
        dbBase.getRocksdb().put(columnFamilyHandle, writeOptions, key, value);
        if (needCacheRecordsNumInMemory()) {
          records.incrementAndGet();
        }
      } catch (RocksDBException ex) {
        if (times-- > 0 && (ex.getStatus().getCode() == Status.Code.TryAgain
            || ex.getStatus().getCode() == Status.Code.Busy
            || ex.getStatus().getCode() == Status.Code.MergeInProgress)) {
          continue;
        }
        logger.error("put rocks db failed, handle is {}", columnFamilyHandle, ex);
        throw KvRocksDbExceptionFactory.build(ex);
      }
      return true;
    }
  }

  public byte[] get(byte[] key) throws KvStoreException {
    checkOpen();

    int times = 1;
    try {
      return dbBase.getRocksdb().get(columnFamilyHandle, key);
    } catch (RocksDBException ex) {
      if (ex.getStatus().getCode() == Status.Code.TryAgain
          || ex.getStatus().getCode() == Status.Code.Busy
          || ex.getStatus().getCode() == Status.Code.MergeInProgress) {
        return get(key);
      }
      logger.error("get rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
  }

  public void delete(byte[] key) throws KvStoreException {
    checkOpen();
    try {
      if (needCacheRecordsNumInMemory()) {
        synchronized (records) {
          byte[] values = dbBase.getRocksdb().get(columnFamilyHandle, key);
          if (null != values) {
            records.decrementAndGet();
          }
        }
      }

      dbBase.getRocksdb().delete(columnFamilyHandle, writeOptions, key);
    } catch (RocksDBException ex) {
      if (ex.getStatus().getCode() == Status.Code.TryAgain
          || ex.getStatus().getCode() == Status.Code.Busy
          || ex.getStatus().getCode() == Status.Code.MergeInProgress) {
        delete(key);
        return;
      }
      logger.error("delete rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
    return;
  }

  public void rangeDelete(byte[] startKey, byte[] endKey) throws KvStoreException {
    checkOpen();
    try {
      dbBase.getRocksdb().deleteRange(columnFamilyHandle, writeOptions, startKey, endKey);
    } catch (RocksDBException ex) {
      if (ex.getStatus().getCode() == Status.Code.TryAgain
          || ex.getStatus().getCode() == Status.Code.Busy
          || ex.getStatus().getCode() == Status.Code.MergeInProgress) {
        rangeDelete(startKey, endKey);
        return;
      }
      logger.error("rangeDelete rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
    return;
  }

  public boolean exist(byte[] key) throws KvStoreException {
    checkOpen();
    try {
      byte[] value = getRocksDb().get(this.columnFamilyHandle, key);
      if (null == value) {
        return false;
      }
      return true;
    } catch (RocksDBException ex) {
      if (ex.getStatus().getCode() == Status.Code.TryAgain
          || ex.getStatus().getCode() == Status.Code.Busy
          || ex.getStatus().getCode() == Status.Code.MergeInProgress) {
        return exist(key);
      }
      logger.error("select rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
  }

  public void sync() throws KvStoreException {
    try {
      FlushOptions ops = new FlushOptions().setWaitForFlush(true);
      dbBase.getRocksdb().flush(ops);
    } catch (RocksDBException ex) {
      logger.error("sync rocks db failed", ex);
      throw KvRocksDbExceptionFactory.build(ex);
    }
  }

  public void closeDb() throws KvStoreException {
    closeColumnFamily();
    RocksDbBaseHelper.close(dbPath);
  }

  public List<Pair<byte[], byte[]>> getLatestRecords(int maxNums) {
    List<Pair<byte[], byte[]>> listLogs = new ArrayList<>();
    RocksIterator iterator = dbBase.getRocksdb().newIterator(columnFamilyHandle);
    try {
      iterator.seekToLast();

      while (iterator.isValid() && --maxNums >= 0) {
        listLogs.add(new Pair<>(iterator.key(), iterator.value()));
        iterator.prev();
      }
    } finally {
      iterator.close();
    }

    Collections.reverse(listLogs);
    return listLogs;
  }

  public int clearRecordsByAsc(int numRecords) throws KvStoreException {
    checkOpen();
    int countLogs = numRecords;
    RocksIterator iterator = dbBase.getRocksdb().newIterator(columnFamilyHandle);
    RocksIterator beginItr = dbBase.getRocksdb().newIterator(columnFamilyHandle);
    try {
      iterator.seekToFirst();
      beginItr.seekToFirst();
      while (iterator.isValid() && countLogs > 0) {
        countLogs--;
        iterator.next();
      }

      if (!iterator.isValid()) {
        iterator.seekToLast();
        getRocksDb().deleteRange(this.columnFamilyHandle, beginItr.key(), iterator.key());

        getRocksDb().delete(this.columnFamilyHandle, iterator.key());
      } else {
        getRocksDb().deleteRange(this.columnFamilyHandle, beginItr.key(), iterator.key());
      }

      if (needCacheRecordsNumInMemory()) {
        records.addAndGet(countLogs - numRecords);
      }
    } catch (RocksDBException ex) {
      logger.error("put rocks db failed, rocks db exception code is {}, subcode is {}",
          ex.getStatus().getCode(), ex.getStatus().getSubCode(), ex);
      throw KvRocksDbExceptionFactory.build(ex);
    } finally {
      iterator.close();
      beginItr.close();
    }
    return numRecords - countLogs;
  }

  protected int getRowNum() throws IOException {
    checkOpen();
    int num = 0;
    RocksIterator iterator = dbBase.getRocksdb().newIterator(columnFamilyHandle);
    try {
      iterator.seekToFirst();
      while (iterator.isValid()) {
        num++;
        iterator.next();
      }
    } finally {
      iterator.close();
    }

    iterator.close();
    return num;
  }

  protected void incAndGet(int delta) {
    records.addAndGet(delta);
  }

  public String getDbPath() {
    return dbPath;
  }

  public RocksDbBase getDbBase() {
    return dbBase;
  }
}
