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

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.WALRecoveryMode;

public class RocksDbOptionConfiguration {
  private String dbRootPath;

  private int maxBackgroundCompactions = 4;
  private int maxBackgroundFlushes = 4;
  private long bytesPerSync = 1048576;
  private long maxTotalWalSize = 1024 * 1024 * 100;
  private long blockSize = 16384;
  private long blockCacheSizePerColumnFamily = 128L * 1024 * 1024;
  private long writeBufferSizePerColumnFamily = 67108864L;
  private boolean enableCacheIndexAndFilterBlocks = true;
  private boolean enablePinL0FilterAndIndexBlocksInCache = true;
  private boolean levelCompactionDynamicLevelBytes = true;
  private boolean syncFlag = false;
  private int maxNumCacheAsync = 100;
  private int rocksDbCount = 10;
  private int maxLogFileSize = 1024 * 1024 * 100;
  private int keepLogFileNumber = 5;

  private String logLevel = InfoLogLevel.WARN_LEVEL.name();

  private String compressionType = CompressionType.NO_COMPRESSION.name();

  private long blockCacheSize;

  private boolean useSpdk = false;

  private String spdkConfName;

  private String spdkBdevName;

  private int spdkCacheSize;

  public RocksDbOptionConfiguration(String dbRootPath) {
    this.dbRootPath = dbRootPath;
  }

  public RocksDbOptionConfiguration(String dbRootPath, int rocksDbCount, long blockCacheSize) {
    this.dbRootPath = dbRootPath;
    this.rocksDbCount = rocksDbCount;
    this.blockCacheSize = blockCacheSize;
  }

  public RocksDbOptionConfiguration(
      String dbRootPath,
      int maxBackgroundCompactions,
      int maxBackgroundFlushes,
      long bytesPerSync,
      long maxTotalWalSize,
      long blockSize,
      long blockCacheSizePerColumnFamily,
      long writeBufferSizePerColumnFamily,
      boolean enableCacheIndexAndFilterBlocks,
      boolean enablePinL0FilterAndIndexBlocksInCache,
      boolean levelCompactionDynamicLevelBytes,
      boolean syncFlag,
      int maxNumCacheAsync,
      String compressionType
  ) {
    this.dbRootPath = dbRootPath;
    this.maxBackgroundCompactions = maxBackgroundCompactions;
    this.maxBackgroundFlushes = maxBackgroundFlushes;
    this.bytesPerSync = bytesPerSync;
    this.maxTotalWalSize = maxTotalWalSize;
    this.blockSize = blockSize;
    this.blockCacheSizePerColumnFamily = blockCacheSizePerColumnFamily;
    this.writeBufferSizePerColumnFamily = writeBufferSizePerColumnFamily;
    this.enableCacheIndexAndFilterBlocks = enableCacheIndexAndFilterBlocks;
    this.enablePinL0FilterAndIndexBlocksInCache = enablePinL0FilterAndIndexBlocksInCache;
    this.levelCompactionDynamicLevelBytes = levelCompactionDynamicLevelBytes;
    this.syncFlag = syncFlag;
    this.maxNumCacheAsync = maxNumCacheAsync;
    this.compressionType = compressionType;
  }

  public DBOptions generateDbOptions() {
    DBOptions dbOptions = new DBOptions();
    dbOptions.setWalRecoveryMode(WALRecoveryMode.TolerateCorruptedTailRecords);
    dbOptions.setUseDirectIoForFlushAndCompaction(true);
    dbOptions.setMaxBackgroundCompactions(this.getMaxBackgroundCompactions());
    dbOptions.setMaxBackgroundFlushes(this.getMaxBackgroundFlushes());
    dbOptions.setBytesPerSync(this.getBytesPerSync());
    dbOptions.setMaxLogFileSize(this.getMaxLogFileSize());
    dbOptions.setKeepLogFileNum(this.getKeepLogFileNumber());
    dbOptions.setInfoLogLevel(this.getLogLevel());
    dbOptions.setWalDir(dbRootPath);

    dbOptions.setMaxTotalWalSize(this.getMaxTotalWalSize());
    if (blockCacheSize != 0) {
      dbOptions.setRowCache(new LRUCache(blockCacheSize));
    }

    return dbOptions;
  }

  public ColumnFamilyOptions generateColumnFamilyOptions() {
    BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
    tableConfig.setBlockSize(this.getBlockSize());
    tableConfig.setBlockCacheSize(this.getBlockCacheSizePerColumnFamily());
    tableConfig.setCacheIndexAndFilterBlocks(this.isEnableCacheIndexAndFilterBlocks());
    tableConfig
        .setPinL0FilterAndIndexBlocksInCache(this.isEnablePinL0FilterAndIndexBlocksInCache());

    ColumnFamilyOptions columnFamilyOptions = new ColumnFamilyOptions();
    columnFamilyOptions
        .setLevelCompactionDynamicLevelBytes(this.isLevelCompactionDynamicLevelBytes());
    columnFamilyOptions.setTableFormatConfig(tableConfig);
    columnFamilyOptions.setCompressionType(this.getCompressionType());
    columnFamilyOptions.setWriteBufferSize(this.getWriteBufferSizePerColumnFamily());

    return columnFamilyOptions;
  }

  private CompressionType getIndexerCompressionType(String indexerCompressionType) {
    if (indexerCompressionType == null) {
      throw new IllegalArgumentException("null compression type.");
    }

    String compressionTypeStr;
    CompressionType ret;

    compressionTypeStr = indexerCompressionType.toUpperCase();
    ret = CompressionType.valueOf(compressionTypeStr);
    if (ret == null) {
      throw new IllegalArgumentException("Invalid compression type: " + indexerCompressionType);
    }

    return ret;
  }

  private InfoLogLevel getIndexerLogLevelType(String indexerLogLevel) {
    if (indexerLogLevel == null) {
      throw new IllegalArgumentException("null indexerLogLevel type.");
    }

    String compressionTypeStr;
    InfoLogLevel ret;

    compressionTypeStr = indexerLogLevel.toUpperCase();
    ret = InfoLogLevel.valueOf(compressionTypeStr);
    if (ret == null) {
      throw new IllegalArgumentException("Invalid indexerLogLevel type: " + indexerLogLevel);
    }

    return ret;
  }

  public CompressionType getCompressionType() {
    return getIndexerCompressionType(this.compressionType);
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
  }

  public InfoLogLevel getLogLevel() {
    return getIndexerLogLevelType(this.logLevel);
  }

  public void setLogLevel(String logLevel) {
    this.logLevel = logLevel;
  }

  public boolean isSyncFlag() {
    return syncFlag;
  }

  public void setSyncFlag(boolean syncFlag) {
    this.syncFlag = syncFlag;
  }

  public int getMaxNumCacheAsync() {
    return maxNumCacheAsync;
  }

  public void setMaxNumCacheAsync(int maxNumCacheAsync) {
    this.maxNumCacheAsync = maxNumCacheAsync;
  }

  public String getDbRootPath() {
    return dbRootPath;
  }

  public void setDbRootPath(String dbRootPath) {
    this.dbRootPath = dbRootPath;
  }

  public long getBlockSize() {
    return blockSize;
  }

  public void setBlockSize(long blockSize) {
    this.blockSize = blockSize;
  }

  public long getBlockCacheSizePerColumnFamily() {
    return blockCacheSizePerColumnFamily;
  }

  public void setBlockCacheSizePerColumnFamily(long blockCacheSizePerColumnFamily) {
    this.blockCacheSizePerColumnFamily = blockCacheSizePerColumnFamily;
  }

  public long getWriteBufferSizePerColumnFamily() {
    return writeBufferSizePerColumnFamily;
  }

  public void setWriteBufferSizePerColumnFamily(long writeBufferSizePerColumnFamily) {
    this.writeBufferSizePerColumnFamily = writeBufferSizePerColumnFamily;
  }

  public boolean isEnableCacheIndexAndFilterBlocks() {
    return enableCacheIndexAndFilterBlocks;
  }

  public void setEnableCacheIndexAndFilterBlocks(boolean enableCacheIndexAndFilterBlocks) {
    this.enableCacheIndexAndFilterBlocks = enableCacheIndexAndFilterBlocks;
  }

  public boolean isEnablePinL0FilterAndIndexBlocksInCache() {
    return enablePinL0FilterAndIndexBlocksInCache;
  }

  public void setEnablePinL0FilterAndIndexBlocksInCache(
      boolean enablePinL0FilterAndIndexBlocksInCache) {
    this.enablePinL0FilterAndIndexBlocksInCache = enablePinL0FilterAndIndexBlocksInCache;
  }

  public boolean isLevelCompactionDynamicLevelBytes() {
    return levelCompactionDynamicLevelBytes;
  }

  public void setLevelCompactionDynamicLevelBytes(boolean levelCompactionDynamicLevelBytes) {
    this.levelCompactionDynamicLevelBytes = levelCompactionDynamicLevelBytes;
  }

  public int getMaxBackgroundCompactions() {
    return maxBackgroundCompactions;
  }

  public void setMaxBackgroundCompactions(int maxBackgroundCompactions) {
    this.maxBackgroundCompactions = maxBackgroundCompactions;
  }

  public int getMaxBackgroundFlushes() {
    return maxBackgroundFlushes;
  }

  public void setMaxBackgroundFlushes(int maxBackgroundFlushes) {
    this.maxBackgroundFlushes = maxBackgroundFlushes;
  }

  public long getBytesPerSync() {
    return bytesPerSync;
  }

  public void setBytesPerSync(long bytesPerSync) {
    this.bytesPerSync = bytesPerSync;
  }

  public long getMaxTotalWalSize() {
    return maxTotalWalSize;
  }

  public void setMaxTotalWalSize(long maxTotalWalSize) {
    this.maxTotalWalSize = maxTotalWalSize;
  }

  public int getRocksDbCount() {
    return rocksDbCount;
  }

  public int getMaxLogFileSize() {
    return maxLogFileSize;
  }

  public void setMaxLogFileSize(int maxLogFileSize) {
    this.maxLogFileSize = maxLogFileSize;
  }

  public int getKeepLogFileNumber() {
    return keepLogFileNumber;
  }

  public void setKeepLogFileNumber(int keepLogFileNumber) {
    this.keepLogFileNumber = keepLogFileNumber;
  }

  public boolean isUseSpdk() {
    return useSpdk;
  }

  public void setUseSpdk(boolean useSpdk) {
    this.useSpdk = useSpdk;
  }

  public String getSpdkConfName() {
    return spdkConfName;
  }

  public void setSpdkConfName(String spdkConfName) {
    this.spdkConfName = spdkConfName;
  }

  public String getSpdkBdevName() {
    return spdkBdevName;
  }

  public void setSpdkBdevName(String spdkBdevName) {
    this.spdkBdevName = spdkBdevName;
  }

  public int getSpdkCacheSize() {
    return spdkCacheSize;
  }

  public void setSpdkCacheSize(int spdkCacheSize) {
    this.spdkCacheSize = spdkCacheSize;
  }
}
