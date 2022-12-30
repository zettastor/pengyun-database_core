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
import org.apache.commons.lang3.Validate;
import org.apache.directory.api.util.Strings;
import org.rocksdb.BackupEngine;
import org.rocksdb.BackupableDBOptions;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDbBase {
  private static final Logger logger = LoggerFactory.getLogger(RocksDbBase.class);

  static {
    RocksDB.loadLibrary();
  }

  private List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
  private List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<ColumnFamilyHandle>();
  private RocksDB rocksdb = null;
  private boolean dbOpenFlag = false;
  private String dbPath = null;

  protected RocksDbBase() {
  }

  public RocksDB getRocksdb() {
    return rocksdb;
  }

  public List<ColumnFamilyDescriptor> getColumnFamilyDescriptors() {
    return columnFamilyDescriptors;
  }

  public List<ColumnFamilyHandle> getColumnFamilyHandles() {
    return columnFamilyHandleList;
  }

  protected synchronized RocksDbBase create(DBOptions options, String path,
      List<ColumnFamilyDescriptor> descriptors,
      List<ColumnFamilyHandle> handleList)
      throws RocksDBException {
    Validate.isTrue(options.createIfMissing() && options.errorIfExists(),
        "invalid create paramer for create rocks db");

    columnFamilyDescriptors.clear();
    columnFamilyHandleList.clear();
    for (ColumnFamilyDescriptor descriptor : descriptors) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(descriptor.columnFamilyName(),
              descriptor.columnFamilyOptions())
      );
    }
    ColumnFamilyOptions ops = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
    columnFamilyDescriptors.add(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, ops)
    );

    options.setMaxFileOpeningThreads(128);
    rocksdb = RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandleList);
    if (null == rocksdb) {
      logger.info("create rocks db failed, path is {}", path);
      throw new RocksDBException("create rocks db failed, maybe it has existed before",
          new Status(Status.Code.Undefined, Status.SubCode.None, "invalid argument"));
    }
    logger.info("rocks db has been created , path is {}", path);
    logger.info("{} column family is created and connected", columnFamilyDescriptors.size());
    int index = 0;
    for (ColumnFamilyDescriptor descriptor : columnFamilyDescriptors) {
      logger
          .info("column family {} name is {}", index++, new String(descriptor.columnFamilyName()));
    }

    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
      handleList.add(handle);
    }

    dbOpenFlag = true;
    dbPath = path;

    return this;
  }

  protected synchronized RocksDB innerOpen(DBOptions options, String path, ColumnFamilyOptions ops)
      throws RocksDBException {
    if (dbOpenFlag) {
      if (Strings.equals(dbPath, path)) {
        return rocksdb;
      } else {
        logger.error("open path is {}, current path is {}", dbPath, path);
        throw new RocksDBException("in vaild rocks db path, system just one db path",
            new Status(Status.Code.Undefined, Status.SubCode.None, "invalid argument"));
      }
    }

    columnFamilyDescriptors.clear();
    columnFamilyHandleList.clear();
    if (null == ops) {
      ops = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
    }
    List<byte[]> columnFamilies = RocksDB.listColumnFamilies(new Options(options, ops), path);
    for (byte[] column : columnFamilies) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(column, ops)
      );
    }

    if (columnFamilyDescriptors.size() == 0) {
      columnFamilyDescriptors.add(
          new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, ops)
      );
    }
    rocksdb = RocksDB.open(options, path, columnFamilyDescriptors, columnFamilyHandleList);
    logger.info("rocks db has been created or opened, path is {}", path);
    logger.info("{} column family is connected", columnFamilies.size());
    int index = 0;
    for (byte[] column : columnFamilies) {
      logger.info("column family {} name is {}", index++, new String(column));
    }

    dbOpenFlag = true;
    dbPath = path;

    return rocksdb;
  }

  protected synchronized RocksDB innerOpen(DBOptions options, String path,
      List<ColumnFamilyDescriptor> columnDescriptors,
      List<ColumnFamilyHandle> columnHandleList)
      throws RocksDBException {
    ColumnFamilyOptions ops = null;
    if (columnDescriptors.size() > 0) {
      ops = columnDescriptors.get(0).columnFamilyOptions();
    }
    innerOpen(options, path, ops);

    boolean bneedCreate;
    for (ColumnFamilyDescriptor descriptor : columnDescriptors) {
      bneedCreate = true;
      for (int i = 0; i < this.columnFamilyDescriptors.size(); i++) {
        ColumnFamilyDescriptor myDescriptor = this.columnFamilyDescriptors.get(i);
        if (Arrays.equals(descriptor.columnFamilyName(), myDescriptor.columnFamilyName())) {
          columnHandleList.add(this.columnFamilyHandleList.get(i));
          bneedCreate = false;
          break;
        }
      }

      if (bneedCreate) {
        if (!options.createMissingColumnFamilies()) {
          throw new RocksDBException("column family has not been created",
              new Status(Status.Code.NotFound, Status.SubCode.None, "invalid argument"));
        }
        ColumnFamilyHandle handle = rocksdb.createColumnFamily(descriptor);
        columnHandleList.add(handle);
        logger.info("create column family name is {}, handle is {}",
            new String(descriptor.columnFamilyName()), handle);

        this.columnFamilyDescriptors.add(descriptor);
        this.columnFamilyHandleList.add(handle);
      }
    }

    return rocksdb;
  }

  public synchronized void dropColumnFamily(ColumnFamilyHandle handle) throws RocksDBException {
    if (null == rocksdb) {
      return;
    }

    if (!columnFamilyHandleList.contains(handle)) {
      throw new RocksDBException("handle is invaild",
          new Status(Status.Code.NotFound, Status.SubCode.None, "invalid argument"));
    }

    for (int i = 0; i < columnFamilyHandleList.size(); i++) {
      if (handle == columnFamilyHandleList.get(i)) {
        ColumnFamilyDescriptor descriptor = columnFamilyDescriptors.remove(i);
        columnFamilyHandleList.remove(i);
        descriptor.columnFamilyName();
        logger.info("drop column family name is {}, handle is {}",
            new String(descriptor.columnFamilyName()), handle);
        rocksdb.dropColumnFamily(handle);
        break;
      }
    }
    return;
  }

  public synchronized void close() {
    logger.warn("close rocksdb, dbpath {}", dbPath);
    if (null == rocksdb) {
      return;
    }

    for (ColumnFamilyHandle handle : columnFamilyHandleList) {
      handle.close();
    }

    columnFamilyHandleList.clear();
    columnFamilyDescriptors.clear();
    rocksdb.close();
    rocksdb = null;
    dbOpenFlag = false;

    logger.info("rocks db has been created or opened, path is {}", dbPath);
    dbPath = null;
  }

  public synchronized boolean backup(String path) throws RocksDBException {
    BackupableDBOptions bkOptions = new BackupableDBOptions(path);
    try {
      BackupEngine engine = BackupEngine.open(Env.getDefault(), bkOptions);
      engine.createNewBackup(rocksdb);
    } catch (RocksDBException ex) {
      logger.error("create a backup rocksdb failed {}", path, ex);
      throw ex;
    }
    return true;
  }
}
