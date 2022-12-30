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
import java.util.List;
import org.rocksdb.ColumnFamilyHandle;
import py.common.struct.Pair;

public class FakeRocksDbColumnFamilyHandle extends RocksDbColumnFamilyHandle {
  private static final String FAKE_ROCKS_DB_PATH = "fake_path";

  public FakeRocksDbColumnFamilyHandle() {
    super(FAKE_ROCKS_DB_PATH);
  }

  @Override
  public boolean exist(byte[] key) throws KvStoreException {
    return false;
  }

  @Override
  public synchronized boolean open(boolean createIfMissing) throws IOException, KvStoreException {
    return false;
  }

  @Override
  public boolean put(byte[] key, byte[] value) throws KvStoreException {
    return false;
  }

  @Override
  public boolean isOpened() {
    return false;
  }

  @Override
  public byte[] get(byte[] key) throws KvStoreException {
    return null;
  }

  @Override
  public ColumnFamilyHandle getColumnFamilyHandle() {
    return null;
  }

  @Override
  public int clearRecordsByAsc(int numRecords) throws KvStoreException {
    return 0;
  }

  @Override
  public int recordNums() throws IOException {
    return 0;
  }

  @Override
  public List<Pair<byte[], byte[]>> getLatestRecords(int maxNums) {
    return new ArrayList<>();
  }

  @Override
  public String getColumnFamilyName() {
    return null;
  }

  @Override
  public synchronized void closeColumnFamily() {
  }

  @Override
  public void closeDb() throws KvStoreException {
  }

  @Override
  public void delete(byte[] key) throws KvStoreException {
  }

  @Override
  public void deleteColumnFamily() throws KvStoreException {
  }

  @Override
  public void sync() throws KvStoreException {
  }

  @Override
  protected String packColumnFamilyName() {
    return null;
  }

  @Override
  protected boolean needCacheRecordsNumInMemory() {
    return false;
  }
}
