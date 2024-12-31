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
