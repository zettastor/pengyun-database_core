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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import py.common.Utils;
import py.common.struct.Pair;
import py.test.TestBase;

public class RocksDbDefaultFamilyHandleTest extends TestBase {
  private static final String basePath = "/tmp/rocks_db_instance_root";
  private static final String dbPath = "/tmp/rocks_db_instance_root/unit_test_for_rocksdb_base";
  private static final String dbPath_performance = "/tmp/rocks_db_instance_root/unit_test_for"
      + "_rocksdb_performance";

  @Before
  public void initialize() {
    File baseDir = new File(basePath);
    if (!baseDir.exists()) {
      Utils.mkDir(baseDir);
    }
    File dbDir = new File(dbPath);
    if (dbDir.exists()) {
      Utils.deleteDirectory(dbDir);
    }
    dbDir = new File(dbPath_performance);
    if (dbDir.exists()) {
      Utils.deleteDirectory(dbDir);
    }
  }

  @After
  public void cleanDatabaseFile() {
  }

  @Ignore
  @Test
  public void rocksDbHomeMoved() throws Exception {
    RocksDbColumnFamilyHandle handle = new DefaultRocksDbColumnFamilyHandle(dbPath);

    assertTrue(handle.open(true));
    assertTrue(handle.isOpened());
    assertEquals(0, handle.recordNums());

    String key1 = "key_1_for_test";
    String value1 = "value_1_for_test";
    assertTrue(handle.put(key1.getBytes(), value1.getBytes()));
    assertEquals(1, handle.recordNums());

    String key2 = "key_2_for_test";
    String value2 = "value_2_for_test";
    assertTrue(handle.put(key2.getBytes(), value2.getBytes()));
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    String key3 = "key_3_for_test";
    String value3 = "value_3_for_test";
    assertTrue(handle.put(key3.getBytes(), value3.getBytes()));
    assertEquals(3, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    handle.closeDb();

    File root = new File(dbPath);
    assertTrue(root.exists());

    String cmd = "cp -fr " + dbPath + " " + dbPath_performance;
    Process ps = Runtime.getRuntime().exec(cmd);
    ps.waitFor();

    RocksDbColumnFamilyHandle handle1 = new DefaultRocksDbColumnFamilyHandle(dbPath_performance);
   
    assertTrue(handle1.open(true));
    assertTrue(handle1.isOpened());
    assertEquals(3, handle1.recordNums());
  }

  @Test
  public void basicTest() throws Exception {
   
    RocksDbColumnFamilyHandle handle = new DefaultRocksDbColumnFamilyHandle(dbPath);

    assertTrue(handle.open(true));
    assertTrue(handle.isOpened());
    assertEquals(0, handle.recordNums());

    String key1 = "key_1_for_test";
    String value1 = "value_1_for_test";
    assertTrue(handle.put(key1.getBytes(), value1.getBytes()));
    assertEquals(1, handle.recordNums());

    String key2 = "key_2_for_test";
    String value2 = "value_2_for_test";
    assertTrue(handle.put(key2.getBytes(), value2.getBytes()));
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    String key3 = "key_3_for_test";
    String value3 = "value_3_for_test";
    assertTrue(handle.put(key3.getBytes(), value3.getBytes()));
    assertEquals(3, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    handle.delete(key2.getBytes());
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    handle.delete(key2.getBytes());
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    handle.closeColumnFamily();
    assertFalse(handle.isOpened());

    assertTrue(handle.open(true));
    assertTrue(handle.isOpened());
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    assertTrue(handle.put(key2.getBytes(), value2.getBytes()));
    assertEquals(3, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    value2 = "value_2_for_test_dup";
    assertTrue(handle.put(key2.getBytes(), value2.getBytes()));
    assertEquals(3, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    List<Pair<byte[], byte[]>> result = handle.getLatestRecords(1);
    assertEquals(1, result.size());
    assertTrue(Arrays.equals(value3.getBytes(), result.get(0).getSecond()));

    result = handle.getLatestRecords(10);
    assertEquals(3, result.size());
    assertTrue(Arrays.equals(value1.getBytes(), result.get(0).getSecond()));
    assertTrue(Arrays.equals(value2.getBytes(), result.get(1).getSecond()));

    assertEquals(1, handle.clearRecordsByAsc(1));
    assertEquals(2, handle.recordNums());
    assertEquals(handle.getRowNum(), handle.recordNums());

    handle.closeColumnFamily();
  }

  @Test
  public void performanceTest() throws Exception {
    final int numOfProcessor = 4;
    final int numOfRecordsPerProcessor = 1000000;

    final AtomicBoolean error = new AtomicBoolean(false);

    long key = 10000;
    List<ColumnFamilyRunable> getters = new ArrayList<>();
    for (int i = 0; i < numOfProcessor; i++) {
      ColumnFamilyRunable runable = new ColumnFamilyRunable(1,
          key, key + numOfRecordsPerProcessor, error);
      getters.add(runable);
      runable.start();

      key += numOfRecordsPerProcessor;
    }

    for (ColumnFamilyRunable runable : getters) {
      runable.join();
    }
    assertFalse(error.get());

    long startTime = System.currentTimeMillis();
    logger.warn(
        "successful put performance test, {} processor put {} records to rocks db, Elapsed time -- "
            + "{} ms",
        numOfProcessor, numOfRecordsPerProcessor,
        startTime - System.currentTimeMillis());

    sleep("delay time between write and get", 1000);
    getters.clear();
    key = 10000;
    startTime = System.currentTimeMillis();
    for (int i = 0; i < numOfProcessor; i++) {
      ColumnFamilyRunable runable = new ColumnFamilyRunable(0,
          key, key + numOfRecordsPerProcessor, error);
      getters.add(runable);
      runable.start();

      key += numOfRecordsPerProcessor;
    }

    for (ColumnFamilyRunable runable : getters) {
      runable.join();
    }
    assertFalse(error.get());
    logger.warn(
        "successful put performance test, {} processor put {} records to rocks db, Elapsed time -- "
            + "{} ms",
        numOfProcessor, numOfRecordsPerProcessor,
        startTime - System.currentTimeMillis());

    return;
  }

  static class ColumnFamilyRunable extends Thread {
    final AtomicBoolean error;
    final long begin;
    final long end;
    final int readOrWrite;

    ColumnFamilyRunable(int readOrWrite, long begin, long end, AtomicBoolean error) {
      this.begin = begin;
      this.end = end;
      this.error = error;
      this.readOrWrite = readOrWrite;
    }

    @Override
    public void run() {
      try {
        RocksDbColumnFamilyHandle handle = new DefaultRocksDbColumnFamilyHandle(dbPath_performance);

        assertTrue(handle.open(true));
        assertTrue(handle.isOpened());

        byte[] key = new byte[Long.SIZE / Byte.SIZE];
        byte[] value = new byte[(Long.SIZE / Byte.SIZE) * 10];
        ByteBuffer bufferKey = ByteBuffer.wrap(key);
        ByteBuffer bufferValue = ByteBuffer.wrap(value);
        long startTime = System.currentTimeMillis();
        if (readOrWrite == 1) {
          for (long l = begin; l < end; l++) {
            bufferKey.clear();
            bufferValue.clear();

            bufferKey.putLong(l);
            bufferValue.putLong(l);
            bufferValue.putLong(l + 1);
            assertTrue(handle.put(key, value));
          }
          logger.warn("put {} records, Elapsed time -- {} ms", end - begin,
              startTime - System.currentTimeMillis());
        } else {
          long l = 0;
          do {
            long k = RandomUtils.nextInt((int) (end - begin));
            k += begin;
            bufferKey.clear();
            bufferValue.clear();

            bufferKey.putLong(k);
            bufferValue.putLong(k);
            bufferValue.putLong(k + 1);
            byte[] result = handle.get(key);
            if (null != result) {
              assertTrue(Arrays.equals(value, result));
            } else {
              break;
            }
            l++;
          } while (l < (end - begin));
          logger.warn("get {} records, Elapsed time -- {} ms", l,
              startTime - System.currentTimeMillis());
        }

      } catch (Exception ex) {
        logger.error("exception error", ex);
        error.set(true);
        return;
      }
      return;
    }
  }
}
