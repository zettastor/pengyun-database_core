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
