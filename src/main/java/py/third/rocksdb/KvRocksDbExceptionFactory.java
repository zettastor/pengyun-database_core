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

import org.rocksdb.RocksDBException;
import py.third.rocksdb.KvStoreException.KvStatus;

public class KvRocksDbExceptionFactory {
  public static KvStoreException build(RocksDBException rocksDbException) {
    switch (rocksDbException.getStatus().getCode()) {
      case IOError:
      case Undefined:
      case Corruption:
        return new KvStoreIoException((byte) rocksDbException.getStatus().getCode().ordinal(),
            rocksDbException.getMessage());
      default:
        return new KvStoreException((byte) rocksDbException.getStatus().getCode().ordinal(),
            rocksDbException.getMessage());
    }
  }

  public static KvStoreException build(KvStatus status, String message) {
    switch (status) {
      case IOError:
      case Undefined:
      case Corruption:
        return new KvStoreIoException(status, message);
      default:
        return new KvStoreException(status, message);
    }
  }
}
