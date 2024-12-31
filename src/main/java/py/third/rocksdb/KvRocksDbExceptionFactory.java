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
