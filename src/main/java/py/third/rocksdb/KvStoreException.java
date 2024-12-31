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

public class KvStoreException extends Exception {
  private static final long serialVersionUID = 1L;
  private final KvStatus status;

  public KvStoreException(KvStatus status, String message) {
    super(message);
    this.status = status;
  }

  public KvStoreException(byte status, String message) {
    super(message);
    this.status = KvStatus.getCode(status);
  }

  public KvStatus getStatus() {
    return status;
  }

  public enum KvStatus {
    Ok((byte) 0x00),
    NotFound((byte) 0x01),
    Corruption((byte) 0x02),
    NotSupported((byte) 0x03),
    InvalidArgument((byte) 0x04),
    IOError((byte) 0x05),
    MergeInProgress((byte) 0x06),
    Incomplete((byte) 0x07),
    ShutdownInProgress((byte) 0x08),
    TimedOut((byte) 0x09),
    Aborted((byte) 0x0A),
    Busy((byte) 0x0B),
    Expired((byte) 0x0C),
    TryAgain((byte) 0x0D),
    HasNotOpen((byte) 0x11),
    Undefined((byte) 0x7F);

    private final byte value;

    KvStatus(final byte value) {
      this.value = value;
    }

    public static KvStatus getCode(final byte value) {
      for (final KvStatus status : KvStatus.values()) {
        if (status.value == value) {
          return status;
        }
      }
      throw new IllegalArgumentException(
          "Illegal value provided for Code (" + value + ").");
    }
  }
}
