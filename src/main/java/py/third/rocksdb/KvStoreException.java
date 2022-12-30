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
