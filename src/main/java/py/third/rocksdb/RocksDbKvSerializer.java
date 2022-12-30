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
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;

public interface RocksDbKvSerializer {
  public int size();

  default void serialize(byte[] bytes) throws IOException {
    Validate.isTrue(bytes.length == size());
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    serialize(buffer);
  }

  public void serialize(ByteBuffer buffer) throws IOException;

  default boolean deserialize(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    return deserialize(buffer);
  }

  public boolean deserialize(ByteBuffer buffer) throws IOException;

}
