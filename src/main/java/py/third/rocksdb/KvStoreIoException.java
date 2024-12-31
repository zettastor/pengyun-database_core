
package py.third.rocksdb;

public class KvStoreIoException extends KvStoreException {
  public KvStoreIoException(KvStatus code, String message) {
    super(code, message);
  }

  public KvStoreIoException(byte status, String message) {
    super(status, message);
  }
}
