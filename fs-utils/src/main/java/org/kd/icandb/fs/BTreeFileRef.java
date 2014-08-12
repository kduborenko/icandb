package org.kd.icandb.fs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.StreamCorruptedException;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileRef<K, V> implements Map.Entry<K, V> {

    private final RandomAccessFile file;
    private long address;
    private K key;
    private V value;

    BTreeFileRef(RandomAccessFile file, long address) {
        this.file = file;
        this.address = address;
    }

    BTreeFileRef(RandomAccessFile file, K key, V value) {
        this.file = file;
        this.key = key;
        this.value = value;
    }

    public static <K, V> BTreeFileRef<K, V> forAddress(RandomAccessFile file, long address) {
        return new BTreeFileRef<>(file, address);
    }

    public static <K, V> BTreeFileRef<K, V> forValue(RandomAccessFile file, K key, V value) {
        return new BTreeFileRef<>(file, key, value);
    }

    public V getValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    public K getKey() {
        if (key == null) {
            try {
                file.seek(address);
                long length = file.readLong();
                boolean active = file.readBoolean();
                byte[] data = new byte[(int) length];
                file.read(data);
                ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data));
                //noinspection unchecked
                key = (K) is.readObject();
            } catch (NegativeArraySizeException|StreamCorruptedException|EOFException e) {
                key = (K) (Long) address; //todo implement with serializers
            } catch (ClassNotFoundException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        return key;
    }

    public long write(long address) throws IOException {
        this.address = address;
        file.seek(address);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(baos);
        os.writeObject(key);
        // todo write value
        byte[] data = baos.toByteArray();
        file.writeLong(data.length);
        file.writeBoolean(true);
        file.write(data);
        return data.length;
    }

    public long getAddress() {
        return address;
    }

    @Deprecated
    public void setAddress(long address) {
        this.address = address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BTreeFileRef that = (BTreeFileRef) o;

        if (key != null ? !key.equals(that.key) : that.key != null) return false;
        if (value != null ? !value.equals(that.value) : that.value != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }
}
