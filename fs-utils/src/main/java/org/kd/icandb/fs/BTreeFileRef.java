package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileRef<K, V> implements Map.Entry<K, V> {

    private final RandomAccessFile file;
    private final BTreeFileEntrySerializer<K, V> serializer;
    private long address;
    private K key;
    private V value;

    BTreeFileRef(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, long address) throws IOException {
        this.file = file;
        this.address = address;
        this.serializer = serializer;
        // todo lazy loading
        if (!serializer.inlineRef()) {
            file.seek(address);
            long length = file.readLong();
            boolean active = file.readBoolean();
            byte[] data = new byte[(int) length];
            file.read(data);
            serializer.readData(data, this);
        }
    }

    BTreeFileRef(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, K key, V value) {
        this.file = file;
        this.key = key;
        this.value = value;
        this.serializer = serializer;
    }

    public static <K, V> BTreeFileRef<K, V> forAddress(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer,
                                                       long address) throws IOException {
        return new BTreeFileRef<>(file, serializer, address);
    }

    public static <K, V> BTreeFileRef<K, V> forValue(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer,
                                                     K key, V value) {
        return new BTreeFileRef<>(file, serializer, key, value);
    }

    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public long write(long address) throws IOException {
        this.address = address;
        file.seek(address);
        byte[] data = serializer.writeData(this);
        file.writeLong(data.length);
        file.writeBoolean(true);
        file.write(data);
        return data.length + 8 + 1;
    }

    public long getAddress() {
        return address;
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
