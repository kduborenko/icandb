package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileRef<T> {

    private final RandomAccessFile file;
    private long address;
    private T value;

    BTreeFileRef(RandomAccessFile file, long address) {
        this.file = file;
        this.address = address;
    }

    BTreeFileRef(RandomAccessFile file, T value) {
        this.file = file;
        this.value = value;
    }

    public static <T> BTreeFileRef<T> forAddress(RandomAccessFile file, long address, Class<T> type) {
        if (Long.class.equals(type)) {
            //noinspection unchecked
            return (BTreeFileRef<T>) new BTreeFileLongRef(file, address);
        }
        return new BTreeFileRef<>(file, address);
    }

    public static <T> BTreeFileRef<T> forValue(RandomAccessFile file, T value) {
        if (value instanceof Long) {
            //noinspection unchecked
            return (BTreeFileRef<T>) new BTreeFileLongRef(file, (Long) value);
        }
        return new BTreeFileRef<>(file, value);
    }

    public T get() throws IOException {
        if (value == null) {
            file.seek(address);
            long length = file.readLong();
            boolean active = file.readBoolean();
            byte[] data = new byte[(int) length];
            file.read(data);
            value = (T) new String(data);
        }
        return value;
    }

    public long write(long address) throws IOException {
        this.address = address;
        file.seek(address);
        byte[] data = value.toString().getBytes();
        file.writeLong(data.length);
        file.writeBoolean(true);
        file.write(data);
        return data.length;
    }

    public long getAddress() {
        return address;
    }
}
