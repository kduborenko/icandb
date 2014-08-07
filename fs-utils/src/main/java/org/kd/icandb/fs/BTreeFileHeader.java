package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kirk
 */
class BTreeFileHeader<T> {

    public static final BTreeFileHeader<Long> ROOT_ADDRESS = new BTreeFileHeader<>(long.class, 0);
    public static final BTreeFileHeader<Long> COLLECTION_SIZE = new BTreeFileHeader<>(long.class, 8);
    public static final BTreeFileHeader<Long> CONTENT_OFFSET = new BTreeFileHeader<>(long.class, 16);
    public static final BTreeFileHeader<Long> B_TREE_DATA_SIZE = new BTreeFileHeader<>(long.class, 24);

    public static final long SIZE = 32;

    private static final Map<Class<?>, WriterMethod<?>> WRITER_METHODS
            = Collections.unmodifiableMap(new HashMap<Class<?>, WriterMethod<?>>() {
        {
            put(long.class, (WriterMethod<Long>) RandomAccessFile::writeLong);
            put(int.class, (WriterMethod<Integer>) RandomAccessFile::writeInt);
        }
    });

    private static final Map<Class<?>, ReaderMethod<?>> READER_METHODS
            = Collections.unmodifiableMap(new HashMap<Class<?>, ReaderMethod<?>>() {
        {
            put(long.class, (ReaderMethod<Long>) RandomAccessFile::readLong);
            put(int.class, (ReaderMethod<Integer>) RandomAccessFile::readInt);
        }
    });

    private final Class<T> type;
    private final int offset;

    private BTreeFileHeader(Class<T> type, int offset) {
        this.type = type;
        this.offset = offset;
    }

    public Class<?> getType() {
        return type;
    }

    public int getOffset() {
        return offset;
    }

    @SuppressWarnings("unchecked")
    public void write(RandomAccessFile file, T val) throws IOException {
        file.seek(getOffset());
        ((WriterMethod<T>) WRITER_METHODS.get(getType())).write(file, val);
    }

    @SuppressWarnings("unchecked")
    public T read(RandomAccessFile file) throws IOException {
        file.seek(getOffset());
        return ((ReaderMethod<T>) READER_METHODS.get(getType())).read(file);
    }

    private interface WriterMethod<T> {

        void write(RandomAccessFile file, T value) throws IOException;

    }

    private interface ReaderMethod<T> {

        T read(RandomAccessFile file) throws IOException;

    }
}
