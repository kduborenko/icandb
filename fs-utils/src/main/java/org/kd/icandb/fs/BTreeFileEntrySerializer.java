package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * @author Kiryl Dubarenka
 */
interface BTreeFileEntrySerializer<K, V> {

    boolean inlineRef();

    void readData(byte[] data, BTreeFileRef<K, V> ref) throws IOException;

    byte[] writeData(BTreeFileRef<K, V> ref) throws IOException;

    default void writeNodeData(BTreeFileRef<K, V> value, ByteBuffer bb) {
        bb.putLong(value == null ? 0 : value.getAddress());
    }

    default BTreeFileRef<K,V> readNodeData(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, ByteBuffer bb) throws IOException {
        long address = bb.getLong();
        return address == 0 ? null : BTreeFileRef.forAddress(file, serializer, address);
    }
}
