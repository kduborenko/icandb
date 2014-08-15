package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

/**
 * @author Kiryl Dubarenka
 */
interface BTreeFileEntrySerializer<K, V> {

    boolean inlineEntry();

    void readData(byte[] data, BTreeFileEntry<K, V> entry) throws IOException;

    byte[] writeData(BTreeFileEntry<K, V> entry) throws IOException;

    default void writeNodeData(BTreeFileEntry<K, V> value, ByteBuffer bb) {
        bb.putLong(value == null ? 0 : value.getAddress());
    }

    default BTreeFileEntry<K,V> readNodeData(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, ByteBuffer bb) throws IOException {
        long address = bb.getLong();
        return address == 0 ? null : BTreeFileEntry.forAddress(file, serializer, address);
    }
}
