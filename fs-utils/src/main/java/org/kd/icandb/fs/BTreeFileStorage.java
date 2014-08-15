package org.kd.icandb.fs;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Stack;
import java.util.stream.IntStream;

/**
 * @author Kiryl Dubarenka
 */
public class BTreeFileStorage<K extends Comparable<K>, V> extends AbstractMap<K, V> implements Closeable {

    private final int order;
    private final RandomAccessFile file;
    private final BTreeFileEntrySerializer<K, V> serializer;

    private BTreeFileStorage(File file, int order, long contentOffset,
                             BTreeFileEntrySerializer<K, V> serializer) throws IOException {
        this.order = order;
        if (!file.exists()) {
            //noinspection ResultOfMethodCallIgnored
            file.createNewFile();
        }
        this.serializer = serializer;
        this.file = new RandomAccessFile(file, "rws");
        set(BTreeFileHeader.ROOT_ADDRESS, 0L);
        set(BTreeFileHeader.COLLECTION_SIZE, 0L);
        set(BTreeFileHeader.CONTENT_OFFSET, contentOffset);
        set(BTreeFileHeader.B_TREE_DATA_SIZE, BTreeFileHeader.SIZE);
    }

    public static <K extends Comparable<K>, V> BTreeFileStorage<K, V> create(
            File file, int order, int contentOffset, BTreeFileEntrySerializer<K, V> serializer) throws IOException {
        return new BTreeFileStorage<>(file, order, contentOffset, serializer);
    }

    private <VT> void set(BTreeFileHeader<VT> header, VT value) {
        try {
            header.write(this.file, value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <VT> VT get(BTreeFileHeader<VT> header) {
        try {
            return header.read(this.file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public V put(K key, V value) {
        try {
            BTreeFileNode<K, V> entry = findLeaf(readRootEntry(), key);
            BTreeFileRef<K, V> ref = BTreeFileRef.forValue(file, serializer, key, value);
            storeRef(ref);
            return addToEntry(ref, entry, 0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeRef(BTreeFileRef<K, V> ref) throws IOException {
        if (serializer.inlineRef()) {
            return;
        }
        Long address = get(BTreeFileHeader.CONTENT_OFFSET);
        long size = ref.write(address);
        set(BTreeFileHeader.CONTENT_OFFSET, address + size);
    }

    private BTreeFileNode<K, V> findLeaf(BTreeFileNode<K, V> entry, K key) throws IOException {
        if (entry == null) {
            return null;
        }
        if (entry.isLeaf()) {
            return entry;
        }
        return findLeaf(entry.resolveChild(file, serializer, entry.findPosition(key)), key);
    }

    private V addToEntry(BTreeFileRef<K, V> t, BTreeFileNode<K, V> entry, long leftChild, long rightChild) throws IOException {
        if (entry == null) {
            @SuppressWarnings("unchecked") BTreeFileRef<K, V>[] values = new BTreeFileRef[order];
            values[0] = t;
            Long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
            long[] children = new long[order + 1];
            children[0] = leftChild;
            children[1] = rightChild;
            createEntry(newAddress, values, children);
            set(BTreeFileHeader.ROOT_ADDRESS, newAddress);
            set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileNode.getBinaryDataSize(order));
            return null;
        } else {
            if (!entry.isFull()) {
                entry.add(t, rightChild);
                entry.write(file, serializer);
                return null; // todo return actual value
            } else {
                long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
                set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileNode.getBinaryDataSize(order));
                BTreeFileRef<K, V> median = entry.splitEntry(t, rightChild)
                        .setOutputFile(file)
                        .createLeftEntry(entry.getAddress(), serializer)
                        .createRightEntry(newAddress, serializer)
                        .getMedian();
                return addToEntry(median, entry.getParent(), entry.getAddress(), newAddress);
            }
        }
    }

    private BTreeFileNode<K, V> readRootEntry() throws IOException {
        long address = get(BTreeFileHeader.ROOT_ADDRESS);
        return address == 0 ? null : BTreeFileNode.read(file, serializer, address, order, null);
    }

    private BTreeFileNode<K, V> createEntry(long offset, BTreeFileRef<K, V>[] values, long[] children) throws IOException {
        BTreeFileNode<K, V> entry = new BTreeFileNode<>(offset, null, values, children);
        entry.write(file, serializer);
        return entry;
    }

    @Override
    public boolean containsKey(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V get(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<Entry<K, V>>() {
            @Override
            public Iterator<Entry<K, V>> iterator() {
                try {
                    return new BTreeFileStorageIterator();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean isEmpty() {
                return BTreeFileStorage.this.isEmpty();
            }

            @Override
            public void clear() {
                BTreeFileStorage.this.clear();
            }

            @Override
            public boolean contains(Object k) {
                return BTreeFileStorage.this.containsKey(k);
            }

            @Override
            public int size() {
                return BTreeFileStorage.this.size();
            }
        };
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void close() throws IOException {
        this.file.close();
    }

    private BTreeFileNode<K, V> getHeadEntry() throws IOException {
        BTreeFileNode<K, V> entry = readRootEntry();
        while (!entry.isLeaf()) {
            entry = entry.resolveChild(file, serializer, 0);
        }
        return entry;
    }

    private BTreeFileNode<K, V> getTailEntry() throws IOException {
        BTreeFileNode<K, V> entry = readRootEntry();
        while (!entry.isLeaf()) {
            entry = entry.resolveChild(file, serializer, entry.size());
        }
        return entry;
    }

    private class BTreeFileStorageIterator implements Iterator<Entry<K, V>> {

        private BTreeFileNode<K, V> currentEntry = getHeadEntry();
        private BTreeFileNode<K, V> tailEntry = getTailEntry();
        private Stack<Integer> position;

        private BTreeFileStorageIterator() throws IOException {
            this.position = new Stack<>();
            IntStream.rangeClosed(1, currentEntry.getDepth() - 1)
                    .forEach(i -> position.push(0));
            position.push(-1);
        }

        @Override
        public boolean hasNext() {
            return currentEntry.getAddress() != tailEntry.getAddress()
                    || position.peek() + 1 != currentEntry.size();
        }

        @Override
        public Entry<K, V> next() {
            try {
                position.push(position.pop() + 1);
                while (!currentEntry.isLeaf()) {
                    currentEntry = currentEntry.resolveChild(file, serializer, position.peek());
                    position.push(0);
                }
                while (position.peek() == currentEntry.size()) {
                    position.pop();
                    currentEntry = currentEntry.getParent();
                }
                return currentEntry.getValue(position.peek());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
