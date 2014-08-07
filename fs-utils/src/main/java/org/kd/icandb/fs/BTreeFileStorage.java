package org.kd.icandb.fs;

import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author kirk
 */
public class BTreeFileStorage<T extends Comparable<T>> implements Collection<T>, Closeable {

    private final int order;
    private final RandomAccessFile file;
    private final Class<T> type;

    private BTreeFileStorage(File file, Class<T> type, int order, long contentOffset) throws IOException {
        this.type = type;
        this.order = order;
        if (!file.exists()) {
            file.createNewFile();
        }
        this.file = new RandomAccessFile(file, "rws");
        set(BTreeFileHeader.ROOT_ADDRESS, 0L);
        set(BTreeFileHeader.COLLECTION_SIZE, 0L);
        set(BTreeFileHeader.CONTENT_OFFSET, contentOffset);
        set(BTreeFileHeader.B_TREE_DATA_SIZE, BTreeFileHeader.SIZE);
    }

    public static <T extends Comparable<T>> BTreeFileStorage<T> create(File file, Class<T> type, int order, int contentOffset) throws IOException {
        return new BTreeFileStorage<>(file, type, order, contentOffset);
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
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public Iterator<T> iterator() {
        try {
            return new BTreeFileStorageIterator();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean add(T t) {
        try {
            BTreeFileEntry<T> entry = findLeaf(readRootEntry(), t);
            BTreeFileRef<T> ref = BTreeFileRef.forValue(file, t);
            storeRef(ref);
            addToEntry(ref, entry, 0, 0);
            return true;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeRef(BTreeFileRef<T> ref) throws IOException {
        Long address = get(BTreeFileHeader.CONTENT_OFFSET);
        long size = ref.write(address);
        set(BTreeFileHeader.CONTENT_OFFSET, address + size + 9);
    }

    private BTreeFileEntry<T> findLeaf(BTreeFileEntry<T> entry, T t) throws IOException {
        if (entry == null) {
            return null;
        }
        if (entry.isLeaf()) {
            return entry;
        }
        return findLeaf(entry.resolveChild(file, entry.findPosition(t)), t);
    }

    private void addToEntry(BTreeFileRef<T> t, BTreeFileEntry<T> entry, long leftChild, long rightChild) throws IOException {
        if (entry == null) {
            @SuppressWarnings("unchecked") BTreeFileRef<T>[] values = new BTreeFileRef[order];
            values[0] = t;
            Long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
            long[] children = new long[order + 1];
            children[0] = leftChild;
            children[1] = rightChild;
            createEntry(newAddress, values, children);
            set(BTreeFileHeader.ROOT_ADDRESS, newAddress);
            set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileEntry.getBinaryDataSize(order));
        } else {
            if (!entry.isFull()) {
                entry.add(t, rightChild);
                entry.write(file);
            } else {
                long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
                set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileEntry.getBinaryDataSize(order));
                BTreeFileRef<T> median = entry.splitEntry(t, rightChild)
                        .setOutputFile(file)
                        .createLeftEntry(entry.getAddress())
                        .createRightEntry(newAddress)
                        .getMedian();
                addToEntry(median, entry.getParent(), entry.getAddress(), newAddress);
            }
        }
    }

    private BTreeFileEntry<T> readRootEntry() throws IOException {
        long address = get(BTreeFileHeader.ROOT_ADDRESS);
        return address == 0 ? null : BTreeFileEntry.read(file, type, address, order, null);
    }

    private BTreeFileEntry<T> createEntry(long offset, BTreeFileRef<T>[] values, long[] children) throws IOException {
        BTreeFileEntry<T> entry = new BTreeFileEntry<>(offset, null, values, children, type);
        entry.write(file);
        return entry;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public void close() throws IOException {
        this.file.close();
    }

    private BTreeFileEntry<T> getHeadEntry() throws IOException {
        BTreeFileEntry<T> entry = readRootEntry();
        while (!entry.isLeaf()) {
            entry = entry.resolveChild(file, 0);
        }
        return entry;
    }

    private BTreeFileEntry<T> getTailEntry() throws IOException {
        BTreeFileEntry<T> entry = readRootEntry();
        while (!entry.isLeaf()) {
            entry = entry.resolveChild(file, entry.size());
        }
        return entry;
    }

    private class BTreeFileStorageIterator implements Iterator<T> {

        private BTreeFileEntry<T> currentEntry = getHeadEntry();
        private BTreeFileEntry<T> tailEntry = getTailEntry();
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
        public T next() {
            try {
                position.push(position.pop() + 1);
                while (!currentEntry.isLeaf()) {
                    currentEntry = currentEntry.resolveChild(file, position.peek());
                    position.push(0);
                }
                while (position.peek() == currentEntry.size()) {
                    position.pop();
                    currentEntry = currentEntry.getParent();
                }
                return currentEntry.getValue(position.peek()).get();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
