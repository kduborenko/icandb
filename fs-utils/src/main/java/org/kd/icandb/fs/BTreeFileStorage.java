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
public class BTreeFileStorage<K, V> extends AbstractMap<K, V> implements Closeable {

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

    public static <K, V> BTreeFileStorage<K, V> create(
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
        // todo optimize
        int count = 0;
        for (Entry<K, V> e : entrySet()) {
            count++;
        }
        return count;
    }

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override
    public V put(K key, V value) {
        try {
            BTreeFileNode<K, V> node = findLeaf(readRootNode(), key);
            BTreeFileEntry<K, V> entry = BTreeFileEntry.forValue(file, serializer, key, value);
            storeEntry(entry);
            return addToNode(entry, node, 0, 0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void storeEntry(BTreeFileEntry<K, V> entry) throws IOException {
        if (serializer.inlineEntry()) {
            return;
        }
        Long address = get(BTreeFileHeader.CONTENT_OFFSET);
        long size = entry.write(address);
        set(BTreeFileHeader.CONTENT_OFFSET, address + size);
    }

    private BTreeFileNode<K, V> findLeaf(BTreeFileNode<K, V> node, K key) throws IOException {
        if (node == null) {
            return null;
        }
        if (node.isLeaf()) {
            return node;
        }
        return findLeaf(node.resolveChild(file, serializer, node.findPosition(key)), key);
    }

    private V addToNode(BTreeFileEntry<K, V> t, BTreeFileNode<K, V> node, long leftChild, long rightChild) throws IOException {
        if (node == null) {
            @SuppressWarnings("unchecked") BTreeFileEntry<K, V>[] values = new BTreeFileEntry[order];
            values[0] = t;
            Long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
            long[] children = new long[order + 1];
            children[0] = leftChild;
            children[1] = rightChild;
            createNode(newAddress, values, children);
            set(BTreeFileHeader.ROOT_ADDRESS, newAddress);
            set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileNode.getBinaryDataSize(order));
            return null;
        } else {
            if (!node.isFull()) {
                node.add(t, rightChild);
                node.write(file, serializer);
                return null; // todo return actual value
            } else {
                long newAddress = get(BTreeFileHeader.B_TREE_DATA_SIZE);
                set(BTreeFileHeader.B_TREE_DATA_SIZE, newAddress + BTreeFileNode.getBinaryDataSize(order));
                BTreeFileEntry<K, V> median = node.splitNode(t, rightChild)
                        .setOutputFile(file)
                        .createLeftNode(node.getAddress(), serializer)
                        .createRightNode(newAddress, serializer)
                        .getMedian();
                return addToNode(median, node.getParent(), node.getAddress(), newAddress);
            }
        }
    }

    private BTreeFileNode<K, V> readRootNode() throws IOException {
        long address = get(BTreeFileHeader.ROOT_ADDRESS);
        return address == 0 ? null : BTreeFileNode.read(file, serializer, address, order, null);
    }

    private BTreeFileNode<K, V> createNode(long offset, BTreeFileEntry<K, V>[] values, long[] children) throws IOException {
        BTreeFileNode<K, V> node = new BTreeFileNode<>(offset, null, values, children);
        node.write(file, serializer);
        return node;
    }

    @Override
    public boolean containsKey(Object key) {
        BTreeFileEntry<K, V> entry = findEntry((K) key);
        return entry != null;
    }

    @Override
    public V get(Object key) {
        BTreeFileEntry<K, V> entry = findEntry((K) key);
        return entry != null ? entry.getValue() : null;
    }

    public BTreeFileEntry<K, V> findEntry(K key) {
        try {
            BTreeFileNode<K, V> node = readRootNode();
            while (node != null) {
                int position = node.findPosition(key);
                if (position != node.size()) {
                    BTreeFileEntry<K, V> entry = node.getValue(position);
                    if (entry.getKey().equals(key)) {
                        return entry;
                    }
                }
                node = node.isLeaf() ? null : node.resolveChild(file, serializer, position);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public V remove(Object key) {
        BTreeFileEntry<K, V> entry = findEntry((K) key);
        if (entry == null) {
            return null;
        }
        entry.getNode().remove(entry.getPosition());
        try {
            entry.getNode().write(file, serializer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return entry.getValue();
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

    private BTreeFileNode<K, V> getHeadNode() throws IOException {
        BTreeFileNode<K, V> node = readRootNode();
        while (!node.isLeaf()) {
            node = node.resolveChild(file, serializer, 0);
        }
        return node;
    }

    private BTreeFileNode<K, V> getTailNode() throws IOException {
        BTreeFileNode<K, V> node = readRootNode();
        while (!node.isLeaf()) {
            node = node.resolveChild(file, serializer, node.size());
        }
        return node;
    }

    private class BTreeFileStorageIterator implements Iterator<Entry<K, V>> {

        private BTreeFileNode<K, V> currentNode = getHeadNode();
        private BTreeFileNode<K, V> tailNode = getTailNode();
        private Stack<Integer> position;

        private BTreeFileStorageIterator() throws IOException {
            this.position = new Stack<>();
            IntStream.rangeClosed(1, currentNode.getDepth() - 1)
                    .forEach(i -> position.push(0));
            position.push(-1);
        }

        @Override
        public boolean hasNext() {
            return currentNode.getAddress() != tailNode.getAddress()
                    || position.peek() + 1 != currentNode.size();
        }

        @Override
        public Entry<K, V> next() {
            try {
                position.push(position.pop() + 1);
                while (!currentNode.isLeaf()) {
                    currentNode = currentNode.resolveChild(file, serializer, position.peek());
                    position.push(0);
                }
                while (position.peek() == currentNode.size()) {
                    position.pop();
                    currentNode = currentNode.getParent();
                }
                return currentNode.getValue(position.peek());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
