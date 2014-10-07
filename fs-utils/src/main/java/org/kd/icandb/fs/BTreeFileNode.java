package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileNode<K extends Comparable<K>, V> {

    private final BTreeFileNode<K, V> parent;
    private final long address;
    private final BTreeFileEntry<K, V>[] values;
    private final long[] children;

    public BTreeFileNode(long address, BTreeFileNode<K, V> parent, BTreeFileEntry<K, V>[] values, long[] children) {
        assert values.length + 1 == children.length;
        this.parent = parent;
        this.address = address;
        this.values = values;
        this.children = children;
    }

    public void write(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer) throws IOException {
        file.seek(address);
        ByteBuffer bb = ByteBuffer.allocate(getBinaryDataSize(values.length));
        bb.putLong(0L); //todo remove
        for (BTreeFileEntry<K, V> value : values) {
            serializer.writeNodeData(value, bb);
        }
        for (long child : children) {
            bb.putLong(child);
        }
        file.write(bb.array());
    }

    public static int getBinaryDataSize(int order) {
        return 8 * (2 * order + 1) + 8;
    }

    public static <K extends Comparable<K>, V> BTreeFileNode<K, V> read(
            RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, long offset,
            int order, BTreeFileNode<K, V> parent) throws IOException {
        file.seek(offset);
        byte[] buffer = new byte[getBinaryDataSize(order)];
        file.read(buffer);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.getLong(); // todo remove read "parent"
        @SuppressWarnings("unchecked") BTreeFileEntry<K, V>[] values = new BTreeFileEntry[order];
        for (int i = 0; i < values.length; i++) {
            values[i] = serializer.readNodeData(file, serializer, bb);
        }
        @SuppressWarnings("unchecked") long[] children = new long[order + 1];
        for (int i = 0; i < children.length; i++) {
            children[i] = bb.getLong();
        }
        return new BTreeFileNode<>(offset, parent, values, children);
    }

    public void add(BTreeFileEntry<K, V> t, long rightChild) throws IOException {
        int position = findPosition(t.getKey());
        System.arraycopy(values, position, values, position + 1, values.length - position - 1);
        values[position] = t;
        if (!isLeaf()) {
            System.arraycopy(children, position + 1, children, position + 2, children.length - position - 2);
            children[position + 1] = rightChild;
        }
    }

    public NodeSplitter splitNode(BTreeFileEntry<K, V> newItem, long rightChild) throws IOException {
        int order = values.length;
        int medianPosition = (order + 1) / 2;
        int pos = findPosition(newItem.getKey());
        BTreeFileEntry<K, V> median;
        @SuppressWarnings("unchecked") BTreeFileEntry<K, V>[] left = new BTreeFileEntry[order];
        @SuppressWarnings("unchecked") BTreeFileEntry<K, V>[] right = new BTreeFileEntry[order];
        long[] childrenLeft = new long[order + 1];
        long[] childrenRight = new long[order + 1];
        if (pos == medianPosition) {
            System.arraycopy(values, 0, left, 0, medianPosition);
            System.arraycopy(values, medianPosition, right, 0, order - medianPosition);
            median = newItem;

            if (!isLeaf()) {
                System.arraycopy(children, 0, childrenLeft, 0, medianPosition + 1);
                childrenRight[0] = rightChild;
                System.arraycopy(children, medianPosition + 1, childrenRight, 1, order - medianPosition);
            }
        } else if (pos < medianPosition) {
            System.arraycopy(values, 0, left, 0, pos);
            left[pos] = newItem;
            System.arraycopy(values, pos, left, pos + 1, medianPosition - pos - 1);
            System.arraycopy(values, medianPosition, right, 0, order - medianPosition);
            median = values[medianPosition - 1];

            if (!isLeaf()) {
                System.arraycopy(children, 0, childrenLeft, 0, pos + 1);
                childrenLeft[pos  + 1] = rightChild;
                System.arraycopy(children, pos + 1, childrenLeft, pos + 2, medianPosition - pos - 1);
                System.arraycopy(children, medianPosition, childrenRight, 0, order + 1 - medianPosition);
            }
        } else {
            System.arraycopy(values, 0, left, 0, medianPosition);
            System.arraycopy(values, medianPosition + 1, right, 0, pos - medianPosition - 1);
            right[pos - medianPosition - 1] = newItem;
            System.arraycopy(values, pos, right, pos - medianPosition, order - pos);
            median = values[medianPosition];

            if (!isLeaf()) {
                System.arraycopy(children, 0, childrenLeft, 0, medianPosition + 1);
                System.arraycopy(children, medianPosition + 1, childrenRight, 0, pos - medianPosition);
                childrenRight[pos - medianPosition] = rightChild;
                System.arraycopy(children, pos + 1, childrenRight, pos - medianPosition + 1, order - pos);
            }
        }
        return new NodeSplitter(left, right, childrenLeft, childrenRight, median);
    }

    public int size() {
        // todo optimize
        return (int) Arrays.stream(values).filter(v -> v != null).count();
    }

    public boolean isFull() {
        return values.length == size();
    }

    public int findPosition(K key) throws IOException {
        int left = 0;
        int right = size() - 1;
        if (right == -1) {
            return 0;
        }
        if (values[left].getKey().compareTo(key) > 0) {
            return 0;
        }
        if (values[right].getKey().compareTo(key) < 0) {
            return right + 1;
        }
        while (right - left > 1) {
            int mid = left + (right - left) / 2;
            if (values[mid].getKey().compareTo(key) < 0) {
                left = mid;
            } else {
                right = mid;
            }
        }
        return right;
    }

    public BTreeFileEntry<K, V> getValue(int pos) {
        return values[pos];
    }

    public long getAddress() {
        return address;
    }

    public BTreeFileNode<K, V> getParent() {
        return parent;
    }

    public boolean isLeaf() {
        return this.children[0] == 0L;
    }

    public BTreeFileNode<K, V> resolveChild(RandomAccessFile file, BTreeFileEntrySerializer<K, V> serializer, int position) throws IOException {
        return read(file, serializer, children[position], this.values.length, this);
    }

    public int getDepth() {
        return parent != null ? parent.getDepth() + 1 : 1;
    }

    public class NodeSplitter {

        private final BTreeFileEntry<K, V>[] left;
        private final BTreeFileEntry<K, V>[] right;
        private final long[] childrenLeft;
        private final long[] childrenRight;
        private final BTreeFileEntry<K, V> median;

        private RandomAccessFile file;

        public NodeSplitter(BTreeFileEntry<K, V>[] left, BTreeFileEntry<K, V>[] right, long[] childrenLeft,
                            long[] childrenRight, BTreeFileEntry<K, V> median) {
            this.left = left;
            this.right = right;
            this.childrenLeft = childrenLeft;
            this.childrenRight = childrenRight;
            this.median = median;
        }

        public NodeSplitter setOutputFile(RandomAccessFile file) {
            if (this.file != null) {
                throw new IllegalStateException("Output file is already set.");
            }
            this.file = file;
            return this;
        }

        public NodeSplitter createLeftNode(long address, BTreeFileEntrySerializer<K, V> serializer) throws IOException {
            if (file == null) {
                throw new IllegalStateException("Output file is not set.");
            }
            new BTreeFileNode<>(address, null, left, childrenLeft).write(file, serializer);
            return this;
        }

        public NodeSplitter createRightNode(long address, BTreeFileEntrySerializer<K, V> serializer) throws IOException {
            if (file == null) {
                throw new IllegalStateException("Output file is not set.");
            }
            new BTreeFileNode<>(address, null, right, childrenRight).write(file, serializer);
            return this;
        }

        public BTreeFileEntry<K, V> getMedian() {
            return median;
        }
    }
}
