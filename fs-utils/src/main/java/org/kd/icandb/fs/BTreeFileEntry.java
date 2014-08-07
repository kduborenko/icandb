package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileEntry<T extends Comparable<T>> {

    private final BTreeFileEntry<T> parent;
    private final long address;
    private final BTreeFileRef<T>[] values;
    private final long[] children;
    private final Class<T> type;

    public BTreeFileEntry(long address, BTreeFileEntry<T> parent, BTreeFileRef<T>[] values, long[] children, Class<T> type) {
        assert values.length + 1 == children.length;
        this.parent = parent;
        this.address = address;
        this.values = values;
        this.children = children;
        this.type = type;
    }

    public void write(RandomAccessFile file) throws IOException {
        file.seek(address);
        ByteBuffer bb = ByteBuffer.allocate(getBinaryDataSize(values.length));
        bb.putLong(0L); //todo remove
        for (BTreeFileRef<T> value : values) {
            bb.putLong(value == null ? 0 : value.getAddress());
        }
        for (long child : children) {
            bb.putLong(child);
        }
        file.write(bb.array());
    }

    public static int getBinaryDataSize(int order) {
        return 8 * (2 * order + 1) + 8;
    }

    public static <T extends Comparable<T>> BTreeFileEntry<T> read(RandomAccessFile file, Class<T> type,
                                                                long offset, int order, BTreeFileEntry<T> parent) throws IOException {
        file.seek(offset);
        byte[] buffer = new byte[getBinaryDataSize(order)];
        file.read(buffer);
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        bb.getLong(); // todo remove read "parent"
        @SuppressWarnings("unchecked") BTreeFileRef<T>[] values = new BTreeFileRef[order];
        for (int i = 0; i < values.length; i++) {
            long address = bb.getLong();
            values[i] = address == 0 ? null : BTreeFileRef.forAddress(file, address, type);
        }
        @SuppressWarnings("unchecked") long[] children = new long[order + 1];
        for (int i = 0; i < children.length; i++) {
            children[i] = bb.getLong();
        }
        return new BTreeFileEntry<>(offset, parent, values, children, type);
    }

    public void add(BTreeFileRef<T> t, long rightChild) throws IOException {
        int position = findPosition(t.get());
        System.arraycopy(values, position, values, position + 1, values.length - position - 1);
        values[position] = t;
        if (!isLeaf()) {
            System.arraycopy(children, position + 1, children, position + 2, children.length - position - 2);
            children[position + 1] = rightChild;
        }
    }

    public EntrySplitter splitEntry(BTreeFileRef<T> newItem, long rightChild) throws IOException {
        int order = values.length;
        int medianPosition = (order + 1) / 2;
        int pos = findPosition(newItem.get());
        BTreeFileRef<T> median;
        @SuppressWarnings("unchecked") BTreeFileRef<T>[] left = new BTreeFileRef[order];
        @SuppressWarnings("unchecked") BTreeFileRef<T>[] right = new BTreeFileRef[order];
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
        return new EntrySplitter(left, right, childrenLeft, childrenRight, median);
    }

    public int size() {
        // todo optimize
        return (int) Arrays.stream(values).filter(v -> v != null).count();
    }

    public boolean isFull() {
        return values.length == size();
    }

    public int findPosition(T t) throws IOException {
        // todo binary search
        for (int i = 0; i < values.length; i++) {
            BTreeFileRef<T> v = values[i];
            if (v == null || v.get().compareTo(t) > 0) {
                return i;
            }
        }
        return values.length;
    }

    public BTreeFileRef<T> getValue(int pos) {
        return values[pos];
    }

    public long getAddress() {
        return address;
    }

    public BTreeFileEntry<T> getParent() {
        return parent;
    }

    public boolean isLeaf() {
        return this.children[0] == 0L;
    }

    public BTreeFileEntry<T> resolveChild(RandomAccessFile file, int position) throws IOException {
        return read(file, type, children[position], this.values.length, this);
    }

    public int getDepth() {
        return parent != null ? parent.getDepth() + 1 : 1;
    }

    public class EntrySplitter {

        private final BTreeFileRef<T>[] left;
        private final BTreeFileRef<T>[] right;
        private final long[] childrenLeft;
        private final long[] childrenRight;
        private final BTreeFileRef<T> median;

        private RandomAccessFile file;

        public EntrySplitter(BTreeFileRef<T>[] left, BTreeFileRef<T>[] right, long[] childrenLeft, long[] childrenRight, BTreeFileRef<T> median) {
            this.left = left;
            this.right = right;
            this.childrenLeft = childrenLeft;
            this.childrenRight = childrenRight;
            this.median = median;
        }

        public EntrySplitter setOutputFile(RandomAccessFile file) {
            if (this.file != null) {
                throw new IllegalStateException("Output file is already set.");
            }
            this.file = file;
            return this;
        }

        public EntrySplitter createLeftEntry(long address) throws IOException {
            if (file == null) {
                throw new IllegalStateException("Output file is not set.");
            }
            new BTreeFileEntry<>(address, null, left, childrenLeft, type).write(file);
            return this;
        }

        public EntrySplitter createRightEntry(long address) throws IOException {
            if (file == null) {
                throw new IllegalStateException("Output file is not set.");
            }
            new BTreeFileEntry<>(address, null, right, childrenRight, type).write(file);
            return this;
        }

        public BTreeFileRef<T> getMedian() {
            return median;
        }
    }
}
