package org.kd.icandb.fs;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BTreeFileStorageTest {

    private BTreeFileEntrySerializer<Long, Void> longVoidSerializer = new BTreeFileEntrySerializer<Long, Void>() {
        @Override
        public boolean inlineEntry() {
            return true;
        }

        @Override
        public void readData(byte[] data, BTreeFileEntry<Long, Void> entry) {
            entry.setKey(entry.getAddress());
        }

        @Override
        public byte[] writeData(BTreeFileEntry<Long, Void> entry) {
            return new byte[0];
        }

        @Override
        public void writeNodeData(BTreeFileEntry<Long, Void> value, ByteBuffer bb) {
            bb.putLong(value == null ? 0 : value.getKey());
        }

        @Override
        public BTreeFileEntry<Long, Void> readNodeData(RandomAccessFile file,
                                                       BTreeFileEntrySerializer<Long, Void> serializer,
                                                       ByteBuffer bb) throws IOException {
            long address = bb.getLong();
            return address == 0 ? null : BTreeFileEntry.forValue(file, serializer, address, null);
        }
    };
    private BTreeFileEntrySerializer<String, Void> stringVoidSerializer = new BTreeFileEntrySerializer<String, Void>() {
        @Override
        public boolean inlineEntry() {
            return false;
        }

        @Override
        public void readData(byte[] data, BTreeFileEntry<String, Void> entry) throws IOException {
            ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data));
            try {
                entry.setKey((String) is.readObject());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] writeData(BTreeFileEntry<String, Void> entry) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(baos);
            os.writeObject(entry.getKey());
            return baos.toByteArray();
        }
    };
    private BTreeFileEntrySerializer<String, String> stringStringSerializer = new BTreeFileEntrySerializer<String, String>() {
        @Override
        public boolean inlineEntry() {
            return false;
        }

        @Override
        public void readData(byte[] data, BTreeFileEntry<String, String> entry) throws IOException {
            ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data));
            try {
                entry.setKey((String) is.readObject());
                entry.setValue((String) is.readObject());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public byte[] writeData(BTreeFileEntry<String, String> entry) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(baos);
            os.writeObject(entry.getKey());
            os.writeObject(entry.getValue());
            return baos.toByteArray();
        }
    };

    @Test
    public void testFileCreation() throws IOException {
        File file = File.createTempFile("test", "tmp");
        //noinspection EmptyTryBlock,UnusedDeclaration
        try (BTreeFileStorage<String, Void> bTreeFileStorage = BTreeFileStorage.create(file, 4, 1024, stringVoidSerializer)) {
            // do nothing
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(32, size);
        assertArrayEquals(toByteArray(1024, 0L, 0L, 1024L, 32L), buffer);
        file.deleteOnExit();
    }

    private static byte[] toByteArray(int size, long... longs) {
        ByteBuffer bb = ByteBuffer.allocate(size);
        Arrays.stream(longs).forEach(bb::putLong);
        return bb.array();
    }

    @Test
    public void testOrder4AddOneItem() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(123L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(112, size);
        assertArrayEquals(toByteArray(
                1024,
                32L, 0L, 1024L, 112L,
                0L, 123L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddTwoItems() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(124L, null));
            assertNull(storage.put(123L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(112, size);
        assertArrayEquals(toByteArray(
                1024,
                32L, 0L, 1024L, 112L,
                0L, 123L, 124L, 0L, 0L, 0L, 0L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddThreeItems() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(124L, null));
            assertNull(storage.put(122L, null));
            assertNull(storage.put(123L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(112, size);
        assertArrayEquals(toByteArray(
                1024,
                32L, 0L, 1024L, 112L,
                0L, 122L, 123L, 124L, 0L, 0L, 0L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddFiveItemsLast1() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(2L, null));
            assertNull(storage.put(3L, null));
            assertNull(storage.put(4L, null));
            assertNull(storage.put(5L, null));
            assertNull(storage.put(1L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertArrayEquals(toByteArray(
                1024,
                192L, 0L, 1024L, 272L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 32L, 112L, 0L, 0L, 0L), buffer);
        assertEquals(272, size);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddFiveItemsLast2() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(1L, null));
            assertNull(storage.put(3L, null));
            assertNull(storage.put(4L, null));
            assertNull(storage.put(5L, null));
            assertNull(storage.put(2L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(272, size);
        assertArrayEquals(toByteArray(
                1024,
                192L, 0L, 1024L, 272L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 32L, 112L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddFiveItemsLast3() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(1L, null));
            assertNull(storage.put(2L, null));
            assertNull(storage.put(4L, null));
            assertNull(storage.put(5L, null));
            assertNull(storage.put(3L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(272, size);
        assertArrayEquals(toByteArray(
                1024,
                192L, 0L, 1024L, 272L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 32L, 112L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddFiveItemsLast4() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(1L, null));
            assertNull(storage.put(2L, null));
            assertNull(storage.put(3L, null));
            assertNull(storage.put(5L, null));
            assertNull(storage.put(4L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(272, size);
        assertArrayEquals(toByteArray(
                1024,
                192L, 0L, 1024L, 272L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 32L, 112L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddFiveItemsLast5() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            assertNull(storage.put(1L, null));
            assertNull(storage.put(2L, null));
            assertNull(storage.put(3L, null));
            assertNull(storage.put(4L, null));
            assertNull(storage.put(5L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(272, size);
        assertArrayEquals(toByteArray(
                1024,
                192L, 0L, 1024L, 272L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 32L, 112L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder2AddFiveItems() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 2, 1024, longVoidSerializer)) {
            assertNull(storage.put(1L, null));
            assertNull(storage.put(2L, null));
            assertNull(storage.put(3L, null));
            assertNull(storage.put(4L, null));
            assertNull(storage.put(5L, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(224L, size);
        assertArrayEquals(toByteArray(
                1024,
                128L, 0L, 1024L, 224L,
                0L, 1L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 0L,
                0L, 2L, 4L, 32L, 80L, 176L,
                0L, 5L, 0L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder2AddSevenItems() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 2, 1024, longVoidSerializer)) {
            LongStream.rangeClosed(1, 7).forEach(i -> storage.put(i, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(368L, size);
        assertArrayEquals(toByteArray(
                1024,
                320L, 0L, 1024L, 368L,
                0L, 1L, 0L, 0L, 0L, 0L,
                0L, 3L, 0L, 0L, 0L, 0L,
                0L, 2L, 0L, 32L, 80L, 0L,
                0L, 5L, 0L, 0L, 0L, 0L,
                0L, 7L, 0L, 0L, 0L, 0L,
                0L, 6L, 0L, 176L, 224L, 0L,
                0L, 4L, 0L, 128L, 272L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testOrder4AddSeventeenItems() throws IOException {
        File file = File.createTempFile("test", "tmp");
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024, longVoidSerializer)) {
            LongStream.rangeClosed(1, 17).forEach(i -> storage.put(i, null));
        }
        InputStream is = new FileInputStream(file);
        byte[] buffer = new byte[1024];
        int size = is.read(buffer);
        assertEquals(752L, size);
        assertArrayEquals(toByteArray(
                1024,
                672L, 0L, 1024L, 752L,
                0L, 1L, 2L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 4L, 5L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 3L, 6L, 0L, 0L, 32L, 112L, 272L, 0L, 0L,
                0L, 7L, 8L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 10L, 11L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 13L, 14L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 16L, 17L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 12L, 15L, 0L, 0L, 352L, 432L, 512L, 0L, 0L,
                0L, 9L, 0L, 0L, 0L, 192L, 592L, 0L, 0L, 0L), buffer);
        file.deleteOnExit();
    }

    @Test
    public void testIterator() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 10, 1024, longVoidSerializer)) {
            List<Long> elements = LongStream.rangeClosed(1, 500).boxed().collect(Collectors.toList());
            Collections.shuffle(elements);
            elements.forEach(i -> storage.put(i, null));
            Iterator<Long> expected = elements.stream().sorted().iterator();
            Iterator<Long> actual = storage.keySet().iterator();
            while (expected.hasNext()) {
                assertTrue(actual.hasNext());
                assertEquals(expected.next(), actual.next());
            }
        }
    }

    @Test
    public void testStringKeys() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<String, Void> storage = BTreeFileStorage.create(file, 2, 1024, stringVoidSerializer)) {
            List<String> elements = LongStream.rangeClosed(1, 10)
                    .boxed()
                    .map(String::valueOf)
                    .collect(Collectors.toList());
            Collections.shuffle(elements);
            elements.forEach(i -> storage.put(i, null));
            Iterator<String> expected = elements.stream().sorted().iterator();
            Iterator<String> actual = storage.keySet().iterator();
            while (expected.hasNext()) {
                assertTrue(actual.hasNext());
                assertEquals(expected.next(), actual.next());
            }
        }
    }

    @Test
    public void testGetMethod() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<String, String> storage = BTreeFileStorage.create(file, 2, 1024, stringStringSerializer)) {
            List<String> elements = LongStream.rangeClosed(1, 10)
                    .boxed()
                    .map(String::valueOf)
                    .collect(Collectors.toList());
            elements.forEach(i -> storage.put(i, "v:" + i));
            assertEquals("v:5", storage.get("5"));
            assertEquals("v:4", storage.get("4"));
            assertNull(storage.get("15"));
        }
    }

    @Test
    public void testContainsKeyMethod() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<String, Void> storage = BTreeFileStorage.create(file, 2, 1024, stringVoidSerializer)) {
            List<String> elements = LongStream.rangeClosed(1, 10)
                    .boxed()
                    .map(String::valueOf)
                    .collect(Collectors.toList());
            elements.forEach(i -> storage.put(i, null));
            assertTrue(storage.containsKey("5"));
            assertTrue(storage.containsKey("4"));
            assertFalse(storage.containsKey("15"));
        }
    }

    @TestFactory
    public Stream<DynamicTest> testPrimitiveRemove() throws IOException {
        List<Long> elements = LongStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        return IntStream.range(0, 10)
                .boxed()
                .map((i) -> {
                    Collections.shuffle(elements);
                    List<Long> add = new ArrayList<>(elements);
                    Collections.shuffle(elements);
                    List<Long> remove = new ArrayList<>(elements);
                    return DynamicTest.dynamicTest(
                            String.format("Add: %s, Remove: %s", add, remove),
                            () -> testAddRemove(
                                    (file) -> BTreeFileStorage.create(file, 10, 1024, longVoidSerializer),
                                    add, remove)
                    );
                });
    }

    @Disabled
    @TestFactory
    public Stream<DynamicTest> test2LevelRemove() throws IOException {
        List<Long> elements = LongStream.rangeClosed(1, 10).boxed().collect(Collectors.toList());
        return IntStream.range(0, 10)
                .boxed()
                .map((i) -> {
                    Collections.shuffle(elements);
                    List<Long> add = new ArrayList<>(elements);
                    Collections.shuffle(elements);
                    List<Long> remove = new ArrayList<>(elements);
                    return DynamicTest.dynamicTest(
                            String.format("Add: %s, Remove: %s", add, remove),
                            () -> testAddRemove(
                                    (file) -> BTreeFileStorage.create(file, 4, 1024, longVoidSerializer),
                                    add, remove)
                    );
                });
    }

    @Disabled
    @TestFactory
    public Stream<DynamicTest> test3LevelRemove() throws IOException {
        List<Long> elements = LongStream.rangeClosed(1, 22).boxed().collect(Collectors.toList());
        return IntStream.range(0, 10)
                .boxed()
                .map((i) -> {
                    Collections.shuffle(elements);
                    List<Long> add = new ArrayList<>(elements);
                    Collections.shuffle(elements);
                    List<Long> remove = new ArrayList<>(elements);
                    return DynamicTest.dynamicTest(
                            String.format("Add: %s, Remove: %s", add, remove),
                            () -> testAddRemove(
                                    (file) -> BTreeFileStorage.create(file, 4, 1024, longVoidSerializer),
                                    add, remove)
                    );
                });
    }

    private void testAddRemove(
            StorageFactory<Long, Void> storageFactory,
            List<Long> add,
            List<Long> remove) throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<Long, Void> storage = storageFactory.create(file)) {
            add.forEach(i -> storage.put(i, null));
            assertEquals(add.size(), storage.size());
            long removedCount = remove.stream()
                    .map(i -> storage.remove(i, null))
                    .filter(b -> b)
                    .count();
            assertEquals(remove.size(), removedCount);
            assertEquals(0, storage.size());
            assertFalse(storage.keySet().iterator().hasNext());
        }
    }

    interface StorageFactory<K, V> {
        BTreeFileStorage<K, V> create(File file) throws IOException;
    }

}