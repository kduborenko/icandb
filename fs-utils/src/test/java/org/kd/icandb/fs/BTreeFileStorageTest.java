package org.kd.icandb.fs;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.LongStream;

import static org.junit.Assert.*;

public class BTreeFileStorageTest {

    @Test
    public void testFileCreation() throws IOException {
        File file = File.createTempFile("test", "tmp");
        //noinspection EmptyTryBlock,UnusedDeclaration
        try (BTreeFileStorage<String, Void> bTreeFileStorage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 2, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 2, 1024)) {
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
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 4, 1024)) {
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

    @Ignore("Turn on after serializers are implemented.")
    @Test
    public void testIterator() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<Long, Void> storage = BTreeFileStorage.create(file, 10, 1024)) {
            List<Long> elements = LongStream.rangeClosed(1, 500).collect(ArrayList::new,
                    (list, e) -> list.add(0, e),
                    (l1, l2) -> l1.addAll(0, l2));
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
    public void testStringReferences() throws IOException {
        File file = File.createTempFile("test", "tmp");
        file.deleteOnExit();
        try (BTreeFileStorage<String, Void> storage = BTreeFileStorage.create(file, 2, 1024)) {
            List<String> elements = LongStream.rangeClosed(1, 10)
                    .boxed()
                    .map(String::valueOf)
                    .collect(ArrayList::new,
                            (list, e) -> list.add(0, e),
                            (l1, l2) -> l1.addAll(0, l2));
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

}