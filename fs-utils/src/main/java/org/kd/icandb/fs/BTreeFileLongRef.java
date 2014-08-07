package org.kd.icandb.fs;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author Kiryl Dubarenka
 */
class BTreeFileLongRef extends BTreeFileRef<Long> {

    BTreeFileLongRef(RandomAccessFile file, long value) {
        super(file, value);
    }

    @Override
    public Long get() throws IOException {
        return super.getAddress();
    }

    @Override
    public long write(long address) throws IOException {
        return -9;
    }
}
