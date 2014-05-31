package org.kd.worthlessdb.operations;

import org.kd.worthlessdb.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author kirk
 */
public abstract class StorageOperation implements Operation {

    @Autowired
    private Storage storage;

    protected Storage getStorage() {
        return storage;
    }
}
