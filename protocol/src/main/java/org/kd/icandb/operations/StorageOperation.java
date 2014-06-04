package org.kd.icandb.operations;

import org.kd.icandb.WorthlessDB;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author kirk
 */
public abstract class StorageOperation<T> implements Operation<T> {

    @Autowired
    private WorthlessDB worthlessDB;

    protected WorthlessDB getStorage() {
        return worthlessDB;
    }
}
