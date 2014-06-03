package org.kd.worthlessdb.operations;

import org.kd.worthlessdb.WorthlessDB;
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
