package org.kd.icandb.operations;

import org.kd.icandb.ICanDB;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author kirk
 */
public abstract class StorageOperation<T> implements Operation<T> {

    @Autowired
    private ICanDB icandb;

    protected ICanDB getStorage() {
        return icandb;
    }
}
