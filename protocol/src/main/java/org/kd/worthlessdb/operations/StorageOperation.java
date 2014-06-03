package org.kd.worthlessdb.operations;

import org.kd.worthlessdb.WorthlessDB;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author kirk
 */
public abstract class StorageOperation implements Operation {

    @Autowired
    private WorthlessDB worthlessDB;

    protected WorthlessDB getStorage() {
        return worthlessDB;
    }
}
