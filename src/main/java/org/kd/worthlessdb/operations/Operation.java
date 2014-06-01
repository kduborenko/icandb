package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.storage.StorageException;

/**
 * @author kirk
 */
public interface Operation {
    Object execute(JSONObject arg) throws StorageException;
}
