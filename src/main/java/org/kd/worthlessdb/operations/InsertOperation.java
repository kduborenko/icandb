package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.storage.StorageException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("insert")
public class InsertOperation extends StorageOperation {

    @Override
    public Object execute(JSONObject arg) throws StorageException {
        String collection = arg.getString("$collection");
        String id = getStorage().insert(collection, arg.getJSONObject("$obj"));
        return new JSONObject().put("$inserted", 1).put("$id", id);
    }

}
