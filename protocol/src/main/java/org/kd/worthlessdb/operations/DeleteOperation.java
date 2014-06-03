package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("delete")
public class DeleteOperation extends StorageOperation {
    @Override
    public Object execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString("$collection");
        int count = getStorage().delete(collection, arg.getJSONObject("$query"));
        return new JSONObject().put("$deleted", count);
    }
}
