package org.kd.worthlessdb.operations;

import org.json.JSONObject;
import org.kd.worthlessdb.WorthlessDBException;
import org.springframework.stereotype.Component;

/**
 * @author kirk
 */
@Component("update")
public class UpdateOperation extends StorageOperation {
    @Override
    public Object execute(JSONObject arg) throws WorthlessDBException {
        String collection = arg.getString("$collection");
        int count = getStorage().update(collection, arg.getJSONObject("$query"), arg.getJSONObject("$obj"));
        return new JSONObject().put("$updated", count);
    }
}
