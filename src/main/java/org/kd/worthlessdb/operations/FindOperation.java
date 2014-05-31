package org.kd.worthlessdb.operations;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author kirk
 */
@Component("find")
public class FindOperation extends StorageOperation {
    @Override
    public Object execute(JSONObject arg) {
        String collection = arg.getString("$collection");
        List<JSONObject> result = getStorage().find(collection, arg.getJSONObject("$query"));
        return new JSONArray(result);
    }
}
