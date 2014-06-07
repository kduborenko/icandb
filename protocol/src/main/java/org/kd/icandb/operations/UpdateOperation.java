package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import static org.kd.icandb.ICanDBConstants.*;

/**
 * @author kirk
 */
@Component("update")
public class UpdateOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(PARAM_COLLECTION);
        return getStorage().update(collection,
                arg.getJSONObject(PARAM_QUERY),
                arg.getJSONObject(PARAM_OBJ));
    }
}
