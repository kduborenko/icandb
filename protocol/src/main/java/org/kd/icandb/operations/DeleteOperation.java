package org.kd.icandb.operations;

import org.json.JSONObject;
import org.kd.icandb.ICanDBException;
import org.springframework.stereotype.Component;

import static org.kd.icandb.ICanDBConstants.PARAM_COLLECTION;
import static org.kd.icandb.ICanDBConstants.PARAM_QUERY;

/**
 * @author kirk
 */
@Component("delete")
public class DeleteOperation extends StorageOperation<Integer> {
    @Override
    public Integer execute(JSONObject arg) throws ICanDBException {
        String collection = arg.getString(PARAM_COLLECTION);
        return getStorage().delete(collection, arg.getJSONObject(PARAM_QUERY));
    }
}
