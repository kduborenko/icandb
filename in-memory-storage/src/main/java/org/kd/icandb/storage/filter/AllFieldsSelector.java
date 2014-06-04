package org.kd.icandb.storage.filter;

import org.json.JSONObject;

/**
 * @author kirk
 */
public class AllFieldsSelector implements FieldsSelector {

    public static final FieldsSelector INSTANCE = new AllFieldsSelector();

    private AllFieldsSelector() {}

    @Override
    public JSONObject map(JSONObject jsonObject) {
        return jsonObject;
    }
}
