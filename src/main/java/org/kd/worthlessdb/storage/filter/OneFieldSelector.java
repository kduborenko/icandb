package org.kd.worthlessdb.storage.filter;

import org.json.JSONObject;

/**
 * @author kirk
 */
public class OneFieldSelector implements FieldsSelector {

    private String key;

    public OneFieldSelector(String key) {
        this.key = key;
    }

    @Override
    public JSONObject map(JSONObject jsonObject) {
        return new JSONObject().put(key, jsonObject.opt(key));
    }
}
