package org.kd.icandb.storage.filter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kirk
 */
public class OneFieldSelector implements FieldsSelector {

    private String key;

    public OneFieldSelector(String key) {
        this.key = key;
    }

    @Override
    public Map<String, ?> map(Map<String, ?> jsonObject) {
        Map<String, Object> res = new HashMap<>();
        res.put(key, jsonObject.get(key));
        return res;
    }
}
