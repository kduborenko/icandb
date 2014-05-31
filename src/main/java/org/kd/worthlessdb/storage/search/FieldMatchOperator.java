package org.kd.worthlessdb.storage.search;

import org.json.JSONObject;
import org.springframework.util.ObjectUtils;

/**
 * @author kirk
 */
public class FieldMatchOperator implements SearchOperator {

    private String key;
    private Object value;

    public FieldMatchOperator(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean match(JSONObject jsonObject) {
        return ObjectUtils.nullSafeEquals(jsonObject.get(key), value);
    }
}
