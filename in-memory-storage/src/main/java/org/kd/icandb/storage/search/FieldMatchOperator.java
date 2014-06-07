package org.kd.icandb.storage.search;

import org.springframework.util.ObjectUtils;

import java.util.Map;

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
    public boolean match(Map<String, ?> jsonObject) {
        return ObjectUtils.nullSafeEquals(jsonObject.get(key), value);
    }
}
