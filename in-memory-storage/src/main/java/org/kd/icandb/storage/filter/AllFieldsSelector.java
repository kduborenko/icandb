package org.kd.icandb.storage.filter;

import java.util.Map;

/**
 * @author kirk
 */
public class AllFieldsSelector implements FieldsSelector {

    public static final FieldsSelector INSTANCE = new AllFieldsSelector();

    private AllFieldsSelector() {}

    @Override
    public Map<String, ?> map(Map<String, ?> jsonObject) {
        return jsonObject;
    }
}
