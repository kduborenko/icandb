package org.kd.icandb.storage.selectors;

import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public class AllFieldsSelector implements FieldsSelector {

    public static final FieldsSelector INSTANCE = new AllFieldsSelector();

    private AllFieldsSelector() {}

    @Override
    public Map<String, ?> map(Map<String, ?> jsonObject) {
        return jsonObject;
    }
}
