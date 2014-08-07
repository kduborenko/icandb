package org.kd.icandb.storage.search;

import java.util.Map;
import java.util.function.Function;

/**
 * @author Kiryl Dubarenka
 */
public class FieldMatchOperator implements SearchOperator {

    private String key;
    private Function<Object, Boolean> matcher;

    public FieldMatchOperator(String key, Function<Object, Boolean> matcher) {
        this.key = key;
        this.matcher = matcher;
    }

    @Override
    public boolean match(Map<String, ?> query) {
        return matcher.apply(query.get(key));
    }
}
