package org.kd.icandb.storage.search;

import java.util.Map;

/**
 * @author kirk
 */
public final class MatchAllOperator implements SearchOperator {

    public static final SearchOperator INSTANCE = new MatchAllOperator();

    private MatchAllOperator() {}

    @Override
    public boolean match(Map<String, ?> query) {
        return true;
    }
}
