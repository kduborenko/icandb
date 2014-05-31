package org.kd.worthlessdb.operations.search;

import org.json.JSONObject;

/**
 * @author kirk
 */
public final class MatchAllOperator implements SearchOperator {

    public static final SearchOperator INSTANCE = new MatchAllOperator();

    private MatchAllOperator() {}

    @Override
    public boolean match(JSONObject jsonObject) {
        return true;
    }
}
