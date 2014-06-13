package org.kd.icandb.storage.search;

import java.util.List;
import java.util.Map;

/**
 * @author kirk
 */
public class OrOperator extends SpecialSearchOperator<List<SearchOperator>> {

    public OrOperator(List<SearchOperator> params) {
        super(params);
    }

    @Override
    public boolean match(Map<String, ?> jsonObject) {
        for (SearchOperator operator : getParams()) {
            if (operator.match(jsonObject)) {
                return true;
            }
        }
        return false;
    }
}
