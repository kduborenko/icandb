package org.kd.icandb.storage.search;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author kirk
 */
public class CompoundSearchOperator implements SearchOperator {

    private List<SearchOperator> searchOperators;

    public CompoundSearchOperator(List<SearchOperator> searchOperators) {
        this.searchOperators = Collections.unmodifiableList(searchOperators);
    }

    @Override
    public boolean match(Map<String, ?> jsonObject) {
        for (SearchOperator operator : searchOperators) {
            if (!operator.match(jsonObject)) {
                return false;
            }
        }
        return true;
    }
}
