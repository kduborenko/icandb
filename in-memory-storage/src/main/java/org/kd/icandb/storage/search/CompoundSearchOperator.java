package org.kd.icandb.storage.search;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public class CompoundSearchOperator implements SearchOperator {

    private List<SearchOperator> searchOperators;

    public CompoundSearchOperator(List<SearchOperator> searchOperators) {
        this.searchOperators = Collections.unmodifiableList(searchOperators);
    }

    @Override
    public boolean match(Map<String, ?> query) {
        for (SearchOperator operator : searchOperators) {
            if (!operator.match(query)) {
                return false;
            }
        }
        return true;
    }
}
