package org.kd.icandb.storage.search;

import java.util.List;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public class OrOperator extends SpecialSearchOperator<List<SearchOperator>> {

    public OrOperator(List<SearchOperator> params) {
        super(params);
    }

    @Override
    public boolean match(Map<String, ?> query) {
        for (SearchOperator operator : getParams()) {
            if (operator.match(query)) {
                return true;
            }
        }
        return false;
    }
}
