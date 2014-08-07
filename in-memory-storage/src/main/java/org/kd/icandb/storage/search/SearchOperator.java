package org.kd.icandb.storage.search;

import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public interface SearchOperator {

    boolean match(Map<String, ?> query);

}
