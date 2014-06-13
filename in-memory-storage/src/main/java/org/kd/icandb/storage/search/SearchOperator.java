package org.kd.icandb.storage.search;

import java.util.Map;

/**
 * @author kirk
 */
public interface SearchOperator {

    boolean match(Map<String, ?> query);

}
