package org.kd.icandb.storage;

import java.util.List;
import java.util.Map;

/**
 * @author kirk
 */
public interface Index {

    Map<String, ?> match(Map<String, ?> fullQuery);

    List<Map<String, ?>> find(Map<String, ?> query);

}
