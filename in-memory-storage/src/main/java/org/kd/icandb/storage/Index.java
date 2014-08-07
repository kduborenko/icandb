package org.kd.icandb.storage;

import java.util.List;
import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public interface Index {

    Map<String, ?> match(Map<String, ?> fullQuery);

    List<Map<String, ?>> find(Map<String, ?> query);

    String getName();

    int size();
}
