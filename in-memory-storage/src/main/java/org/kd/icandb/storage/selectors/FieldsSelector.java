package org.kd.icandb.storage.selectors;

import java.util.Map;

/**
 * @author Kiryl Dubarenka
 */
public interface FieldsSelector {

    Map<String, ?> map(Map<String, ?> jsonObject);

}
