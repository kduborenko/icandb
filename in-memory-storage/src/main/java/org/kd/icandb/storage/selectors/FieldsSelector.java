package org.kd.icandb.storage.selectors;

import java.util.Map;

/**
 * @author kirk
 */
public interface FieldsSelector {

    Map<String, ?> map(Map<String, ?> jsonObject);

}
