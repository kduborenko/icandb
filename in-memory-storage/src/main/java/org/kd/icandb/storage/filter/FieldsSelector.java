package org.kd.icandb.storage.filter;

import java.util.Map;

/**
 * @author kirk
 */
public interface FieldsSelector {

    Map<String, ?> map(Map<String, ?> jsonObject);

}
