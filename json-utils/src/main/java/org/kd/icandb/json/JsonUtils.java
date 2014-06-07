package org.kd.icandb.json;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;

/**
 * @author kirk
 */
public final class JsonUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonUtils() {}

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> readMap(String jsonLine, Class<K> keyType,
                                           Class<V> valueType) throws IOException {
        return Collections.checkedMap(MAPPER.readValue(jsonLine, Map.class), keyType, valueType);
    }

    public static void write(PrintWriter pw, Object object) throws IOException {
        pw.println(MAPPER.writeValueAsString(object));  // todo implement streaming
    }
}
