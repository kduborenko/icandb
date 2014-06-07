package org.kd.icandb.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.icandb.json.JsonUtils;
import org.kd.icandb.operations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.kd.icandb.utils.MapUtils.getMap;

/**
 * @author kirk
 */
@Component
public class JsonProtocol implements Protocol {

    private static final Log LOG = LogFactory.getLog(JsonProtocol.class);

    private final Map<String, Operation> operations;

    @Autowired
    public JsonProtocol(Map<String, Operation> operations) {
        this.operations = Collections.unmodifiableMap(operations);
    }

    @Override
    public void handleInputStream(InputStream inputStream, OutputStream outputStream) {
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
        PrintWriter pw = new PrintWriter(outputStream, true);
        try {
            for (String command; (command = br.readLine()) != null; ) {
                processCommand(command, pw);
            }
        } catch (IOException e) {
            LOG.error("Error during command processing.", e); // todo send response
            throw new RuntimeException(e);
        }
    }

    private void processCommand(String command, PrintWriter pw) throws IOException {
        LOG.debug("Command is received: " + command);
        Map<String, ?> jsonObject
                = JsonUtils.readMap(command, String.class, Object.class);
        String operationName = (String) jsonObject.get("$op");
        Operation operation = operations.get(operationName);
        if (operation == null) {
            JsonUtils.write(pw, new HashMap<String, Object>() {{
                put("$status", "error");
                put("$message", String.format("Operation '%s' not found.", operationName));
            }});
            return;
        }
        final Object res = run(operation, jsonObject, pw);

        if (res == null) {
            return;
        }
        JsonUtils.write(pw, new HashMap<String, Object>() {{
            put("$status", "ok");
            put("$res", res);
        }});
    }

    private Object run(Operation<?> operation, Map<String, ?> jsonObject, PrintWriter pw) throws IOException {
        try {
            return operation.execute(getMap(jsonObject, "$arg", String.class, Object.class));
        } catch (Exception e) {
            LOG.error("Error during operation execution.", e);
            JsonUtils.write(pw, new HashMap<String, Object>() {{
                put("$status", "error");
                put("$message", e.getMessage());
            }});
        }
        return null;
    }

}
