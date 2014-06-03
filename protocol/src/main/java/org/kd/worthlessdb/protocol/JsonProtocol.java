package org.kd.worthlessdb.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.kd.worthlessdb.operations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Collections;
import java.util.Map;

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
            for (String command; (command = br.readLine()) != null;) {
                processCommand(command, pw);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processCommand(String command, PrintWriter pw) {
        LOG.debug("Command is received: " + command);
        JSONObject jsonObject = new JSONObject(command);
        String operationName = jsonObject.optString("$op");
        Operation operation = operations.get(operationName);
        if (operation == null) {
            pw.println(new JSONStringer()
                    .object()
                    .key("$status").value("error")
                    .key("$message").value(String.format("Operation '%s' not found.", operationName))
                    .endObject().toString());
            return;
        }
        Object res;
        try {
             res = operation.execute(jsonObject.getJSONObject("$arg"));
        } catch (Exception e) {
            LOG.error("Error during operation execution.", e);
            pw.println(new JSONStringer()
                    .object()
                    .key("$status").value("error")
                    .key("$message").value(e.getMessage())
                    .key("$trace").value(e.getStackTrace())
                    .endObject().toString());
            return;
        }
        pw.println(new JSONStringer()
                .object()
                .key("$status").value("ok")
                .key("$res").value(res)
                .endObject().toString());
    }

}
