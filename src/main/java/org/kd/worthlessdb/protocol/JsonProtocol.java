package org.kd.worthlessdb.protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import java.io.*;

/**
 * @author kirk
 */
@Component
public class JsonProtocol implements Protocol {

    private static final Log LOG = LogFactory.getLog(JsonProtocol.class);

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
        // echo
        pw.println(command);
    }

}
