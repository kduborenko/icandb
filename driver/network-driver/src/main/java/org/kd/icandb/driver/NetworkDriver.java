package org.kd.icandb.driver;

import org.kd.icandb.ICanDBException;
import org.kd.icandb.json.JsonUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import static org.kd.icandb.utils.MapUtils.get;

/**
 * @author kirk
 */
public class NetworkDriver implements AutoCloseable {

    private final Socket socket;
    private final BufferedReader br;
    private final PrintWriter pw;

    public NetworkDriver(Socket socket) throws IOException {
        this.socket = socket;
        this.br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.pw = new PrintWriter(socket.getOutputStream(), true);
    }

    protected Object send(String operation, Object arg) throws ICanDBException {
        Map<String, Object> request = new HashMap<>();
        request.put("$op", operation);
        request.put("$arg", arg);
        try {
            JsonUtils.write(pw, request);
            Map<String, ?> response
                    = JsonUtils.readMap(br.readLine(), String.class, Object.class);
            String status = get(response, "$status", String.class);
            if ("error".equals(status)) {
                throw new ICanDBException(get(response, "$message", String.class));
            }
            return response.get("$res");
        } catch (IOException e) {
            throw new ICanDBException(e);
        }
    }

    protected static void addArgument(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    @Override
    public void close() throws Exception {
        br.close();
        pw.close();
        socket.close();
    }
}
