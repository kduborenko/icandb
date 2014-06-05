package org.kd.icandb;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kirk
 */
public class NetworkDriverBuilder implements ICanDBDriverBuilder {

    private static final Pattern CONNECTION_STRING_PATTERN
            = Pattern.compile("^(?<hostname>.+?):(?<port>\\d{1,5})/$");

    @Override
    public String getProtocol() {
        return "net";
    }

    @Override
    public ICanDB getDriver(String connectionString) {
        Matcher matcher = CONNECTION_STRING_PATTERN.matcher(connectionString);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format(
                    "Connection string '%s' cannot be parsed.", connectionString));
        }

        return getNetworkDriver(matcher);
    }

    private static ICanDB getNetworkDriver(Matcher matcher) {
        try {
            Socket socket = new Socket(matcher.group("hostname"), Integer.valueOf(matcher.group("port")));
            BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
            return (ICanDB) Proxy.newProxyInstance(
                    ClassLoader.getSystemClassLoader(),
                    new Class[]{ICanDB.class},
                    (proxy, method, args) -> {
                        pw.println(getRequestObject(method, args));
                        JSONObject response = new JSONObject(br.readLine());
                        String status = response.getString("$status");
                        if ("error".equals(status)) {
                            throw new ICanDBException(response.getString("$message"));
                        }
                        return response.get("$res");
                    }
            );
        } catch (IOException e) {
            throw new RuntimeException(e); //todo throw better exception
        }
    }

    private static JSONObject getRequestObject(Method method, Object[] args) {
        JSONObject request = new JSONObject();
        for (int i = 0; i < method.getParameters().length; i++) {
            request.put(
                    method.getParameters()[i]
                            .getAnnotation(ReqParam.class).value(),
                    args[i]);
        }
        return new JSONObject().put("$op", method.getName()).put("$arg", request);
    }
}
