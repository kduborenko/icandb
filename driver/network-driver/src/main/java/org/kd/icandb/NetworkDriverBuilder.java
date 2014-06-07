package org.kd.icandb;

import org.kd.icandb.json.JsonUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.kd.icandb.utils.MapUtils.get;

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
                        JsonUtils.write(pw, getRequestObject(method, args));
                        Map<String, ?> response
                                = JsonUtils.readMap(br.readLine(), String.class, Object.class);
                        String status = get(response, "$status", String.class);
                        if ("error".equals(status)) {
                            throw new ICanDBException(get(response, "$message", String.class));
                        }
                        return response.get("$res");
                    }
            );
        } catch (IOException e) {
            throw new RuntimeException(e); //todo throw better exception
        }
    }

    private static Map<String, ?> getRequestObject(Method method, Object[] args) {
        Map<String, Object> arg = new HashMap<>();
        for (int i = 0; i < method.getParameters().length; i++) {
            if (args[i] != null) {
                arg.put(method.getParameters()[i]
                                .getAnnotation(ReqParam.class).value(),
                        args[i]);
            }
        }
        Map<String, Object> request = new HashMap<>();
        request.put("$op", method.getName());
        request.put("$arg", arg);
        return request;
    }
}
