package org.kd.icandb;

import org.json.JSONObject;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.*;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author kirk
 */
public final class ICanDBDriver {

    private static final Pattern CONNECTION_STRING_PROTOCOL_PATTERN
            = Pattern.compile("^(?<protocol>\\w+)://(?<parametersString>.*)$");

    private static final Map<String, Pattern> CONNECTION_STRING_PATTERNS
            = Collections.unmodifiableMap(new HashMap<String, Pattern>() {
        {
            put("net", Pattern.compile("^(?<hostname>.+?):(?<port>\\d{1,5})/$"));
            put("mem", Pattern.compile("^$"));
        }
    });

    private static final Map<String, Function<Matcher, ICanDB>> DRIVER_BUILDERS
            = Collections.unmodifiableMap(new HashMap<String, Function<Matcher, ICanDB>>() {
        {
            put("net", ICanDBDriver::getNetworkDriver);
            put("mem", ICanDBDriver::getInMemoryDriver);
        }
    });

    private ICanDBDriver() {}

    public static ICanDB getDriver(String connectionString) {
        Matcher matcher = CONNECTION_STRING_PROTOCOL_PATTERN.matcher(connectionString);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format(
                    "Connection string '%s' cannot be parsed.", connectionString));
        }

        String protocol = matcher.group("protocol");
        String parametersString = matcher.group("parametersString");

        if (!CONNECTION_STRING_PATTERNS.containsKey(protocol)) {
            throw new IllegalArgumentException(String.format(
                    "Protocol '%s' is not supported.", protocol));
        }

        matcher = CONNECTION_STRING_PATTERNS.get(protocol).matcher(parametersString);

        if (!matcher.find()) {
            throw new IllegalArgumentException(String.format(
                    "Connection string '%s' cannot be parsed.", connectionString));
        }

        if (!DRIVER_BUILDERS.containsKey(protocol)) {
            throw new IllegalArgumentException(String.format(
                    "Protocol '%s' is not supported.", protocol));
        }
        return DRIVER_BUILDERS.get(protocol).apply(matcher);
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

    private static ICanDB getInMemoryDriver(Matcher matcher) {
        return new AnnotationConfigApplicationContext(
                ICanDBDriver.class.getPackage().getName()
        ).getBean(ICanDB.class);
    }

}
