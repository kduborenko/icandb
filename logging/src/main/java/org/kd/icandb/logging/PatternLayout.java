package org.kd.icandb.logging;

import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.helpers.PatternParser;
import org.apache.log4j.spi.LoggingEvent;
import org.kd.icandb.network.TcpNetworkTask;

import java.net.Socket;

/**
 * @author kirk
 */
public class PatternLayout extends org.apache.log4j.PatternLayout {

    @Override
    protected PatternParser createPatternParser(String pattern) {
        return new NetworkInfoSupportPatternParser(pattern);
    }

    private static class NetworkInfoSupportPatternParser extends PatternParser {
        public NetworkInfoSupportPatternParser(String pattern) {
            super(pattern);
        }

        @Override
        protected void finalizeConverter(char c) {
            if (c == 'N') {
                currentLiteral.setLength(0);
                addConverter(new NetworkInfoSupportPatternConverter());
            } else {
                super.finalizeConverter(c);
            }
        }
    }

    private static class NetworkInfoSupportPatternConverter extends PatternConverter {
        @Override
        protected String convert(LoggingEvent event) {
            Socket socket = TcpNetworkTask.SOCKET.get();
            return socket == null ? "---" : socket.getInetAddress().getHostAddress();
        }
    }
}
