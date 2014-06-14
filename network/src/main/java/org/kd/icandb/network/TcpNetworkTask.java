package org.kd.icandb.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.icandb.protocol.Protocol;

import java.io.IOException;
import java.net.Socket;

/**
 * @author kirk
 */
public class TcpNetworkTask implements NetworkTask {

    public static final ThreadLocal<Socket> SOCKET = new ThreadLocal<>();
    private static final Log LOG = LogFactory.getLog(TcpNetworkTask.class);

    private Socket socket;

    private Protocol protocol;

    public TcpNetworkTask(Socket socket, Protocol protocol) {
        this.socket = socket;
        this.protocol = protocol;
    }

    @Override
    public void run() {
        try {
            SOCKET.set(socket);
            LOG.info("Connection established.");
            protocol.handleInputStream(socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            LOG.error("Error occurred during opening of input stream.", e);
        } finally {
            LOG.info("Connection closed.");
            SOCKET.remove();
        }
    }
}
