package org.kd.worthlessdb.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.worthlessdb.protocol.Protocol;

import java.io.IOException;
import java.net.Socket;

/**
 * @author kirk
 */
public class TcpNetworkTask implements NetworkTask {

    private static final Log LOG = LogFactory.getLog(TcpNetworkTask.class);

    private Socket socket;

    private Protocol protocol;

    public TcpNetworkTask(Socket socket, Protocol protocol) {
        this.socket = socket;
        this.protocol = protocol;
    }

    @Override
    public void run() {
        LOG.info("Connection established.");
        try {
            protocol.handleInputStream(socket.getInputStream(), socket.getOutputStream());
        } catch (IOException e) {
            LOG.error("Error occurred during opening of input stream.", e);
        }
        LOG.info("Connection closed.");
    }
}
