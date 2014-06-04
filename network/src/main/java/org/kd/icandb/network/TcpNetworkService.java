package org.kd.icandb.network;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kd.icandb.ICanDBException;
import org.kd.icandb.protocol.Protocol;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author kirk
 */
@Service
public class TcpNetworkService implements NetworkService {

    private static final Log LOG = LogFactory.getLog(TcpNetworkService.class);
    private static final int PORT = Integer.getInteger("network.port", 8978);
    private static final int POOL_SIZE = Integer.getInteger("network.pool_size", 10);

    private ExecutorService executorService;

    private ServerSocket serverSocket;

    @Autowired
    private Protocol protocol;

    @Override
    public void start() throws ICanDBException {
        executorService = Executors.newFixedThreadPool(POOL_SIZE);
        try {
            serverSocket = new ServerSocket(PORT);
        } catch (IOException e) {
            throw new ICanDBException(e);
        }
        new Thread(() -> {
            try {
                for (Socket socket; (socket = serverSocket.accept()) != null;) {
                    executorService.submit(new TcpNetworkTask(socket, protocol));
                }
            } catch (SocketException e) {
                if (!executorService.isShutdown()) {
                    LOG.error("Error during connection receiving.", e);
                }
            } catch (IOException e) {
                LOG.error("Error during connection receiving.", e);
            }
        }).start();
        LOG.info(String.format("Server started listening on port %s", PORT));
    }

    @Override
    public void stop() {
        executorService.shutdown();
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOG.error("Error is occurred during server socket closing.", e);
        }
    }
}
