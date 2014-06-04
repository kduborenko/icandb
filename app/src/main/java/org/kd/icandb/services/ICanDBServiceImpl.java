package org.kd.icandb.services;

import org.kd.icandb.ICanDBException;
import org.kd.icandb.network.NetworkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author kirk
 */
@Service
public class ICanDBServiceImpl implements ICanDBService {

    @Autowired
    private NetworkService networkService;

    @Override
    public void start() throws ICanDBException {
        networkService.start();
    }

    @Override
    public void stop() {
        networkService.stop();
    }

}
