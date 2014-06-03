package org.kd.worthlessdb.services;

import org.kd.worthlessdb.WorthlessDBException;
import org.kd.worthlessdb.network.NetworkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @author kirk
 */
@Service
public class WorthlessDbServiceImpl implements WorthlessDbService {

    @Autowired
    private NetworkService networkService;

    @Override
    public void start() throws WorthlessDBException {
        networkService.start();
    }

    @Override
    public void stop() {
        networkService.stop();
    }

}
