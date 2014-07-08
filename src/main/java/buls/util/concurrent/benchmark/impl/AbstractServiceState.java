package buls.util.concurrent.benchmark.impl;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractServiceState {
    public static final int CAPACITY = 1000000;

    protected Service service;

    @Setup
    public void startup() {
        getService().start();
    }

    protected abstract Service createService();

    @TearDown
    public void shutdown() {
        try {
            getService().shutdownAndWait();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


    }

    public Service getService() {
        if(service == null) {
            service = createService();
        }
        return service;
    }
}
