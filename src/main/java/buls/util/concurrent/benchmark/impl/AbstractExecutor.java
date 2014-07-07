package buls.util.concurrent.benchmark.impl;

/**
 * Created by Bulgakov Alex on 25.05.14.
 */
public interface AbstractExecutor extends Runnable {
    void shutdown();
}
