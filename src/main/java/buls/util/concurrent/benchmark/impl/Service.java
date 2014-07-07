package buls.util.concurrent.benchmark.impl;

import java.io.PrintStream;

/**
 * Created by AlSBulgakov on 26.05.2014.
 */
public interface Service {
    void sendMessage() throws InterruptedException;

    void shutdownAndWait() throws InterruptedException;

    void printStatistic(PrintStream ps);

    void start();

}
