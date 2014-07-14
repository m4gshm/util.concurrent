package buls.util.concurrent.benchmark.impl;

import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

import java.util.Queue;

/**
 * Created by Bulgakov Alex on 14.06.2014.
 */
public abstract class AbstractServiceState {
    public abstract Service createService(Queue<Runnable> queue, AbstractExecutor executor);
}
