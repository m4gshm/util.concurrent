package buls.util.concurrent.benchmark.impl;

/**
 * Created by AlSBulgakov on 26.05.2014.
 */
public abstract class AbstractService implements Service {

    protected AbstractExecutor executor;

    protected AbstractService(AbstractExecutor executor) {
        this.executor = executor;
    }

    @Override
    public final void sendMessage() throws InterruptedException {
        doInvoke();
    }

    protected abstract void doInvoke() throws InterruptedException;

}
