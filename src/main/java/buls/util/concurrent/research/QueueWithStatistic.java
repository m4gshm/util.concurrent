package buls.util.concurrent.research;

import java.io.PrintStream;
import java.util.Queue;

/**
 * Created by Alex on 25.06.2014.
 */
public interface QueueWithStatistic<E> extends Queue<E> {
    void printStatistic(PrintStream printStream);
}
