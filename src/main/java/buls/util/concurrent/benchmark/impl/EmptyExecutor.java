package buls.util.concurrent.benchmark.impl;

import buls.util.concurrent.benchmark.impl.AbstractExecutor;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Alex on 06.07.2014.
 */
public class EmptyExecutor implements AbstractExecutor {

    @Override
    public void shutdown() {
    }

    @Override
    public void run() {
    }
}
