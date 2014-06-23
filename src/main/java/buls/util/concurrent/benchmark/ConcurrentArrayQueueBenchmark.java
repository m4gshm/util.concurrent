package buls.util.concurrent.benchmark;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

/**
 * Created by Alex on 21.06.2014.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@Measurement(timeUnit = TimeUnit.SECONDS)
public class ConcurrentArrayQueueBenchmark {
}
