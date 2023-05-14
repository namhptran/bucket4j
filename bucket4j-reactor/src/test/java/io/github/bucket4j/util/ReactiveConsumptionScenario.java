package io.github.bucket4j.util;

import io.github.bucket4j.distributed.ReactiveBucketProxy;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.junit.Assert.assertTrue;

public class ReactiveConsumptionScenario {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final ReactiveConsumerThread[] consumers;
    private final long initializationNanotime;
    private final double permittedRatePerSecond;

    public ReactiveConsumptionScenario(int threadCount, long workTimeNanos, Supplier<ReactiveBucketProxy> bucketSupplier, Function<ReactiveBucketProxy, Long> action, double permittedRatePerSecond) {
        this.startLatch = new CountDownLatch(threadCount);
        this.endLatch = new CountDownLatch(threadCount);
        this.consumers = new ReactiveConsumerThread[threadCount];
        this.initializationNanotime = System.nanoTime();
        this.permittedRatePerSecond = permittedRatePerSecond;
        ReactiveBucketProxy bucket = bucketSupplier.get();
        for (int i = 0; i < threadCount; i++) {
            this.consumers[i] = new ReactiveConsumerThread(startLatch, endLatch, bucket, workTimeNanos, action);
        }
    }

    public void executeAndValidateRate() throws Exception {
        for (ReactiveConsumerThread consumer : consumers) {
            consumer.start();
        }
        endLatch.await();

        long durationNanos = System.nanoTime() - initializationNanotime;
        long consumed = 0;
        for (ReactiveConsumerThread consumer : consumers) {
            if (consumer.getException() != null) {
                throw consumer.getException();
            } else {
                consumed += consumer.getConsumed();
            }
        }

        double actualRatePerSecond = (double) consumed * 1_000_000_000.0d / durationNanos;
        System.out.println("Consumed " + consumed + " tokens in the "
                + durationNanos + " nanos, actualRatePerSecond=" + Formatter.format(actualRatePerSecond)
                + ", permitted rate=" + Formatter.format(permittedRatePerSecond));

        String msg = "Actual rate " + Formatter.format(actualRatePerSecond) + " is greater then permitted rate " + Formatter.format(permittedRatePerSecond);
        assertTrue(msg, actualRatePerSecond <= permittedRatePerSecond + 0.02);
    }
}
