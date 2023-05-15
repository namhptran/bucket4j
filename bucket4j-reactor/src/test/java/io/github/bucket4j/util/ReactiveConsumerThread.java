package io.github.bucket4j.util;

import io.github.bucket4j.distributed.ReactiveBucketProxy;

import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

public class ReactiveConsumerThread extends Thread {
    private final CountDownLatch startLatch;
    private final CountDownLatch endLatch;
    private final ReactiveBucketProxy bucket;
    private final long workTimeNanos;
    private final Function<ReactiveBucketProxy, Long> action;
    private long consumed;
    private Exception exception;

    public ReactiveConsumerThread(CountDownLatch startLatch, CountDownLatch endLatch, ReactiveBucketProxy bucket, long workTimeNanos, Function<ReactiveBucketProxy, Long> action) {
        this.startLatch = startLatch;
        this.endLatch = endLatch;
        this.bucket = bucket;
        this.workTimeNanos = workTimeNanos;
        this.action = action;
    }

    @Override
    public void run() {
        try {
            startLatch.countDown();
            startLatch.await();
            long endNanoTime = System.nanoTime() + workTimeNanos;
            while(true) {
                if (System.nanoTime() >= endNanoTime) {
                    return;
                }
                consumed += action.apply(bucket);
            }
        } catch (Exception e) {
            exception.printStackTrace();
            exception = e;
        } finally {
            endLatch.countDown();
        }
    }

    public Exception getException() {
        return exception;
    }

    public long getConsumed() {
        return consumed;
    }
}
