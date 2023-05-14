package io.github.bucket4j.distributed.proxy.generic.compare_and_swap;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import org.reactivestreams.Publisher;

@Experimental
public interface ReactiveCompareAndSwapOperation {
    Publisher<byte[]> getStateData();

    Publisher<Boolean> compareAndSwap(byte[] originalData, byte[] newData, RemoteBucketState newState);
}
