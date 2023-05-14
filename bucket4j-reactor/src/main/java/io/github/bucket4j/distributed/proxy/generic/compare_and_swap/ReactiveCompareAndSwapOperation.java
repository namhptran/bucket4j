package io.github.bucket4j.distributed.proxy.generic.compare_and_swap;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import reactor.core.publisher.Mono;

@Experimental
public interface ReactiveCompareAndSwapOperation {
    Mono<byte[]> getStateData();

    Mono<Boolean> compareAndSwap(byte[] originalData, byte[] newData, RemoteBucketState newState);
}
