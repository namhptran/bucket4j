package io.github.bucket4j.distributed.proxy.generic.pessimistic_locking;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import org.reactivestreams.Publisher;

@Experimental
public interface ReactiveLockBasedTransaction<R> {
    Publisher<R> begin();
    Publisher<?> rollback(R resource);
    Publisher<?> commit(R resource);
    Publisher<byte[]> lockAndGet(R resource);
    Publisher<?> unlock(R resource);
    Publisher<?> create(R resource, byte[] data, RemoteBucketState state);
    Publisher<?> update(R resource, byte[] data, RemoteBucketState state);
    Publisher<?> release(R resource);
}
