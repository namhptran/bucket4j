package io.github.bucket4j.distributed.proxy.generic.pessimistic_locking;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import org.reactivestreams.Publisher;

@Experimental
public interface ReactiveLockBasedTransaction<R> {
    Publisher<R> begin();
    Publisher<Void> rollback(R resource);
    Publisher<Void> commit(R resource);
    Publisher<byte[]> lockAndGet(R resource);
    Publisher<Void> unlock(R resource);
    Publisher<Void> create(R resource, byte[] data, RemoteBucketState state);
    Publisher<Void> update(R resource, byte[] data, RemoteBucketState state);
    Publisher<Void> release(R resource);
}
