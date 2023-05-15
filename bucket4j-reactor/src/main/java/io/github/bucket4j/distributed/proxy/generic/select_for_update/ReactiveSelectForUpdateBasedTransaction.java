package io.github.bucket4j.distributed.proxy.generic.select_for_update;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import org.reactivestreams.Publisher;

@Experimental
public interface ReactiveSelectForUpdateBasedTransaction<R> {
    Publisher<R> begin();

    Publisher<?> rollback(R resource);

    Publisher<?> commit(R resource);

    Publisher<LockAndGetResult> tryLockAndGet(R resource);

    Publisher<Boolean> tryInsertEmptyData(R resource);

    Publisher<?> update(R resource, byte[] data, RemoteBucketState newState);

    Publisher<?> release(R resource);
}
