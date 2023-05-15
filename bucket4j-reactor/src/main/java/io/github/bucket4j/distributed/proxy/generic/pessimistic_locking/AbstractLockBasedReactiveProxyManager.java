package io.github.bucket4j.distributed.proxy.generic.pessimistic_locking;

import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.proxy.AbstractReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.proxy.generic.GenericEntry;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import io.github.bucket4j.distributed.remote.Request;
import reactor.core.publisher.Mono;

public abstract class AbstractLockBasedReactiveProxyManager<K, R> extends AbstractReactiveProxyManager<K> {
    private static final byte[] NO_DATA = new byte[0];


    protected AbstractLockBasedReactiveProxyManager(ClientSideConfig clientSideConfig) {
        super(injectTimeClock(clientSideConfig));
    }

    @Override
    public <T> Mono<CommandResult<T>> executeReactive(K key, Request<T> request) {
        ReactiveLockBasedTransaction<R> transaction = allocateTransaction(key);
        return execute(request, transaction);
    }

    protected abstract ReactiveLockBasedTransaction<R> allocateTransaction(K key);

    private <T> Mono<CommandResult<T>> execute(Request<T> request, ReactiveLockBasedTransaction<R> transaction) {
        return Mono.usingWhen(
                transaction.begin(),
                resource -> execute(resource, request, transaction),
                resource -> Mono.from(transaction.commit(resource))
                        .thenMany(transaction.unlock(resource))
                        .thenMany(transaction.release(resource)),
                (resource, throwable) -> Mono.from(transaction.rollback(resource))
                        .thenMany(transaction.unlock(resource))
                        .thenMany(transaction.release(resource)),
                resource -> Mono.from(transaction.rollback(resource))
                        .thenMany(transaction.unlock(resource))
                        .thenMany(transaction.release(resource))
        );
    }

    private <T> Mono<CommandResult<T>> execute(R resource, Request<T> request, ReactiveLockBasedTransaction<R> transaction) {
        RemoteCommand<T> command = request.getCommand();

        return Mono.from(transaction.lockAndGet(resource))
                .defaultIfEmpty(NO_DATA)
                .flatMap(persistedDataOnBeginOfTransaction -> {
                    persistedDataOnBeginOfTransaction = persistedDataOnBeginOfTransaction == NO_DATA ? null : persistedDataOnBeginOfTransaction;

                    if (persistedDataOnBeginOfTransaction == null && !request.getCommand().isInitializationCommand()) {
                        return Mono.from(transaction.rollback(resource))
                                .thenReturn(CommandResult.bucketNotFound());
                    }

                    GenericEntry entry = new GenericEntry(persistedDataOnBeginOfTransaction, request.getBackwardCompatibilityVersion());
                    CommandResult<T> result = command.execute(entry, getClientSideTime());
                    if (entry.isModified()) {
                        byte[] bytes = entry.getModifiedStateBytes();
                        if (persistedDataOnBeginOfTransaction == null) {
                            System.out.println("inserting empty data");
                            return Mono.from(transaction.create(resource, bytes, entry.getModifiedState())).thenReturn(result);
                        } else {
                            System.out.println("updating data");
                            return Mono.from(transaction.update(resource, bytes, entry.getModifiedState())).thenReturn(result);
                        }
                    }
                    return Mono.just(result);
                });
    }

    private static ClientSideConfig injectTimeClock(ClientSideConfig clientSideConfig) {
        if (clientSideConfig.getClientSideClock().isPresent()) {
            return clientSideConfig;
        }
        return clientSideConfig.withClientClock(TimeMeter.SYSTEM_MILLISECONDS);
    }
}
