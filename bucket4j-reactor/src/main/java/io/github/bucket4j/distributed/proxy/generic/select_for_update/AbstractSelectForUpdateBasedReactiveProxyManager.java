package io.github.bucket4j.distributed.proxy.generic.select_for_update;

import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.proxy.AbstractReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.proxy.generic.GenericEntry;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import io.github.bucket4j.distributed.remote.Request;
import reactor.core.publisher.Mono;

public abstract class AbstractSelectForUpdateBasedReactiveProxyManager<K, R> extends AbstractReactiveProxyManager<K> {
    private static final CommandResult<?> RETRY_IN_THE_SCOPE_OF_NEW_TRANSACTION = CommandResult.success(true, 666);

    protected AbstractSelectForUpdateBasedReactiveProxyManager(ClientSideConfig clientSideConfig) {
        super(injectTimeClock(clientSideConfig));
    }

    @Override
    protected <T> Mono<CommandResult<T>> executeReactive(K key, Request<T> request) {
        ReactiveSelectForUpdateBasedTransaction<R> transaction = allocateTransaction(key);
        return retrySelectForUpdate(executeReactive(request, transaction));

    }

    protected abstract ReactiveSelectForUpdateBasedTransaction<R> allocateTransaction(K key);

    private <T> Mono<CommandResult<T>> executeReactive(Request<T> request, ReactiveSelectForUpdateBasedTransaction<R> transaction) {
        return Mono.usingWhen(
                transaction.begin(),
                resource -> executeReactive(resource, request, transaction),
                resource -> Mono.from(transaction.commit(resource))
                        .thenMany(transaction.release(resource)),
                (resource, throwable) -> Mono.from(transaction.rollback(resource))
                        .thenMany(transaction.release(resource)),
                resource -> Mono.from(transaction.rollback(resource))
                        .thenMany(transaction.release(resource))
        );
    }

    private <T> Mono<CommandResult<T>> executeReactive(R resource, Request<T> request, ReactiveSelectForUpdateBasedTransaction<R> transaction) {
        RemoteCommand<T> command = request.getCommand();
        Mono<CommandResult<T>> execution = Mono.from(transaction.tryLockAndGet(resource))
                .flatMap(lockResult -> {
                    if (!lockResult.isLocked()) {
                        return Mono.from(transaction.tryInsertEmptyData(resource))
                                .flatMap(insertResult -> Mono.from(
                                        insertResult ? transaction.commit(resource)
                                                : transaction.rollback(resource)
                                ).thenReturn((CommandResult<T>) RETRY_IN_THE_SCOPE_OF_NEW_TRANSACTION));
                    }

                    byte[] persistedDataOnBeginOfTransaction = lockResult.getData();
                    if (persistedDataOnBeginOfTransaction == null && !request.getCommand().isInitializationCommand()) {
                        return Mono.from(transaction.rollback(resource))
                                .thenReturn(CommandResult.bucketNotFound());
                    }

                    GenericEntry entry = new GenericEntry(persistedDataOnBeginOfTransaction, request.getBackwardCompatibilityVersion());
                    CommandResult<T> result = command.execute(entry, super.getClientSideTime());
                    if (entry.isModified()) {
                        byte[] bytes = entry.getModifiedStateBytes();
                        return Mono.from(transaction.update(resource, bytes, entry.getModifiedState())).thenReturn(result);
                    }
                    return Mono.just(result);
                });

        return execution;
    }

    private static <T> Mono<CommandResult<T>> retrySelectForUpdate(Mono<CommandResult<T>> execution) {
        return execution.flatMap(result -> {
            if (result == RETRY_IN_THE_SCOPE_OF_NEW_TRANSACTION) {
                return retrySelectForUpdate(execution);
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
