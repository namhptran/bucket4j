package io.github.bucket4j.distributed.proxy.generic.compare_and_swap;

import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.proxy.AbstractReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.proxy.generic.GenericEntry;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import io.github.bucket4j.distributed.remote.Request;
import reactor.core.publisher.Mono;

public abstract class AbstractCompareAndSwapBasedReactiveProxyManager<K> extends AbstractReactiveProxyManager<K> {
    private static final byte[] NO_DATA = new byte[0];

    protected AbstractCompareAndSwapBasedReactiveProxyManager(ClientSideConfig clientSideConfig) {
        super(injectTimeClock(clientSideConfig));
    }

    @Override
    protected <T> Mono<CommandResult<T>> executeReactive(K key, Request<T> request) {
        return Mono.fromSupplier(() -> beginReactiveCompareAndSwapOperation(key))
                .flatMap(operation -> executeReactive(request, operation));
    }

    protected abstract ReactiveCompareAndSwapOperation beginReactiveCompareAndSwapOperation(K key);


    private <T> Mono<CommandResult<T>> executeReactive(Request<T> request, ReactiveCompareAndSwapOperation operation) {
        return operation.getStateData()
                .defaultIfEmpty(NO_DATA)
                .flatMap(originalStateBytes -> {
                    originalStateBytes = originalStateBytes == NO_DATA ? null : originalStateBytes;
                    RemoteCommand<T> command = request.getCommand();
                    GenericEntry entry = new GenericEntry(originalStateBytes, request.getBackwardCompatibilityVersion());
                    CommandResult<T> result = command.execute(entry, getClientSideTime());
                    if (!entry.isModified()) {
                        return Mono.just(result);
                    }

                    byte[] newStateBytes = entry.getModifiedStateBytes();
                    return operation.compareAndSwap(originalStateBytes, newStateBytes, entry.getModifiedState())
                            .flatMap(casWasSuccessful -> casWasSuccessful
                                    ? Mono.just(result)
                                    : executeReactive(request, operation)); // Retry
                });
    }

    private static ClientSideConfig injectTimeClock(ClientSideConfig clientSideConfig) {
        if (clientSideConfig.getClientSideClock().isPresent()) {
            return clientSideConfig;
        }
        return clientSideConfig.withClientClock(TimeMeter.SYSTEM_MILLISECONDS);
    }
}
