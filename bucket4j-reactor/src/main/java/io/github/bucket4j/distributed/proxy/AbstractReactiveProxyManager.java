package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.BucketExceptions;
import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.TokensInheritanceStrategy;
import io.github.bucket4j.distributed.ReactiveBucketProxy;
import io.github.bucket4j.distributed.proxy.optimization.Optimization;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import io.github.bucket4j.distributed.remote.Request;
import io.github.bucket4j.distributed.remote.commands.GetConfigurationCommand;
import io.github.bucket4j.distributed.versioning.Version;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public abstract class AbstractReactiveProxyManager<K> implements ReactiveProxyManager<K> {
    private static final RecoveryStrategy DEFAULT_RECOVERY_STRATEGY = RecoveryStrategy.RECONSTRUCT;

    private final ClientSideConfig clientSideConfig;

    protected AbstractReactiveProxyManager(ClientSideConfig clientSideConfig) {
        this.clientSideConfig = requireNonNull(clientSideConfig);
    }

    @Override
    public ProxyManager<K> asProxyManager() {
        return new AbstractProxyManager<>(clientSideConfig) {
            @Override
            protected <T> CommandResult<T> execute(K key, Request<T> request) {
                return executeReactive(key, request).block();
            }

            @Override
            protected <T> CompletableFuture<CommandResult<T>> executeAsync(K key, Request<T> request) {
                return executeReactive(key, request).toFuture();
            }

            @Override
            protected CompletableFuture<Void> removeAsync(K key) {
                return removeReactive(key).toFuture();
            }

            @Override
            public void removeProxy(K key) {
                AbstractReactiveProxyManager.this.removeProxy(key).block();
            }

            @Override
            public boolean isAsyncModeSupported() {
                return true;
            }
        };
    }

    @Override
    public RemoteReactiveBucketBuilder<K> builder() {
        return new DefaultReactiveRemoteBucketBuilder();
    }

    @Override
    public Mono<BucketConfiguration> getProxyConfiguration(K key) {
        GetConfigurationCommand cmd = new GetConfigurationCommand();
        getClientSideTime();
        Request<BucketConfiguration> request = new Request<>(cmd, getBackwardCompatibilityVersion(), getClientSideTime());
        return executeReactive(key, request).flatMap(result -> {
            if (result.isBucketNotFound()) {
                return Mono.empty();
            }
            return Mono.just(result.getData());
        });
    }

    private class DefaultReactiveRemoteBucketBuilder implements RemoteReactiveBucketBuilder<K> {

        private RecoveryStrategy recoveryStrategy = DEFAULT_RECOVERY_STRATEGY;
        private ImplicitConfigurationReplacement implicitConfigurationReplacement;

        @Override
        public DefaultReactiveRemoteBucketBuilder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
            this.recoveryStrategy = requireNonNull(recoveryStrategy);
            return this;
        }

        @Override
        public DefaultReactiveRemoteBucketBuilder withOptimization(Optimization requestOptimizer) {
            throw new UnsupportedOperationException("Optimization is not yet supported for reactive bucket");
        }

        @Override
        public DefaultReactiveRemoteBucketBuilder withImplicitConfigurationReplacement(long desiredConfigurationVersion, TokensInheritanceStrategy tokensInheritanceStrategy) {
            this.implicitConfigurationReplacement = new ImplicitConfigurationReplacement(desiredConfigurationVersion, requireNonNull(tokensInheritanceStrategy));
            return this;
        }

        @Override
        public ReactiveBucketProxy build(K key, BucketConfiguration configuration) {
            if (configuration == null) {
                throw BucketExceptions.nullConfiguration();
            }
            return build(key, Mono.just(configuration));
        }

        @Override
        public ReactiveBucketProxy build(K key, Publisher<BucketConfiguration> configurationMono) {
            if (configurationMono == null) {
                throw BucketExceptions.nullConfigurationSupplier();
            }

            ReactiveCommandExecutor commandExecutor = new ReactiveCommandExecutor() {
                @Override
                public <T> Mono<CommandResult<T>> executeReactive(RemoteCommand<T> command) {
                    Request<T> request = new Request<>(command, getBackwardCompatibilityVersion(), getClientSideTime());
                    return AbstractReactiveProxyManager.this.executeReactive(key, request);
                }
            };

            return new DefaultReactiveBucketProxy(commandExecutor, recoveryStrategy, configurationMono, implicitConfigurationReplacement);
        }

    }

    abstract protected <T> Mono<CommandResult<T>> executeReactive(K key, Request<T> request);

    abstract protected Mono<Void> removeReactive(K key);

    public Mono<Void> removeProxy(K key) {
        return removeReactive(key);
    }

    protected ClientSideConfig getClientSideConfig() {
        return clientSideConfig;
    }

    protected Version getBackwardCompatibilityVersion() {
        return clientSideConfig.getBackwardCompatibilityVersion();
    }

    protected Long getClientSideTime() {
        Optional<TimeMeter> clientClock = clientSideConfig.getClientSideClock();
        if (clientClock.isEmpty()) {
            return null;
        }
        return clientClock.get().currentTimeNanos();
    }

}
