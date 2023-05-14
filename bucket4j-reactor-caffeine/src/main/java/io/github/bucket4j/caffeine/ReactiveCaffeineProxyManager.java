package io.github.bucket4j.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import io.github.bucket4j.Experimental;
import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.proxy.AbstractReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.MutableBucketEntry;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import io.github.bucket4j.distributed.remote.Request;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

@Experimental
public class ReactiveCaffeineProxyManager<K> extends AbstractReactiveProxyManager<K> {
    private final Cache<K, RemoteBucketState> cache;

    public ReactiveCaffeineProxyManager(Caffeine<? super K, ? super RemoteBucketState> builder, Duration keepAfterRefillDuration) {
        this(builder, keepAfterRefillDuration, ClientSideConfig.getDefault());
    }

    public ReactiveCaffeineProxyManager(Caffeine<? super K, ? super RemoteBucketState> builder, Duration keepAfterRefillDuration, ClientSideConfig clientSideConfig) {
        super(clientSideConfig);
        this.cache = builder
                .expireAfter(new Expiry<K, RemoteBucketState>() {
                    @Override
                    public long expireAfterCreate(K key, RemoteBucketState bucketState, long currentTime) {
                        long currentTimeNanos = getCurrentTime(clientSideConfig);
                        long nanosToFullRefill = bucketState.calculateFullRefillingTime(currentTimeNanos);
                        return nanosToFullRefill + keepAfterRefillDuration.toNanos();
                    }

                    @Override
                    public long expireAfterUpdate(K key, RemoteBucketState bucketState, long currentTime, long currentDuration) {
                        long currentTimeNanos = getCurrentTime(clientSideConfig);
                        long nanosToFullRefill = bucketState.calculateFullRefillingTime(currentTimeNanos);
                        return nanosToFullRefill + keepAfterRefillDuration.toNanos();
                    }

                    @Override
                    public long expireAfterRead(K key, RemoteBucketState bucketState, long currentTime, long currentDuration) {
                        long currentTimeNanos = getCurrentTime(clientSideConfig);
                        long nanosToFullRefill = bucketState.calculateFullRefillingTime(currentTimeNanos);
                        return nanosToFullRefill + keepAfterRefillDuration.toNanos();
                    }
                })
                .build();
    }

    public Cache<K, RemoteBucketState> getCache() {
        return cache;
    }

    @Override
    protected <T> Mono<CommandResult<T>> executeReactive(K key, Request<T> request) {
        return Mono.fromSupplier(() -> {
            @SuppressWarnings("unchecked")
            CommandResult<T>[] resultHolder = new CommandResult[1];

            cache.asMap().compute(key, (K k, RemoteBucketState previousState) -> {
                Long clientSideTime = request.getClientSideTime();
                long timeNanos = clientSideTime != null ? clientSideTime : System.currentTimeMillis() * 1_000_000;
                RemoteBucketState[] stateHolder = new RemoteBucketState[] {
                        previousState == null ? null : previousState.copy()
                };
                MutableBucketEntry entry = new MutableBucketEntry() {
                    @Override
                    public boolean exists() {
                        return stateHolder[0] != null;
                    }
                    @Override
                    public void set(RemoteBucketState state) {
                        stateHolder[0] = state;
                    }
                    @Override
                    public RemoteBucketState get() {
                        return stateHolder[0];
                    }
                };
                resultHolder[0] = request.getCommand().execute(entry, timeNanos);
                return stateHolder[0];
            });

            return resultHolder[0];
        });
    }

    @Override
    protected Mono<Void> removeReactive(K key) {
        return Mono.fromSupplier(() -> cache.asMap().remove(key)).then();
    }

    private static long getCurrentTime(ClientSideConfig clientSideConfig) {
        Optional<TimeMeter> clock = clientSideConfig.getClientSideClock();
        return clock.map(TimeMeter::currentTimeNanos).orElseGet(() -> System.currentTimeMillis() * 1_000_000);
    }
}
