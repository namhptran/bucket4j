package io.github.bucket4j.redis.lettuce.cas;

import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.generic.compare_and_swap.AbstractCompareAndSwapBasedReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.generic.compare_and_swap.ReactiveCompareAndSwapOperation;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import io.github.bucket4j.redis.AbstractRedisProxyManagerBuilder;
import io.github.bucket4j.redis.consts.LuaScripts;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public class LettuceBasedReactiveProxyManager<K> extends AbstractCompareAndSwapBasedReactiveProxyManager<K> {
    private final RedisApi<K> redisApi;
    private final ExpirationAfterWriteStrategy expirationStrategy;

    public static <K> LettuceBasedReactiveProxyManagerBuilder<K> builderFor(RedisReactiveCommands<K, byte[]> redisReactiveCommands) {
        Objects.requireNonNull(redisReactiveCommands);
        RedisApi<K> redisApi = new RedisApi<>() {
            @Override
            public <V> Flux<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params) {
                return redisReactiveCommands.eval(script, scriptOutputType, keys, params);
            }
            @Override
            public Mono<byte[]> get(K key) {
                return redisReactiveCommands.get(key);
            }
            @Override
            public Mono<Void> delete(K key) {
                return redisReactiveCommands.del(key).then();
            }
        };
        return new LettuceBasedReactiveProxyManagerBuilder<>(redisApi);
    }

    public static <K> LettuceBasedReactiveProxyManagerBuilder<K> builderFor(StatefulRedisConnection<K, byte[]> statefulRedisConnection) {
        return builderFor(statefulRedisConnection.reactive());
    }

    public static LettuceBasedReactiveProxyManagerBuilder<byte[]> builderFor(RedisClient redisClient) {
        return builderFor(redisClient.connect(ByteArrayCodec.INSTANCE));
    }

    public static LettuceBasedReactiveProxyManagerBuilder<byte[]> builderFor(RedisClusterClient redisClient) {
        return builderFor(redisClient.connect(ByteArrayCodec.INSTANCE));
    }

    public static <K> LettuceBasedReactiveProxyManagerBuilder<K> builderFor(StatefulRedisClusterConnection<K, byte[]> connection) {
        return builderFor(connection.reactive());
    }

    public static <K> LettuceBasedReactiveProxyManagerBuilder<K> builderFor(RedisAdvancedClusterReactiveCommands<K, byte[]> redisReactiveCommands) {
        Objects.requireNonNull(redisReactiveCommands);
        RedisApi<K> redisApi = new RedisApi<>() {
            @Override
            public <V> Flux<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params) {
                return redisReactiveCommands.eval(script, scriptOutputType, keys, params);
            }
            @Override
            public Mono<byte[]> get(K key) {
                return redisReactiveCommands.get(key);
            }
            @Override
            public Mono<Void> delete(K key) {
                return redisReactiveCommands.del(key).then();
            }
        };
        return new LettuceBasedReactiveProxyManagerBuilder<>(redisApi);
    }

    public static class LettuceBasedReactiveProxyManagerBuilder<K> extends AbstractRedisProxyManagerBuilder<LettuceBasedReactiveProxyManagerBuilder<K>> {

        private final RedisApi<K> redisApi;

        private LettuceBasedReactiveProxyManagerBuilder(RedisApi<K> redisApi) {
            this.redisApi = Objects.requireNonNull(redisApi);
        }

        public LettuceBasedReactiveProxyManager<K> build() {
            return new LettuceBasedReactiveProxyManager<K>(this);
        }

    }

    private LettuceBasedReactiveProxyManager(LettuceBasedReactiveProxyManagerBuilder<K> builder) {
        super(builder.getClientSideConfig());
        this.expirationStrategy = builder.getNotNullExpirationStrategy();
        this.redisApi = builder.redisApi;
    }


    @Override
    protected ReactiveCompareAndSwapOperation beginReactiveCompareAndSwapOperation(K key) {
        @SuppressWarnings("unchecked")
        K[] keys = (K[]) new Object[]{key};
        return new ReactiveCompareAndSwapOperation() {
            @Override
            public Mono<byte[]> getStateData() {
                return redisApi.get(key);
            }

            @Override
            public Mono<Boolean> compareAndSwap(byte[] originalData, byte[] newData, RemoteBucketState newState) {
                return compareAndSwapMono(keys, originalData, newData, newState);
            }
        };
    }

    @Override
    protected Mono<Void> removeReactive(K key) {
        return redisApi.delete(key).then();
    }

    private Mono<Boolean> compareAndSwapMono(K[] keys, byte[] originalData, byte[] newData, RemoteBucketState newState) {
        long ttlMillis = calculateTtlMillis(newState);
        if (ttlMillis > 0) {
            if (originalData == null) {
                // nulls are prohibited as values, so "replace" must not be used in such cases
                byte[][] params = {newData, encodeLong(ttlMillis)};
                return redisApi.<Boolean>eval(LuaScripts.SCRIPT_SET_NX_PX, ScriptOutputType.BOOLEAN, keys, params).next();
            } else {
                byte[][] params = {originalData, newData, encodeLong(ttlMillis)};
                return redisApi.<Boolean>eval(LuaScripts.SCRIPT_COMPARE_AND_SWAP_PX, ScriptOutputType.BOOLEAN, keys, params).next();
            }
        } else {
            if (originalData == null) {
                // nulls are prohibited as values, so "replace" must not be used in such cases
                byte[][] params = {newData};
                return redisApi.<Boolean>eval(LuaScripts.SCRIPT_SET_NX, ScriptOutputType.BOOLEAN, keys, params).log().next();
            } else {
                byte[][] params = {originalData, newData};
                return redisApi.<Boolean>eval(LuaScripts.SCRIPT_COMPARE_AND_SWAP, ScriptOutputType.BOOLEAN, keys, params).next();
            }
        }
    }

    private byte[] encodeLong(Long value) {
        return ("" + value).getBytes(StandardCharsets.UTF_8);
    }

    private long calculateTtlMillis(RemoteBucketState state) {
        Optional<TimeMeter> clock = getClientSideConfig().getClientSideClock();
        long currentTimeNanos = clock.map(TimeMeter::currentTimeNanos).orElseGet(() -> System.currentTimeMillis() * 1_000_000);
        return expirationStrategy.calculateTimeToLiveMillis(state, currentTimeNanos);
    }

    private interface RedisApi<K> {

        <V> Flux<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params);

        Mono<byte[]> get(K key);

        Mono<?> delete(K key);

    }
}
