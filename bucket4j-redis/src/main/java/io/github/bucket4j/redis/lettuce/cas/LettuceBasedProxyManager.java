/*-
 * ========================LICENSE_START=================================
 * Bucket4j
 * %%
 * Copyright (C) 2015 - 2022 Vladimir Bukhtoyarov
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

package io.github.bucket4j.redis.lettuce.cas;

import io.github.bucket4j.TimeMeter;
import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.generic.compare_and_swap.AbstractCompareAndSwapBasedProxyManager;
import io.github.bucket4j.distributed.proxy.generic.compare_and_swap.AsyncCompareAndSwapOperation;
import io.github.bucket4j.distributed.proxy.generic.compare_and_swap.CompareAndSwapOperation;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import io.github.bucket4j.redis.AbstractRedisProxyManagerBuilder;
import io.github.bucket4j.redis.consts.LuaScripts;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class LettuceBasedProxyManager<K> extends AbstractCompareAndSwapBasedProxyManager<K> {

    private final RedisApi<K> redisApi;
    private final ExpirationAfterWriteStrategy expirationStrategy;

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(RedisAsyncCommands<K, byte[]> redisAsyncCommands) {
        Objects.requireNonNull(redisAsyncCommands);
        RedisApi<K> redisApi = new RedisApi<>() {
            @Override
            public <V> RedisFuture<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params) {
                return redisAsyncCommands.eval(script, scriptOutputType, keys, params);
            }
            @Override
            public RedisFuture<byte[]> get(K key) {
                return redisAsyncCommands.get(key);
            }
            @Override
            public RedisFuture<Void> delete(K key) {
                return (RedisFuture) redisAsyncCommands.del(key);
            }
        };
        return new LettuceBasedProxyManagerBuilder<>(redisApi);
    }

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(StatefulRedisConnection<K, byte[]> statefulRedisConnection) {
        return builderFor(statefulRedisConnection.async());
    }

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(RedisClient redisClient, RedisCodec<K, ?> keyCodec) {
        return builderFor(redisClient.connect(RedisCodec.of(keyCodec, ByteArrayCodec.INSTANCE)));
    }

    public static LettuceBasedProxyManagerBuilder<byte[]> builderFor(RedisClient redisClient) {
        return builderFor(redisClient.connect(ByteArrayCodec.INSTANCE));
    }

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(RedisClusterClient redisClient, RedisCodec<K, ?> keyCodec) {
        return builderFor(redisClient.connect(RedisCodec.of(keyCodec, ByteArrayCodec.INSTANCE)));
    }

    public static LettuceBasedProxyManagerBuilder<byte[]> builderFor(RedisClusterClient redisClient) {
        return builderFor(redisClient.connect(ByteArrayCodec.INSTANCE));
    }

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(StatefulRedisClusterConnection<K, byte[]> connection) {
        return builderFor(connection.async());
    }

    public static <K> LettuceBasedProxyManagerBuilder<K> builderFor(RedisAdvancedClusterAsyncCommands<K, byte[]> redisAsyncCommands) {
        Objects.requireNonNull(redisAsyncCommands);
        RedisApi<K> redisApi = new RedisApi<>() {
            @Override
            public <V> RedisFuture<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params) {
                return redisAsyncCommands.eval(script, scriptOutputType, keys, params);
            }
            @Override
            public RedisFuture<byte[]> get(K key) {
                return redisAsyncCommands.get(key);
            }
            @Override
            public RedisFuture<Void> delete(K key) {
                return (RedisFuture) redisAsyncCommands.del(key);
            }
        };
        return new LettuceBasedProxyManagerBuilder<>(redisApi);
    }

    public static class LettuceBasedProxyManagerBuilder<K> extends AbstractRedisProxyManagerBuilder<LettuceBasedProxyManagerBuilder<K>> {

        private final RedisApi<K> redisApi;

        private LettuceBasedProxyManagerBuilder(RedisApi<K> redisApi) {
            this.redisApi = Objects.requireNonNull(redisApi);
        }

        public LettuceBasedProxyManager<K> build() {
            return new LettuceBasedProxyManager<>(this);
        }

    }

    private LettuceBasedProxyManager(LettuceBasedProxyManagerBuilder<K> builder) {
        super(builder.getClientSideConfig());
        this.expirationStrategy = builder.getNotNullExpirationStrategy();
        this.redisApi = builder.redisApi;
    }

    @Override
    protected CompareAndSwapOperation beginCompareAndSwapOperation(K key) {
        @SuppressWarnings("unchecked")
        K[] keys = (K[]) new Object[]{key};
        return new CompareAndSwapOperation() {
            @Override
            public Optional<byte[]> getStateData() {
                RedisFuture<byte[]> stateFuture = redisApi.get(key);
                return Optional.ofNullable(getFutureValue(stateFuture));
            }

            @Override
            public boolean compareAndSwap(byte[] originalData, byte[] newData, RemoteBucketState newState) {
                return getFutureValue(compareAndSwapFuture(keys, originalData, newData, newState));
            }
        };
    }

    @Override
    protected AsyncCompareAndSwapOperation beginAsyncCompareAndSwapOperation(K key) {
        @SuppressWarnings("unchecked")
        K[] keys = (K[]) new Object[]{key};
        return new AsyncCompareAndSwapOperation() {
            @Override
            public CompletableFuture<Optional<byte[]>> getStateData() {
                RedisFuture<byte[]> stateFuture = redisApi.get(key);
                return convertToCompletableFuture(stateFuture)
                        .thenApply((byte[] resultBytes) -> Optional.ofNullable(resultBytes));
            }

            @Override
            public CompletableFuture<Boolean> compareAndSwap(byte[] originalData, byte[] newData, RemoteBucketState newState) {
                return convertToCompletableFuture(compareAndSwapFuture(keys, originalData, newData, newState));
            }
        };
    }

    @Override
    public void removeProxy(K key) {
        RedisFuture<?> future = redisApi.delete(key);
        getFutureValue(future);
    }

    @Override
    protected CompletableFuture<Void> removeAsync(K key) {
        RedisFuture<?> future = redisApi.delete(key);
        return convertToCompletableFuture(future).thenApply(bytes -> null);
    }

    @Override
    public boolean isAsyncModeSupported() {
        return true;
    }

    private RedisFuture<Boolean> compareAndSwapFuture(K[] keys, byte[] originalData, byte[] newData, RemoteBucketState newState) {
        long ttlMillis = calculateTtlMillis(newState);
        if (ttlMillis > 0) {
            if (originalData == null) {
                // nulls are prohibited as values, so "replace" must not be used in such cases
                byte[][] params = {newData, encodeLong(ttlMillis)};
                return redisApi.eval(LuaScripts.SCRIPT_SET_NX_PX, ScriptOutputType.BOOLEAN, keys, params);
            } else {
                byte[][] params = {originalData, newData, encodeLong(ttlMillis)};
                return redisApi.eval(LuaScripts.SCRIPT_COMPARE_AND_SWAP_PX, ScriptOutputType.BOOLEAN, keys, params);
            }
        } else {
            if (originalData == null) {
                // nulls are prohibited as values, so "replace" must not be used in such cases
                byte[][] params = {newData};
                return redisApi.eval(LuaScripts.SCRIPT_SET_NX, ScriptOutputType.BOOLEAN, keys, params);
            } else {
                byte[][] params = {originalData, newData};
                return redisApi.eval(LuaScripts.SCRIPT_COMPARE_AND_SWAP, ScriptOutputType.BOOLEAN, keys, params);
            }
        }
    }

    private <T> CompletableFuture<T> convertToCompletableFuture(RedisFuture<T> redisFuture) {
        CompletableFuture<T> jdkFuture = new CompletableFuture<>();
        redisFuture.whenComplete((result, error) -> {
            if (error != null) {
                jdkFuture.completeExceptionally(error);
            } else {
                jdkFuture.complete(result);
            }
        });
        return jdkFuture;
    }

    private <V> V getFutureValue(RedisFuture<V> value) {
        try {
            return value.get();
        } catch (InterruptedException e) {
            value.cancel(true);
            Thread.currentThread().interrupt();
            throw new RedisException(e);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof RedisException ? (RedisException) e.getCause() :
                    new RedisException("Unexpected exception while processing command", e.getCause());
        }
    }

    private byte[] encodeLong(Long value) {
        return ("" + value).getBytes(StandardCharsets.UTF_8);
    }

    private long calculateTtlMillis(RemoteBucketState state) {
        Optional<TimeMeter> clock = getClientSideConfig().getClientSideClock();
        long currentTimeNanos = clock.isPresent() ? clock.get().currentTimeNanos() : System.currentTimeMillis() * 1_000_000;
        return expirationStrategy.calculateTimeToLiveMillis(state, currentTimeNanos);
    }

    private interface RedisApi<K> {

        <V> RedisFuture<V> eval(String script, ScriptOutputType scriptOutputType, K[] keys, byte[][] params);

        RedisFuture<byte[]> get(K key);

        RedisFuture<Void> delete(K key);

    }

}