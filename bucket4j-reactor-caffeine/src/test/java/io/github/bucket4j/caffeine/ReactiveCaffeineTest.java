package io.github.bucket4j.caffeine;

import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.tck.AbstractDistributedReactiveBucketTest;

import java.time.Duration;
import java.util.UUID;

class ReactiveCaffeineTest extends AbstractDistributedReactiveBucketTest<String> {
    @Override
    protected ReactiveProxyManager<String> getReactiveProxyManager() {
        return new ReactiveCaffeineProxyManager<>(Caffeine.newBuilder().maximumSize(100), Duration.ofMinutes(1));
    }

    @Override
    protected String generateRandomKey() {
        return UUID.randomUUID().toString();
    }
}
