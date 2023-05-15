package io.github.bucket4j.redis.lettuce.cas;

import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.tck.AbstractDistributedReactiveBucketTest;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Testcontainers
class LettuceBasedReactiveProxyManagerTest extends AbstractDistributedReactiveBucketTest<byte[]> {

    @Container
    private static final GenericContainer container = new GenericContainer("redis:7.0.2").withExposedPorts(6379);
    private static RedisClient redisClient;

    @BeforeAll
    static void setup() {
        redisClient = createLettuceClient(container);
    }

    @AfterAll
    static void shutdown() {
        if (redisClient != null) {
            redisClient.shutdown();
        }
    }

    private static RedisClient createLettuceClient(GenericContainer container) {
        String redisHost = container.getHost();
        Integer redisPort = container.getMappedPort(6379);
        String redisUrl = "redis://" + redisHost + ":" + redisPort;

        return RedisClient.create(redisUrl);
    }

    @Override
    protected ReactiveProxyManager<byte[]> getReactiveProxyManager() {
        return LettuceBasedReactiveProxyManager.builderFor(redisClient)
                .withExpirationStrategy(ExpirationAfterWriteStrategy.none())
                .build();
    }

    @Override
    protected byte[] generateRandomKey() {
        return UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8);
    }
}
