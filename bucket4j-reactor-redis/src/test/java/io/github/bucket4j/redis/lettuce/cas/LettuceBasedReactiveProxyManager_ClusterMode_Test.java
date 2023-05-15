package io.github.bucket4j.redis.lettuce.cas;

import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.tck.AbstractDistributedReactiveBucketTest;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LettuceBasedReactiveProxyManager_ClusterMode_Test extends AbstractDistributedReactiveBucketTest<byte[]> {

    private static final Logger logger = LoggerFactory.getLogger(LettuceBasedReactiveProxyManager_ClusterMode_Test.class);
    private static final Integer[] CONTAINER_CLUSTER_PORTS = {7000, 7001, 7002, 7003, 7004, 7005};

    private static GenericContainer container;
    private static RedisClusterClient redisClient;

    @BeforeClass
    public static void setup() {
        container = startRedisContainer();
        redisClient = createLettuceClient(container);
    }

    @AfterClass
    public static void shutdown() {
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (container != null) {
            container.close();
        }
    }

    private static RedisClusterClient createLettuceClient(GenericContainer container) {
        List<RedisURI> clusterHosts = Arrays.stream(CONTAINER_CLUSTER_PORTS)
                .map(port -> RedisURI.create(container.getHost(), container.getMappedPort(port)))
                .collect(Collectors.toList());

        return RedisClusterClient.create(clusterHosts);
    }

    private static GenericContainer startRedisContainer() {
        // see this doc https://github.com/Grokzen/docker-redis-cluster
        GenericContainer genericContainer = new GenericContainer("grokzen/redis-cluster:6.0.7");

        genericContainer.withExposedPorts(CONTAINER_CLUSTER_PORTS);
        // start does not wait cluster availability
        genericContainer.start();

        List<RedisURI> clusterHosts = Arrays.stream(CONTAINER_CLUSTER_PORTS)
                .map(port -> RedisURI.create(genericContainer.getHost(), genericContainer.getMappedPort(port)))
                .collect(Collectors.toList());

        // need to wait until all nodes join to cluster
        for (int i = 0; i < 200; i++) {
            RedisClusterClient redisClientProbe = null;
            try {
                redisClientProbe = RedisClusterClient.create(clusterHosts);
                StatefulRedisClusterConnection<String, String> clusterConnection = redisClientProbe.connect();
                AtomicInteger availableSlots = new AtomicInteger();
                clusterConnection.getPartitions().forEach(partition -> partition.forEachSlot(slot -> availableSlots.incrementAndGet()));
                if (availableSlots.get() < SlotHash.SLOT_COUNT) {
                    throw new IllegalStateException("Only " + availableSlots.get() + " slots is ready from required " + SlotHash.SLOT_COUNT);
                }

                clusterConnection.sync().get("42");
                return genericContainer;
            } catch (Throwable e) {
                logger.error("Failed to check Redis Cluster availability: {}", e.getMessage(), e);

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } finally {
                if (redisClientProbe != null) {
                    redisClientProbe.shutdown();
                }
            }
        }

        throw new IllegalStateException("Cluster was not assembled in " + TimeUnit.MILLISECONDS.toSeconds(200 * 100) + " seconds");
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
