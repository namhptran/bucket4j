package io.github.bucket4j.tck;

import io.github.bucket4j.*;
import io.github.bucket4j.distributed.ReactiveBucketProxy;
import io.github.bucket4j.distributed.proxy.BucketNotFoundException;
import io.github.bucket4j.distributed.proxy.ProxyManager;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.util.ReactiveConsumptionScenario;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.github.bucket4j.distributed.proxy.RecoveryStrategy.THROW_BUCKET_NOT_FOUND_EXCEPTION;
import static org.junit.Assert.assertEquals;

public abstract class AbstractDistributedReactiveBucketTest<K> extends AbstractDistributedBucketTest<K> {

    private final K key = generateRandomKey();
    private final K anotherKey = generateRandomKey();
    private final ReactiveProxyManager<K> proxyManager = getReactiveProxyManager();

    private BucketConfiguration configurationForLongRunningTests = BucketConfiguration.builder()
            .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)).withInitialTokens(0))
            .addLimit(Bandwidth.simple(200, Duration.ofSeconds(10)).withInitialTokens(0))
            .build();
    private double permittedRatePerSecond = Math.min(1_000d / 60, 200.0 / 10);

    protected abstract ReactiveProxyManager<K> getReactiveProxyManager();

    protected abstract K generateRandomKey();

    @Override
    protected ProxyManager<K> getProxyManager() {
        return getReactiveProxyManager().asProxyManager();
    }

    @Test
    public void testReactiveReconstructRecoveryStrategy() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .addLimit(Bandwidth.simple(200, Duration.ofSeconds(10)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);

        StepVerifier.create(bucket.asReactive().tryConsume(1, 1))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        // simulate crash
        StepVerifier.create(proxyManager.removeProxy(key))
                .expectSubscription()
                .expectComplete()
                .verify();

        StepVerifier.create(bucket.asReactive().tryConsume(1, 1))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReactiveThrowExceptionRecoveryStrategy() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .build();


        ReactiveBucketProxy bucket = proxyManager.builder()
                .withRecoveryStrategy(THROW_BUCKET_NOT_FOUND_EXCEPTION)
                .build(key, configuration);

        StepVerifier.create(bucket.asReactive().tryConsume(1, 1))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        // simulate crash
        StepVerifier.create(proxyManager.removeProxy(key))
                .verifyComplete();

        StepVerifier.create(bucket.asReactive().tryConsume(1, 1))
                .expectSubscription()
                .expectError(BucketNotFoundException.class)
                .verify();
    }

    @Test
    public void testReactiveLocateConfigurationThroughProxyManager() {
        StepVerifier.create(proxyManager.getProxyConfiguration(key))
                .expectSubscription()
                .expectComplete()
                .verify(); // Complete without onNext

        // should return not empty options if bucket is stored
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .build();

        StepVerifier.create(proxyManager.builder().build(key, configuration).getAvailableTokens())
                .expectSubscription()
                .expectNext(1000L)
                .verifyComplete();

        StepVerifier.create(proxyManager.builder()
                .withRecoveryStrategy(THROW_BUCKET_NOT_FOUND_EXCEPTION)
                .build(key, configuration)
                .getAvailableTokens())
                .expectSubscription()
                .expectNext(1000L)
                .verifyComplete();

        StepVerifier.create(proxyManager.getProxyConfiguration(key))
                .expectSubscription()
                .expectNextCount(1)
                .verifyComplete();

        // should return empty optional if bucket is removed
        StepVerifier.create(proxyManager.removeProxy(key))
                .verifyComplete();

        StepVerifier.create(proxyManager.getProxyConfiguration(key))
                .expectSubscription()
                .expectComplete()
                .verify();
    }

    @Test
    public void testReactiveBucketRemoval() {
        K key = generateRandomKey();

        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(4, Duration.ofHours(1)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);

        StepVerifier.create(bucket.getAvailableTokens())
                .expectSubscription()
                .expectNext(4L)
                .verifyComplete();

        StepVerifier.create(proxyManager.getProxyConfiguration(key))
                .expectSubscription()
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(proxyManager.removeProxy(key))
                .expectSubscription()
                .verifyComplete();

        StepVerifier.create(proxyManager.getProxyConfiguration(key))
                .expectSubscription()
                .verifyComplete();
    }

    @Test
    public void testReativeParallelInitialization() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.classic(10, Refill.intervally(1, Duration.ofMinutes(1))))
                .build();

        int PARALLELISM = 4;

        Flux<Boolean> consumptions = Flux.range(0, PARALLELISM)
                .flatMap(i -> proxyManager.builder()
                        .build(key, Mono.just(configuration))
                        .tryConsume(1), PARALLELISM);

        StepVerifier.create(consumptions)
                .expectSubscription()
                .expectNextCount(4)
                .expectComplete()
                .verify();

        StepVerifier.create(proxyManager.builder().build(key, configuration).getAvailableTokens())
                .expectSubscription()
                .expectNext(10L - PARALLELISM)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReactiveUnconditionalConsume() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);

        StepVerifier.create(bucket.consumeIgnoringRateLimits(121_000))
                .expectSubscription()
                .expectNext(TimeUnit.MINUTES.toNanos(120))
                .expectComplete()
                .verify();
    }

    @Test
    public void testReactiveUnconditionalConsumeVerbose() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(1_000, Duration.ofMinutes(1)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);

        StepVerifier.create(bucket.asVerbose().consumeIgnoringRateLimits(121_000))
                .expectSubscription()
                .expectNextMatches(result -> TimeUnit.MINUTES.toNanos(120) == result.getValue() && configuration.equals(result.getConfiguration()))
                .expectComplete()
                .verify();
    }

    @Test
    public void testTryConsumeReactive() throws Exception {
        Function<ReactiveBucketProxy, Long> action = bucket -> bucket.tryConsume(1).block() ? 1L : 0L;
        Supplier<ReactiveBucketProxy> bucketSupplier = () -> proxyManager.builder()
                .withRecoveryStrategy(THROW_BUCKET_NOT_FOUND_EXCEPTION)
                .build(key, configurationForLongRunningTests);
        int durationSeconds = System.getenv("CI") == null ? 5 : 1;
        ReactiveConsumptionScenario scenario = new ReactiveConsumptionScenario(4, TimeUnit.SECONDS.toNanos(durationSeconds), bucketSupplier, action, permittedRatePerSecond);
        scenario.executeAndValidateRate();
    }

    @Test
    public void testTryConsumeReactiveWithLimit() throws Exception {
        Scheduler scheduler = Schedulers.single();
        Function<ReactiveBucketProxy, Long> action = bucket -> Boolean.TRUE.equals(bucket.asReactive().tryConsume(1, TimeUnit.MILLISECONDS.toNanos(50), scheduler).block()) ? 1L :0L;
        Supplier<ReactiveBucketProxy> bucketSupplier = () -> proxyManager.builder()
                .withRecoveryStrategy(THROW_BUCKET_NOT_FOUND_EXCEPTION)
                .build(key, configurationForLongRunningTests);
        int durationSeconds = System.getenv("CI") == null ? 5 : 1;
        ReactiveConsumptionScenario scenario = new ReactiveConsumptionScenario(4, TimeUnit.SECONDS.toNanos(durationSeconds), bucketSupplier, action, permittedRatePerSecond);
        scenario.executeAndValidateRate();
    }

    @Test
    public void testTryConsumeReactiveWithLimitAsScheduling() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        Function<ReactiveBucketProxy, Long> action = bucket -> {
            try {
                return Boolean.TRUE.equals(bucket.asScheduler().tryConsume(1, TimeUnit.MILLISECONDS.toNanos(50), scheduler).get()) ? 1L :0L;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        Supplier<ReactiveBucketProxy> bucketSupplier = () -> proxyManager.builder()
                .withRecoveryStrategy(THROW_BUCKET_NOT_FOUND_EXCEPTION)
                .build(key, configurationForLongRunningTests);
        int durationSeconds = System.getenv("CI") == null ? 5 : 1;
        ReactiveConsumptionScenario scenario = new ReactiveConsumptionScenario(4, TimeUnit.SECONDS.toNanos(durationSeconds), bucketSupplier, action, permittedRatePerSecond);
        scenario.executeAndValidateRate();
    }

    @Test
    public void testReactiveBucketRegistryWithKeyIndependentConfiguration() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(10, Duration.ofDays(1)))
                .build();

        ReactiveBucketProxy bucket1 = proxyManager.builder().build(key, configuration);

        StepVerifier.create(bucket1.tryConsume(10))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        StepVerifier.create(bucket1.tryConsume(1))
                .expectSubscription()
                .expectNext(false)
                .expectComplete()
                .verify();

        ReactiveBucketProxy bucket2 = proxyManager.builder().build(anotherKey, Mono.just(configuration));

        StepVerifier.create(bucket2.tryConsume(10))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        StepVerifier.create(bucket2.tryConsume(1))
                .expectSubscription()
                .expectNext(false)
                .expectComplete()
                .verify();
    }

    @Test
    public void testReactiveBucketWithNotLazyConfiguration() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(10, Duration.ofDays(1)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);
        StepVerifier.create(bucket.tryConsume(10))
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        StepVerifier.create(bucket.tryConsume(1))
                .expectSubscription()
                .expectNext(false)
                .expectComplete()
                .verify();
    }

    // https://github.com/bucket4j/bucket4j/issues/279
    @Test
    public void testReactiveVerboseBucket() {
        int MIN_CAPACITY = 4;
        int MAX_CAPACITY = 10;
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.classic(MIN_CAPACITY, Refill.intervally(4, Duration.ofMinutes(20))))
                .addLimit(Bandwidth.classic(MAX_CAPACITY, Refill.intervally(10, Duration.ofMinutes(60))))
                .build();

        K key = generateRandomKey();
        ReactiveBucketProxy bucket = proxyManager.builder().build(key, configuration);

        for (int i = 1; i <= 4; i++) {
            VerboseResult<ConsumptionProbe> verboseResult = bucket.asVerbose().tryConsumeAndReturnRemaining(1).block();
            ConsumptionProbe probe = verboseResult.getValue();
            long[] availableTokensPerEachBandwidth = verboseResult.getDiagnostics().getAvailableTokensPerEachBandwidth();
            System.out.println("Remaining tokens = " + probe.getRemainingTokens());
            System.out.println("Tokens per bandwidth = " + Arrays.toString(availableTokensPerEachBandwidth));
            assertEquals(MIN_CAPACITY - i, probe.getRemainingTokens());
            assertEquals(MIN_CAPACITY - i, availableTokensPerEachBandwidth[0]);
            assertEquals(MAX_CAPACITY - i, availableTokensPerEachBandwidth[1]);
        }
    }

    // Tokens are not actually consumed until the Monos are subscribed to, and Monos can be subscribed multiple times.
    @Test
    public void testReactiveMonosAreColdSources() {
        BucketConfiguration configuration = BucketConfiguration.builder()
                .addLimit(Bandwidth.simple(10, Duration.ofDays(1)))
                .build();

        ReactiveBucketProxy bucket = proxyManager.builder().build(key, Mono.just(configuration));

        Mono<Boolean> mono1 = bucket.tryConsume(10);
        Mono<Boolean> mono2 = bucket.tryConsume(2);
        Mono<Void> removeMono = proxyManager.removeProxy(key);

        StepVerifier.create(mono2)
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        StepVerifier.create(mono1)
                .expectSubscription()
                .expectNext(false)
                .expectComplete()
                .verify();

        StepVerifier.create(removeMono)
                .expectSubscription()
                .expectComplete()
                .verify();

        StepVerifier.create(mono1)
                .expectSubscription()
                .expectNext(true)
                .expectComplete()
                .verify();

        StepVerifier.create(mono2)
                .expectSubscription()
                .expectNext(false)
                .expectComplete()
                .verify();
    }
}
