package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.TokensInheritanceStrategy;
import io.github.bucket4j.distributed.ReactiveBucketProxy;
import io.github.bucket4j.distributed.proxy.optimization.Optimization;
import org.reactivestreams.Publisher;

public interface RemoteReactiveBucketBuilder<K> {

    RemoteReactiveBucketBuilder<K> withRecoveryStrategy(RecoveryStrategy recoveryStrategy);

    RemoteReactiveBucketBuilder<K> withOptimization(Optimization optimization);

    RemoteReactiveBucketBuilder<K> withImplicitConfigurationReplacement(long desiredConfigurationVersion, TokensInheritanceStrategy tokensInheritanceStrategy);

    ReactiveBucketProxy build(K key, BucketConfiguration configuration);

    ReactiveBucketProxy build(K key, Publisher<BucketConfiguration> configurationSupplier);
}
