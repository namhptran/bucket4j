package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.BucketConfiguration;
import reactor.core.publisher.Mono;

public interface ReactiveProxyManager<K> {

    RemoteReactiveBucketBuilder<K> builder();

    Mono<Void> removeProxy(K key);

    Mono<BucketConfiguration> getProxyConfiguration(K key);
}
