package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.Experimental;
import reactor.core.publisher.Mono;

@Experimental
public interface ReactiveProxyManager<K> {

    RemoteReactiveBucketBuilder<K> builder();

    Mono<Void> removeProxy(K key);

    Mono<BucketConfiguration> getProxyConfiguration(K key);

    ProxyManager<K> asProxyManager();
}
