package io.github.bucket4j.distributed;

import reactor.core.publisher.Mono;

import java.time.Duration;

public interface ReactiveOptimizationController {

    default Mono<Void> syncImmediately() {
        return syncByCondition(0L, Duration.ZERO);
    }

    Mono<Void> syncByCondition(long unsynchronizedTokens, Duration timeSinceLastSync);

}
