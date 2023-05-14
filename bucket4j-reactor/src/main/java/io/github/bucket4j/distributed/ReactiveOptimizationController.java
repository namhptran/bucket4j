package io.github.bucket4j.distributed;

import io.github.bucket4j.Experimental;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Experimental
public interface ReactiveOptimizationController {

    default Mono<Void> syncImmediately() {
        return syncByCondition(0L, Duration.ZERO);
    }

    Mono<Void> syncByCondition(long unsynchronizedTokens, Duration timeSinceLastSync);

}
