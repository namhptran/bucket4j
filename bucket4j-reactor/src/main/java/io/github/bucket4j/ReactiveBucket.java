package io.github.bucket4j;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

public interface ReactiveBucket {

    default Mono<Boolean> tryConsume(long numTokens, long maxWaitNanos) {
        return tryConsume(numTokens, maxWaitNanos, Schedulers.parallel());
    }

    Mono<Boolean> tryConsume(long numTokens, long maxWaitNanos, Scheduler scheduler);

    default Mono<Boolean> tryConsume(long numTokens, Duration maxWait) {
        return tryConsume(numTokens, maxWait.toNanos(), Schedulers.parallel());
    }

    default Mono<Boolean> tryConsume(long numTokens, Duration maxWait, Scheduler scheduler) {
        return tryConsume(numTokens, maxWait.toNanos(), scheduler);
    }

    default Mono<Void> consume(long numTokens) {
        return consume(numTokens, Schedulers.parallel());
    }

    Mono<Void> consume(long numTokens, Scheduler scheduler);

}
