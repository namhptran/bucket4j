package io.github.bucket4j.distributed;

import io.github.bucket4j.*;
import reactor.core.publisher.Mono;

@Experimental
public interface ReactiveVerboseBucket {

    Mono<VerboseResult<Boolean>> tryConsume(long numTokens);

    Mono<VerboseResult<Long>> consumeIgnoringRateLimits(long tokens);

    Mono<VerboseResult<ConsumptionProbe>> tryConsumeAndReturnRemaining(long numTokens);

    Mono<VerboseResult<EstimationProbe>> estimateAbilityToConsume(long numTokens);

    Mono<VerboseResult<Long>> tryConsumeAsMuchAsPossible();

    Mono<VerboseResult<Long>> tryConsumeAsMuchAsPossible(long limit);

    Mono<VerboseResult<Nothing>> addTokens(long tokensToAdd);

    Mono<VerboseResult<Nothing>> forceAddTokens(long tokensToAdd);

    Mono<VerboseResult<Nothing>> reset();

    Mono<VerboseResult<Nothing>> replaceConfiguration(BucketConfiguration newConfiguration, TokensInheritanceStrategy tokensInheritanceStrategy);

    Mono<VerboseResult<Long>> getAvailableTokens();

}
