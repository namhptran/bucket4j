package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.*;
import io.github.bucket4j.distributed.ReactiveBucketProxy;
import io.github.bucket4j.distributed.ReactiveOptimizationController;
import io.github.bucket4j.distributed.ReactiveVerboseBucket;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import io.github.bucket4j.distributed.remote.RemoteVerboseResult;
import io.github.bucket4j.distributed.remote.commands.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.github.bucket4j.LimitChecker.*;

public class DefaultReactiveBucketProxy implements ReactiveBucketProxy, ReactiveOptimizationController, ReactiveBucket {

    private final ReactiveCommandExecutor commandExecutor;
    private final RecoveryStrategy recoveryStrategy;
    private final Mono<BucketConfiguration> configurationMono;
    private final BucketListener listener;
    private final ImplicitConfigurationReplacement implicitConfigurationReplacement;
    private final AtomicBoolean wasInitialized;

    private final SchedulingBucket schedulerView = new SchedulingBucket() {
        @Override
        public CompletableFuture<Boolean> tryConsume(long numTokens, long maxWaitNanos, ScheduledExecutorService scheduler) {
            return DefaultReactiveBucketProxy.this.tryConsume(numTokens, maxWaitNanos, Schedulers.fromExecutorService(scheduler)).toFuture();
        }

        @Override
        public CompletableFuture<Void> consume(long numTokens, ScheduledExecutorService scheduler) {
            return DefaultReactiveBucketProxy.this.consume(numTokens, Schedulers.fromExecutorService(scheduler)).toFuture();
        }
    };

    @Override
    public ReactiveVerboseBucket asVerbose() {
        return reactiveVerboseView;
    }

    @Override
    public SchedulingBucket asScheduler() {
        return schedulerView;
    }

    @Override
    public DefaultReactiveBucketProxy toListenable(BucketListener listener) {
        return new DefaultReactiveBucketProxy(commandExecutor, recoveryStrategy, configurationMono, implicitConfigurationReplacement, wasInitialized, listener);
    }

    @Override
    public ReactiveBucket asReactive() {
        return this;
    }

    public DefaultReactiveBucketProxy(ReactiveCommandExecutor commandExecutor, RecoveryStrategy recoveryStrategy, Publisher<BucketConfiguration> configurationPublisher, ImplicitConfigurationReplacement implicitConfigurationReplacement) {
        this(commandExecutor, recoveryStrategy, configurationPublisher, implicitConfigurationReplacement, new AtomicBoolean(false), BucketListener.NOPE);
    }

    private DefaultReactiveBucketProxy(ReactiveCommandExecutor commandExecutor, RecoveryStrategy recoveryStrategy, Publisher<BucketConfiguration> configurationPublisher, ImplicitConfigurationReplacement implicitConfigurationReplacement, AtomicBoolean wasInitialized, BucketListener listener) {
        this.commandExecutor = Objects.requireNonNull(commandExecutor);
        this.recoveryStrategy = recoveryStrategy;
        this.configurationMono = Mono.from(configurationPublisher);
        this.implicitConfigurationReplacement = implicitConfigurationReplacement;
        this.wasInitialized = wasInitialized;

        if (listener == null) {
            throw BucketExceptions.nullListener();
        }

        this.listener = listener;
    }

    private final ReactiveVerboseBucket reactiveVerboseView = new ReactiveVerboseBucket() {
        @Override
        public Mono<VerboseResult<Boolean>> tryConsume(long tokensToConsume) {
            checkTokensToConsume(tokensToConsume);

            VerboseCommand<Boolean> command = new VerboseCommand<>(new TryConsumeCommand(tokensToConsume));
            return executeReactive(command).doOnNext(consumed -> {
                if (consumed.getValue()) {
                    listener.onConsumed(tokensToConsume);
                } else {
                    listener.onRejected(tokensToConsume);
                }
            }).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Long>> consumeIgnoringRateLimits(long tokensToConsume) {
            checkTokensToConsume(tokensToConsume);
            VerboseCommand<Long> command = new VerboseCommand<>(new ConsumeIgnoringRateLimitsCommand(tokensToConsume));
            return executeReactive(command).doOnNext(penaltyNanos -> {
                if (penaltyNanos.getValue() == INFINITY_DURATION) {
                    throw BucketExceptions.reservationOverflow();
                }
                listener.onConsumed(tokensToConsume);
            }).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<ConsumptionProbe>> tryConsumeAndReturnRemaining(long tokensToConsume) {
            checkTokensToConsume(tokensToConsume);

            VerboseCommand<ConsumptionProbe> command = new VerboseCommand<>(new TryConsumeAndReturnRemainingTokensCommand(tokensToConsume));
            return executeReactive(command).doOnNext(probe -> {
                if (probe.getValue().isConsumed()) {
                    listener.onConsumed(tokensToConsume);
                } else {
                    listener.onRejected(tokensToConsume);
                }
            }).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<EstimationProbe>> estimateAbilityToConsume(long numTokens) {
            checkTokensToConsume(numTokens);
            return executeReactive(new VerboseCommand<>(new EstimateAbilityToConsumeCommand(numTokens)))
                    .map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Long>> tryConsumeAsMuchAsPossible() {
            return tryConsumeAsMuchAsPossible(UNLIMITED_AMOUNT);
        }

        @Override
        public Mono<VerboseResult<Long>> tryConsumeAsMuchAsPossible(long limit) {
            checkTokensToConsume(limit);

            VerboseCommand<Long> verboseCommand = new VerboseCommand<>(new ConsumeAsMuchAsPossibleCommand(limit));
            return executeReactive(verboseCommand).doOnNext(consumedTokens -> {
                long actuallyConsumedTokens = consumedTokens.getValue();
                if (actuallyConsumedTokens > 0) {
                    listener.onConsumed(actuallyConsumedTokens);
                }
            }).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Nothing>> addTokens(long tokensToAdd) {
            checkTokensToAdd(tokensToAdd);
            VerboseCommand<Nothing> verboseCommand = new VerboseCommand<>(new AddTokensCommand(tokensToAdd));
            return executeReactive(verboseCommand).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Nothing>> forceAddTokens(long tokensToAdd) {
            checkTokensToAdd(tokensToAdd);
            VerboseCommand<Nothing> verboseCommand = new VerboseCommand<>(new ForceAddTokensCommand(tokensToAdd));
            return executeReactive(verboseCommand).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Nothing>> reset() {
            VerboseCommand<Nothing> verboseCommand = new VerboseCommand<>(new ResetCommand());
            return executeReactive(verboseCommand).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Nothing>> replaceConfiguration(BucketConfiguration newConfiguration, TokensInheritanceStrategy tokensInheritanceStrategy) {
            checkConfiguration(newConfiguration);
            checkMigrationMode(tokensInheritanceStrategy);
            VerboseCommand<Nothing> command = new VerboseCommand<>(new ReplaceConfigurationCommand(newConfiguration, tokensInheritanceStrategy));
            return executeReactive(command).map(RemoteVerboseResult::asLocal);
        }

        @Override
        public Mono<VerboseResult<Long>> getAvailableTokens() {
            VerboseCommand<Long> command = new VerboseCommand<>(new GetAvailableTokensCommand());
            return executeReactive(command).map(RemoteVerboseResult::asLocal);
        }
    };

    @Override
    public Mono<Long> consumeIgnoringRateLimits(long tokensToConsume) {
        checkTokensToConsume(tokensToConsume);
        return executeReactive(new ConsumeIgnoringRateLimitsCommand(tokensToConsume)).doOnNext(penaltyNanos -> {
            if (penaltyNanos == INFINITY_DURATION) {
                throw BucketExceptions.reservationOverflow();
            }
            listener.onConsumed(tokensToConsume);
        });
    }

    @Override
    public Mono<Boolean> tryConsume(long tokensToConsume) {
        checkTokensToConsume(tokensToConsume);

        return executeReactive(new TryConsumeCommand(tokensToConsume)).doOnNext(consumed -> {
            if (consumed) {
                listener.onConsumed(tokensToConsume);
            } else {
                listener.onRejected(tokensToConsume);
            }
        });
    }

    @Override
    public Mono<ConsumptionProbe> tryConsumeAndReturnRemaining(long tokensToConsume) {
        checkTokensToConsume(tokensToConsume);

        return executeReactive(new TryConsumeAndReturnRemainingTokensCommand(tokensToConsume)).doOnNext(probe -> {
            if (probe.isConsumed()) {
                listener.onConsumed(tokensToConsume);
            } else {
                listener.onRejected(tokensToConsume);
            }
        });
    }

    @Override
    public Mono<EstimationProbe> estimateAbilityToConsume(long numTokens) {
        checkTokensToConsume(numTokens);
        return executeReactive(new EstimateAbilityToConsumeCommand(numTokens));
    }

    @Override
    public Mono<Long> tryConsumeAsMuchAsPossible() {
        return executeReactive(new ConsumeAsMuchAsPossibleCommand(UNLIMITED_AMOUNT)).doOnNext(consumedTokens -> {
            if (consumedTokens > 0) {
                listener.onConsumed(consumedTokens);
            }
        });
    }

    @Override
    public Mono<Long> tryConsumeAsMuchAsPossible(long limit) {
        checkTokensToConsume(limit);

        return executeReactive(new ConsumeAsMuchAsPossibleCommand(limit)).map(consumedTokens -> {
            if (consumedTokens > 0) {
                listener.onConsumed(consumedTokens);
            }
            return consumedTokens;
        });
    }

    @Override
    public Mono<Boolean> tryConsume(long tokensToConsume, long maxWaitTimeNanos, Scheduler scheduler) {
        checkMaxWaitTime(maxWaitTimeNanos);
        checkTokensToConsume(tokensToConsume);
        //checkScheduler(scheduler);
        ReserveAndCalculateTimeToSleepCommand consumeCommand = new ReserveAndCalculateTimeToSleepCommand(tokensToConsume, maxWaitTimeNanos);

        return executeReactive(consumeCommand).flatMap(nanosToSleep -> {
            if (nanosToSleep == INFINITY_DURATION) {
                listener.onRejected(tokensToConsume);
                return Mono.just(false);
            }
            if (nanosToSleep == 0L) {
                listener.onConsumed(tokensToConsume);
                return Mono.just(true);
            }
            listener.onConsumed(tokensToConsume);
            listener.onDelayed(nanosToSleep);
            return Mono.delay(Duration.ofNanos(nanosToSleep), scheduler).map(x -> true);
        });
    }

    @Override
    public Mono<Void> consume(long tokensToConsume, Scheduler scheduler) {
        checkTokensToConsume(tokensToConsume);
        //checkScheduler(scheduler);
        ReserveAndCalculateTimeToSleepCommand consumeCommand = new ReserveAndCalculateTimeToSleepCommand(tokensToConsume, INFINITY_DURATION);

        return executeReactive(consumeCommand).flatMap(nanosToSleep -> {
            if (nanosToSleep == INFINITY_DURATION) {
                return Mono.error(BucketExceptions.reservationOverflow());
            }
            if (nanosToSleep == 0L) {
                listener.onConsumed(tokensToConsume);
                return Mono.empty();
            }
            try {
                listener.onConsumed(tokensToConsume);
                listener.onDelayed(nanosToSleep);
                return Mono.delay(Duration.ofNanos(nanosToSleep), scheduler).then();
            } catch (Throwable t) {
                return Mono.error(t);
            }
        });
    }

    @Override
    public Mono<Void> replaceConfiguration(BucketConfiguration newConfiguration, TokensInheritanceStrategy tokensInheritanceStrategy) {
        checkConfiguration(newConfiguration);
        checkMigrationMode(tokensInheritanceStrategy);
        ReplaceConfigurationCommand replaceConfigCommand = new ReplaceConfigurationCommand(newConfiguration, tokensInheritanceStrategy);
        return executeReactive(replaceConfigCommand).then();
    }

    @Override
    public Mono<Void> addTokens(long tokensToAdd) {
        checkTokensToAdd(tokensToAdd);
        return executeReactive(new AddTokensCommand(tokensToAdd)).then();
    }

    @Override
    public Mono<Void> forceAddTokens(long tokensToAdd) {
        checkTokensToAdd(tokensToAdd);
        return executeReactive(new ForceAddTokensCommand(tokensToAdd)).then();
    }

    @Override
    public Mono<Void> reset() {
        return executeReactive(new ResetCommand()).then();
    }

    @Override
    public Mono<Long> getAvailableTokens() {
        return executeReactive(new GetAvailableTokensCommand());
    }

    @Override
    public ReactiveOptimizationController getOptimizationController() {
        return this;
    }

    @Override
    public Mono<Void> syncByCondition(long unsynchronizedTokens, Duration timeSinceLastSync) {
        return executeReactive(new SyncCommand(unsynchronizedTokens, timeSinceLastSync.toNanos())).then();
    }

    private <T> Mono<T> executeReactive(RemoteCommand<T> command) {
        RemoteCommand<T> commandToExecute = implicitConfigurationReplacement == null ? command :
                new CheckConfigurationVersionAndExecuteCommand<>(command, implicitConfigurationReplacement.getDesiredConfigurationVersion());

        boolean wasInitializedBeforeExecution = wasInitialized.get();
        return commandExecutor.executeReactive(commandToExecute)
                .flatMap(cmdResult -> {
                    if (!cmdResult.isBucketNotFound() && !cmdResult.isConfigurationNeedToBeReplaced()) {
                        T resultDate = cmdResult.getData();
                        return Mono.just(resultDate);
                    }

                    // the bucket was removed or lost, or not initialized yet, or needs to upgrade configuration
                    if (cmdResult.isBucketNotFound() && recoveryStrategy == RecoveryStrategy.THROW_BUCKET_NOT_FOUND_EXCEPTION && wasInitializedBeforeExecution) {
                        return Mono.error(new BucketNotFoundException());
                    }

                    return configurationMono.flatMap(configuration -> {
                        if (configuration == null) {
                            return Mono.error(BucketExceptions.nullConfiguration());
                        }
                        RemoteCommand<T> initAndExecuteCommand = implicitConfigurationReplacement == null ?
                                new CreateInitialStateAndExecuteCommand<>(configuration, command) :
                                new CreateInitialStateWithVersionOrReplaceConfigurationAndExecuteCommand<>(configuration, command, implicitConfigurationReplacement.getDesiredConfigurationVersion(), implicitConfigurationReplacement.getTokensInheritanceStrategy());

                        return commandExecutor.executeReactive(initAndExecuteCommand).map(initAndExecuteCmdResult -> {
                            wasInitialized.set(true);
                            return initAndExecuteCmdResult.getData();
                        });
                    });
                });
    }
}
