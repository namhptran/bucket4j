package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.Experimental;
import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import reactor.core.publisher.Mono;

@Experimental
public interface ReactiveCommandExecutor {

    <T> Mono<CommandResult<T>> executeReactive(RemoteCommand<T> command);
}
