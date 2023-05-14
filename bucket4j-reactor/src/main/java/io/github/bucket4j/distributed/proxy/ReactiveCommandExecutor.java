package io.github.bucket4j.distributed.proxy;

import io.github.bucket4j.distributed.remote.CommandResult;
import io.github.bucket4j.distributed.remote.RemoteCommand;
import reactor.core.publisher.Mono;

public interface ReactiveCommandExecutor {

    <T> Mono<CommandResult<T>> execute(RemoteCommand<T> command);
}
