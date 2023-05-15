package io.github.bucket4j.postgresql;

import io.github.bucket4j.distributed.proxy.generic.select_for_update.AbstractSelectForUpdateBasedReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.generic.select_for_update.LockAndGetResult;
import io.github.bucket4j.distributed.proxy.generic.select_for_update.ReactiveSelectForUpdateBasedTransaction;
import io.github.bucket4j.distributed.r2dbc.ReactiveSQLProxyConfiguration;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.Objects;

public class ReactivePostgreSQLSelectForUpdateBasedProxyManager<K> extends AbstractSelectForUpdateBasedReactiveProxyManager<K, Connection> {
    private final ConnectionFactory connectionFactory;
    private final ReactiveSQLProxyConfiguration configuration;
    private final String removeSqlQuery;
    private final String updateSqlQuery;
    private final String insertSqlQuery;
    private final String selectSqlQuery;

    public ReactivePostgreSQLSelectForUpdateBasedProxyManager(ReactiveSQLProxyConfiguration configuration) {
        super(configuration.getClientSideConfig());
        this.connectionFactory = Objects.requireNonNull(configuration.getConnectionFactory());
        this.configuration = configuration;
        this.removeSqlQuery = MessageFormat.format("DELETE FROM {0} WHERE {1} = $1", configuration.getTableName(), configuration.getIdName());
        this.updateSqlQuery = MessageFormat.format("UPDATE {0} SET {1}=$1 WHERE {2}=$2", configuration.getTableName(), configuration.getStateName(), configuration.getIdName());
        this.insertSqlQuery = MessageFormat.format("INSERT INTO {0}({1}, {2}) VALUES($1, null) ON CONFLICT({3}) DO NOTHING",
                configuration.getTableName(), configuration.getIdName(), configuration.getStateName(), configuration.getIdName());
        this.selectSqlQuery = MessageFormat.format("SELECT {0} FROM {1} WHERE {2} = $1 FOR UPDATE", configuration.getStateName(), configuration.getTableName(), configuration.getIdName());
    }

    @Override
    protected ReactiveSelectForUpdateBasedTransaction<Connection> allocateTransaction(K key) {
        return new ReactiveSelectForUpdateBasedTransaction<>() {
            @Override
            public Publisher<Connection> begin() {
                return Mono.from(connectionFactory.create())
                        .flatMap(connection -> Mono.from(connection.beginTransaction())
                                .thenReturn(connection));
            }

            @Override
            public Publisher<?> rollback(Connection connection) {
                return connection.rollbackTransaction();
            }

            @Override
            public Publisher<?> commit(Connection connection) {
                return connection.commitTransaction();
            }

            @Override
            public Publisher<LockAndGetResult> tryLockAndGet(Connection connection) {
                return Mono.from(connection.createStatement(selectSqlQuery)
                        .bind("$1", key)
                        .execute())
                        .flatMapMany(result -> result.map(row -> {
                                    byte[] state = row.get(configuration.getStateName(), byte[].class);
                                    return LockAndGetResult.locked(state);
                                }))
                        .next()
                        .defaultIfEmpty(LockAndGetResult.notLocked());
            }

            @Override
            public Publisher<Boolean> tryInsertEmptyData(Connection connection) {
                return Mono.from(connection.createStatement(insertSqlQuery)
                        .bind("$1", key)
                        .execute())
                        .flatMapMany(Result::getRowsUpdated)
                        .map(i -> i > 0)
                        .defaultIfEmpty(false);
            }

            @Override
            public Publisher<?> update(Connection connection, byte[] data, RemoteBucketState newState) {
                return Mono.from(connection.createStatement(updateSqlQuery)
                                .bind("$1", data)
                                .bind("$2", key)
                                .execute())
                        .flatMapMany(Result::getRowsUpdated);
            }

            @Override
            public Publisher<?> release(Connection connection) {
                return connection.close();
            }
        };
    }

    @Override
    protected Mono<Void> removeReactive(K key) {
        return Mono.from(connectionFactory.create())
                .flatMapMany(c -> c.createStatement(removeSqlQuery).bind("$1", key).execute())
                .flatMap(Result::getRowsUpdated)
                .then();
    }
}
