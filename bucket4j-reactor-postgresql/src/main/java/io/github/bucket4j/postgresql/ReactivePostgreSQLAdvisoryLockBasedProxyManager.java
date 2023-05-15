package io.github.bucket4j.postgresql;

import io.github.bucket4j.distributed.proxy.generic.pessimistic_locking.AbstractLockBasedReactiveProxyManager;
import io.github.bucket4j.distributed.proxy.generic.pessimistic_locking.ReactiveLockBasedTransaction;
import io.github.bucket4j.distributed.r2dbc.ReactiveSQLProxyConfiguration;
import io.github.bucket4j.distributed.remote.RemoteBucketState;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.Objects;

public class ReactivePostgreSQLAdvisoryLockBasedProxyManager<K> extends AbstractLockBasedReactiveProxyManager<K, Connection> {
    private final ConnectionFactory connectionFactory;
    private final ReactiveSQLProxyConfiguration configuration;
    private final String removeSqlQuery;
    private final String updateSqlQuery;
    private final String insertSqlQuery;
    private final String selectSqlQuery;

    public ReactivePostgreSQLAdvisoryLockBasedProxyManager(ReactiveSQLProxyConfiguration configuration) {
        super(configuration.getClientSideConfig());
        this.connectionFactory = Objects.requireNonNull(configuration.getConnectionFactory());
        this.configuration = configuration;
        this.removeSqlQuery = MessageFormat.format("DELETE FROM {0} WHERE {1} = $1", configuration.getTableName(), configuration.getIdName());
        this.updateSqlQuery = MessageFormat.format("UPDATE {0} SET {1}=$1 WHERE {2}=$2", configuration.getTableName(), configuration.getStateName(), configuration.getIdName());
        this.insertSqlQuery = MessageFormat.format("INSERT INTO {0}({1}, {2}) VALUES($1, $2)", configuration.getTableName(), configuration.getIdName(), configuration.getStateName());
        this.selectSqlQuery = MessageFormat.format("SELECT {0} FROM {1} WHERE {2} = $1", configuration.getStateName(), configuration.getTableName(), configuration.getIdName());
    }

    @Override
    protected ReactiveLockBasedTransaction<Connection> allocateTransaction(K key) {
        return new ReactiveLockBasedTransaction<>() {
            @Override
            public Publisher<Connection> begin() {
                return Mono.from(connectionFactory.create())
                        .flatMap(connection -> Mono.from(connection.beginTransaction())
                                .thenReturn(connection));
            }

            @Override
            public Publisher<byte[]> lockAndGet(Connection connection) {
                String lockSQL = "SELECT pg_advisory_xact_lock($1)";

                return Mono.from(connection.createStatement(lockSQL).bind("$1", key).execute())
                        .flatMapMany(Result::getRowsUpdated)
                        .thenMany(connection.createStatement(selectSqlQuery).bind("$1", key).execute())
                        .flatMap(result -> result.map(row -> row.get(configuration.getStateName(), byte[].class)));
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

            @Override
            public Publisher<?> create(Connection connection, byte[] data, RemoteBucketState newState) {
                return Mono.from(connection.createStatement(insertSqlQuery)
                        .bind("$1", key)
                        .bind("$2", data)
                        .execute())
                        .flatMapMany(Result::getRowsUpdated);
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
            public Publisher<?> unlock(Connection connection) {
                // advisory lock implicitly unlocked on commit/rollback
                return Mono.empty();
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
