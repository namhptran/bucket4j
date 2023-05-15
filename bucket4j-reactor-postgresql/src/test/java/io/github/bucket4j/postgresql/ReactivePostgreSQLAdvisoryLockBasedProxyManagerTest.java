package io.github.bucket4j.postgresql;

import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.distributed.r2dbc.ReactiveSQLProxyConfiguration;
import io.github.bucket4j.tck.AbstractDistributedReactiveBucketTest;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.concurrent.ThreadLocalRandom;

@Testcontainers
class ReactivePostgreSQLAdvisoryLockBasedProxyManagerTest extends AbstractDistributedReactiveBucketTest<Long> {
    @Container
    private final PostgreSQLContainer container = new PostgreSQLContainer();
    private ConnectionFactory connectionFactory;

    @BeforeEach
    void initializeInstance() {
        BucketTableSettings tableSettings = BucketTableSettings.getDefault();
        final String INIT_TABLE_SCRIPT = "CREATE TABLE IF NOT EXISTS {0}({1} BIGINT PRIMARY KEY, {2} BYTEA)";

        Mono.from(createR2dbcConnectionFactory(container).create())
                .flatMapMany(connection -> connection.createStatement(
                        MessageFormat.format(
                                INIT_TABLE_SCRIPT,
                                tableSettings.getTableName(),
                                tableSettings.getIdName(),
                                tableSettings.getStateName()
                        )).execute())
                .blockLast();
    }

    @Override
    protected ReactiveProxyManager<Long> getReactiveProxyManager() {
        BucketTableSettings tableSettings = BucketTableSettings.getDefault();
        ReactiveSQLProxyConfiguration configuration = ReactiveSQLProxyConfiguration.builder()
                .withClientSideConfig(ClientSideConfig.getDefault())
                .withTableSettings(tableSettings)
                .build(createR2dbcConnectionFactory(container));
        return new ReactivePostgreSQLAdvisoryLockBasedProxyManager<>(configuration);
    }

    @Override
    protected Long generateRandomKey() {
        return ThreadLocalRandom.current().nextLong(1_000_000_000);
    }

    private ConnectionFactory createR2dbcConnectionFactory(PostgreSQLContainer container) {
        if (connectionFactory != null) {
            return connectionFactory;
        }

        connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.HOST, container.getHost())
                .option(ConnectionFactoryOptions.PORT, container.getMappedPort(5432))
                .option(ConnectionFactoryOptions.USER, container.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, container.getPassword())
                .build());
        return connectionFactory;
    }
}
