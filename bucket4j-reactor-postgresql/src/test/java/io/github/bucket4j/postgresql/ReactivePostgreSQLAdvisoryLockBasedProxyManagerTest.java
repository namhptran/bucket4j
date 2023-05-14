package io.github.bucket4j.postgresql;

import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.github.bucket4j.distributed.proxy.ReactiveProxyManager;
import io.github.bucket4j.distributed.r2dbc.ReactiveSQLProxyConfiguration;
import io.github.bucket4j.tck.AbstractDistributedReactiveBucketTest;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.PostgreSQLContainer;
import reactor.core.publisher.Mono;

import java.text.MessageFormat;
import java.util.concurrent.ThreadLocalRandom;

public class ReactivePostgreSQLAdvisoryLockBasedProxyManagerTest extends AbstractDistributedReactiveBucketTest<Long> {
    private static PostgreSQLContainer container;
    private static ConnectionFactory connectionFactory;
    private static ReactiveProxyManager<Long> proxyManager;

    @BeforeClass
    public static void initializeInstance() {
        container = startPostgreSQLContainer();
        connectionFactory = createR2dbcConnectionFactory(container);
        BucketTableSettings tableSettings = BucketTableSettings.getDefault();
        final String INIT_TABLE_SCRIPT = "CREATE TABLE IF NOT EXISTS {0}({1} BIGINT PRIMARY KEY, {2} BYTEA)";

        Mono.from(connectionFactory.create())
                .flatMapMany(connection -> connection.createStatement(
                        MessageFormat.format(
                                INIT_TABLE_SCRIPT,
                                tableSettings.getTableName(),
                                tableSettings.getIdName(),
                                tableSettings.getStateName()
                        )).execute())
                .blockLast();

        ReactiveSQLProxyConfiguration configuration = ReactiveSQLProxyConfiguration.builder()
                .withClientSideConfig(ClientSideConfig.getDefault())
                .withTableSettings(tableSettings)
                .build(connectionFactory);
        proxyManager = new ReactivePostgreSQLAdvisoryLockBasedProxyManager<>(configuration);
    }

    @Override
    protected ReactiveProxyManager<Long> getReactiveProxyManager() {
        return proxyManager;
    }

    @Override
    protected Long generateRandomKey() {
        return ThreadLocalRandom.current().nextLong(1_000_000_000);
    }

    @AfterClass
    public static void shutdown() {
        if (container != null) {
            container.stop();
        }
    }

    private static ConnectionFactory createR2dbcConnectionFactory(PostgreSQLContainer container) {

        return ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.HOST, container.getHost())
                .option(ConnectionFactoryOptions.PORT, container.getMappedPort(5432))
                .option(ConnectionFactoryOptions.USER, container.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, container.getPassword())
                .build());
    }

    private static PostgreSQLContainer startPostgreSQLContainer() {
        PostgreSQLContainer container = new PostgreSQLContainer();
        container.start();
        return container;
    }
}
