package io.github.bucket4j.distributed.r2dbc;

import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.r2dbc.spi.ConnectionFactory;

public class ReactiveSQLProxyConfiguration {
    private final ConnectionFactory connectionFactory;
    private final ClientSideConfig clientSideConfig;
    private final BucketTableSettings tableSettings;

    public static ReactiveSQLProxyConfigurationBuilder builder() {
        return new ReactiveSQLProxyConfigurationBuilder();
    }

    ReactiveSQLProxyConfiguration(ReactiveSQLProxyConfigurationBuilder builder, ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.clientSideConfig = builder.clientSideConfig;
        this.tableSettings = builder.tableSettings;
    }

    public String getIdName() {
        return tableSettings.getIdName();
    }

    public String getStateName() {
        return tableSettings.getStateName();
    }

    public String getTableName() {
        return tableSettings.getTableName();
    }

    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public ClientSideConfig getClientSideConfig() {
        return clientSideConfig;
    }
}
