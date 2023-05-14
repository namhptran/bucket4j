package io.github.bucket4j.distributed.r2dbc;

import io.github.bucket4j.BucketExceptions;
import io.github.bucket4j.distributed.jdbc.BucketTableSettings;
import io.github.bucket4j.distributed.proxy.ClientSideConfig;
import io.r2dbc.spi.ConnectionFactory;

import java.util.Objects;

public class ReactiveSQLProxyConfigurationBuilder {
    ClientSideConfig clientSideConfig;
    BucketTableSettings tableSettings;

    ReactiveSQLProxyConfigurationBuilder() {
        this.tableSettings = BucketTableSettings.getDefault();
        this.clientSideConfig = ClientSideConfig.getDefault();
    }
    public static ReactiveSQLProxyConfigurationBuilder builder() {
        return new ReactiveSQLProxyConfigurationBuilder();
    }
    public ReactiveSQLProxyConfigurationBuilder withClientSideConfig(ClientSideConfig clientSideConfig) {
        this.clientSideConfig = Objects.requireNonNull(clientSideConfig);
        return this;
    }
    public ReactiveSQLProxyConfigurationBuilder withTableSettings(BucketTableSettings tableSettings) {
        this.tableSettings = Objects.requireNonNull(tableSettings);
        return this;
    }

    public ReactiveSQLProxyConfiguration build(ConnectionFactory connectionFactory) {
        if (connectionFactory == null) {
            throw new BucketExceptions.BucketExecutionException("DataSource cannot be null");
        }
        return new ReactiveSQLProxyConfiguration(this, connectionFactory);
    }
}
