package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Owns the Snowpipe Streaming client and one channel per Kafka topic-partition.
 *
 * <p>Channel names are derived from the topic-partition and are stable across restarts and
 * rebalances, which is what lets Snowflake remember the offset token for that partition and give
 * us exactly-once delivery.
 */
public class StreamingChannelManager implements AutoCloseable {

    private static final Logger LOGGER = LogManager.getLogger(StreamingChannelManager.class);

    /** Snowflake auto-creates this pipe for a table the first time a channel is opened on it. */
    protected static final String DEFAULT_PIPE_SUFFIX = "-STREAMING";

    protected static final int HTTP_REQUEST_TIMEOUT = 408;
    protected static final int HTTP_TOO_MANY_REQUESTS = 429;
    protected static final int HTTP_CONFLICT = 409;
    protected static final int HTTP_INTERNAL_SERVER_ERROR = 500;
    protected static final int HTTP_SERVICE_UNAVAILABLE = 503;

    private final SnowflakeStreamingIngestClient client;
    private final String channelNamePrefix;
    private final int maxRetries;

    private final Map<TopicPartition, SnowflakeStreamingIngestChannel> channels = new HashMap<>();
    private final Map<TopicPartition, String> channelNames = new HashMap<>();

    public StreamingChannelManager(SnowflakeStreamingIngestClient client, String channelNamePrefix,
                                   int maxRetries) {
        this.client = client;
        this.channelNamePrefix = sanitize(channelNamePrefix);
        this.maxRetries = maxRetries;
    }

    /**
     * Builds the SDK client. {@code pipe} may be null, in which case the default pipe is used.
     *
     * <p>{@code password} is the same value v2/v3 hand to JDBC: the programmatic access token.
     * JDBC accepts it as a plain password, but the SDK has no password concept and must be told
     * the authorization type explicitly.
     */
    public static SnowflakeStreamingIngestClient buildClient(String account, String streamingUrl,
                                                            String user, String password,
                                                            String role, String database,
                                                            String schema, String ingestTable,
                                                            String pipe, Integer maxClientLagSeconds) {
        var props = new Properties();
        props.put("account", account);
        props.put("url", streamingUrl);
        props.put("authorization_type", "PAT");
        props.put("personal_access_token", password);
        if (user != null && !user.isBlank()) {
            props.put("user", user);
        }
        if (role != null && !role.isBlank()) {
            props.put("role", role);
        }

        var pipeName = pipe != null && !pipe.isBlank() ? pipe : ingestTable + DEFAULT_PIPE_SUFFIX;
        var clientName = String.format("kc_%s_%s", ingestTable, UUID.randomUUID());

        LOGGER.info("Creating Snowpipe Streaming client {} on {}.{} pipe {}",
                clientName, database, schema, pipeName);

        var builder = SnowflakeStreamingIngestClientFactory
                .builder(clientName, database, schema, pipeName)
                .setProperties(props);

        if (maxClientLagSeconds != null) {
            builder.setParameterOverrides(Map.of("max_client_lag_seconds", maxClientLagSeconds));
        }

        return builder.build();
    }

    /**
     * Opens a channel per partition, reusing whatever offset token Snowflake already has for it.
     *
     * @return the committed offset per partition, absent when the channel has never committed
     */
    public Map<TopicPartition, Long> open(Collection<TopicPartition> partitions) {
        var committed = new LinkedHashMap<TopicPartition, Long>();
        for (TopicPartition tp : partitions) {
            var name = channelName(tp);
            // the single-argument overload keeps the channel's existing offset token; the
            // two-argument one would reset it and break exactly-once on rebalance
            var channel = client.openChannel(name).getChannel();
            channels.put(tp, channel);

            var token = channel.getLatestCommittedOffsetToken();
            LOGGER.info("Opened channel {} for {} with committed offset token {}", name, tp, token);
            parseOffset(token).ifPresent(offset -> committed.put(tp, offset));
        }
        return committed;
    }

    /** Closes the channels of the given partitions and forgets them. */
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            var channel = channels.remove(tp);
            channelNames.remove(tp);
            if (channel == null) {
                continue;
            }
            try {
                channel.close();
            } catch (Exception e) {
                LOGGER.warn("Error while closing channel for {}", tp, e);
            }
        }
    }

    /** Reopens a partition's channel, used to recover from a channel conflict (HTTP 409). */
    public Long reopen(TopicPartition tp) {
        close(List.of(tp));
        return open(List.of(tp)).get(tp);
    }

    /**
     * Appends one row, retrying transient failures with exponential backoff.
     *
     * <p>The offset token is the Kafka offset, so a replay of already-committed records is
     * discarded by Snowflake instead of being written twice.
     */
    public void appendRow(TopicPartition tp, Map<String, Object> row, long kafkaOffset) {
        var channel = channel(tp);
        var offsetToken = Long.toString(kafkaOffset);

        SFException lastError = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                channel.appendRow(row, offsetToken);
                return;
            } catch (SFException e) {
                if (!isRetryable(e)) {
                    throw e;
                }
                lastError = e;
                var backoffMillis = Math.min(30_000L, (long) (100L * Math.pow(2, attempt)));
                LOGGER.warn("Retryable error appending to {} (attempt {}/{}, http {}), backing off {} ms",
                        tp, attempt + 1, maxRetries + 1, e.getHttpStatusCode(), backoffMillis, e);
                try {
                    Thread.sleep(backoffMillis);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying appendRow", ie);
                }
            }
        }
        throw new RuntimeException("Giving up appending to " + tp + " after " + (maxRetries + 1)
                + " attempts", lastError);
    }

    public static boolean isRetryable(SFException e) {
        var status = e.getHttpStatusCode();
        return status == HTTP_REQUEST_TIMEOUT
                || status == HTTP_TOO_MANY_REQUESTS
                || status == HTTP_INTERNAL_SERVER_ERROR
                || status == HTTP_SERVICE_UNAVAILABLE;
    }

    public static boolean isChannelConflict(SFException e) {
        return e.getHttpStatusCode() == HTTP_CONFLICT;
    }

    /** Waits until Snowflake has committed everything up to {@code targetOffset} on that partition. */
    public CompletableFuture<Void> waitForCommit(TopicPartition tp, long targetOffset, Duration timeout) {
        return channel(tp).waitForCommit(
                token -> parseOffset(token).filter(offset -> offset >= targetOffset).isPresent(),
                timeout);
    }

    /**
     * Status of every open channel, fetched in a single call. Carries both the committed offset
     * token and the rejected-row counters, so one round trip covers offset commit and error
     * monitoring.
     */
    public Map<TopicPartition, ChannelStatus> statuses() {
        var result = new LinkedHashMap<TopicPartition, ChannelStatus>();
        if (channels.isEmpty()) {
            return result;
        }

        var names = channels.keySet().stream().map(this::channelName).toList();
        var batch = client.getChannelStatus(names).getChannelStatusBatch();
        for (TopicPartition tp : channels.keySet()) {
            var status = batch.get(channelName(tp));
            if (status != null) {
                result.put(tp, status);
            }
        }
        return result;
    }

    /** Committed offset per partition, absent when the channel has never committed. */
    public Map<TopicPartition, Long> committedOffsets(Map<TopicPartition, ChannelStatus> statuses) {
        var offsets = new LinkedHashMap<TopicPartition, Long>();
        statuses.forEach((tp, status) ->
                parseOffset(status.getLatestCommittedOffsetToken()).ifPresent(offset -> offsets.put(tp, offset)));
        return offsets;
    }

    public ChannelStatus status(TopicPartition tp) {
        return channel(tp).getChannelStatus();
    }

    public SnowflakeStreamingIngestChannel channel(TopicPartition tp) {
        var channel = channels.get(tp);
        if (channel == null) {
            throw new IllegalStateException("No open channel for " + tp
                    + ". Partitions must be opened before use.");
        }
        return channel;
    }

    public String channelName(TopicPartition tp) {
        return channelNames.computeIfAbsent(tp, key -> String.format("%s_%s_%d",
                channelNamePrefix, sanitize(key.topic()), key.partition()));
    }

    private static Optional<Long> parseOffset(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(token.trim()));
        } catch (NumberFormatException e) {
            LOGGER.warn("Ignoring non-numeric offset token '{}'", token);
            return Optional.empty();
        }
    }

    /** Channel names only accept a restricted character set; topic names carry dots. */
    protected static String sanitize(String value) {
        return value == null ? "" : value.replaceAll("[^A-Za-z0-9_-]", "_");
    }

    @Override
    public void close() {
        close(List.copyOf(channels.keySet()));
        try {
            client.close();
        } catch (Exception e) {
            LOGGER.warn("Error while closing Snowpipe Streaming client", e);
        }
    }
}
