package br.com.datastreambrasil.v4;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.SFException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger LOGGER = LogManager.getLogger(SnowflakeSinkTask.class);

    protected static final String INGEST_SUFFIX = "_INGEST";

    private boolean ingestionOnly;
    private boolean failOnRowError;
    private Duration commitWaitTimeout;
    private String tableName;
    private String ingestTableName;
    private String databaseName;
    private String schemaName;
    private String account;
    private String streamingUrl;

    private SnowflakeJdbc jdbc;
    private StreamingChannelManager channels;
    private RowMapper rowMapper;
    private MergeExecutor mergeExecutor;
    private Scheduler scheduler;

    private List<String> pks = List.of();

    /** Offsets already durable in Snowflake when a partition was opened, used to drop replays. */
    private final Map<TopicPartition, Long> committedAtOpen = new HashMap<>();
    /** Rejected-row counters last reported per partition, to log only the delta. */
    private final Map<TopicPartition, Long> rowErrorCounts = new HashMap<>();

    // merge-mode bookkeeping, only touched from the single Connect worker thread
    private String currentBlockId = UUID.randomUUID().toString();
    private final Map<TopicPartition, Long> appendedMax = new HashMap<>();
    private boolean blockHasUpserts;
    private boolean blockHasDeletes;

    @Override
    public String version() {
        return SnowflakeSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> map) {
        var config = new AbstractConfig(SnowflakeSinkConnector.CONFIG_DEF, map);

        var profile = config.getString(SnowflakeSinkConnector.CFG_PROFILE);
        if (!SnowflakeSinkConnector.PROFILE_CDC_SCHEMA.equals(profile)) {
            throw new ConnectException("Unknown profile: " + profile);
        }

        ingestionOnly = config.getBoolean(SnowflakeSinkConnector.CFG_INGESTION_ONLY);
        failOnRowError = config.getBoolean(SnowflakeSinkConnector.CFG_FAIL_ON_ROW_ERROR);
        commitWaitTimeout = Duration.parse(config.getString(SnowflakeSinkConnector.CFG_COMMIT_WAIT_TIMEOUT));
        tableName = config.getString(SnowflakeSinkConnector.CFG_TABLE_NAME);
        ingestTableName = tableName + INGEST_SUFFIX;
        resolveSnowflakeLocation(config);

        jdbc = createJdbc(config);

        var columnsIngestTable = jdbc.columnsOf(ingestTableName,
                config.getBoolean(SnowflakeSinkConnector.CFG_FIND_COLUMNS_IN_METADATA),
                config.getList(SnowflakeSinkConnector.CFG_IGNORE_COLUMNS));

        var ingestOnlyColumns = config.getList(SnowflakeSinkConnector.CFG_EXCLUDE_INGEST_ADDITIONAL_FIELDS)
                .stream().map(c -> c.toUpperCase(Locale.ROOT)).toList();
        var columnsFinalTable = columnsIngestTable.stream()
                .filter(c -> !ingestOnlyColumns.contains(c.toUpperCase(Locale.ROOT)))
                .toList();

        rowMapper = new RowMapper(columnsIngestTable,
                config.getList(SnowflakeSinkConnector.CFG_TIMESTAMP_FIELDS_CONVERT),
                config.getList(SnowflakeSinkConnector.CFG_DATE_FIELDS_CONVERT),
                config.getList(SnowflakeSinkConnector.CFG_TIME_FIELDS_CONVERT));

        channels = createChannelManager(config, map);

        if (ingestionOnly) {
            // nothing else needs Snowflake over JDBC: no MERGE, no cleanup job. Releasing the
            // session now means no warehouse or session footprint at steady state.
            LOGGER.info("ingestion_only is on: streaming into {} only, no MERGE and no cleanup job",
                    ingestTableName);
            jdbc.close();
        } else {
            mergeExecutor = new MergeExecutor(jdbc.connection(), tableName, ingestTableName,
                    columnsFinalTable, ingestOnlyColumns);
            startCleanUpJob(config);
        }
    }

    /**
     * JDBC takes the account, database and schema from the URL, like v3 does. The streaming SDK
     * cannot - it wants them as arguments - so read them back out of that same URL rather than
     * asking the user to repeat what the connection string already says.
     */
    private void resolveSnowflakeLocation(AbstractConfig config) {
        var url = config.getString(SnowflakeSinkConnector.CFG_URL);

        account = SnowflakeJdbc.accountFrom(url)
                .orElseThrow(() -> new ConnectException(
                        "Could not determine the account from " + SnowflakeSinkConnector.CFG_URL
                                + ". Expected something like "
                                + "jdbc:snowflake://<account>.snowflakecomputing.com"));

        streamingUrl = config.getString(SnowflakeSinkConnector.CFG_STREAMING_URL);
        if (streamingUrl == null || streamingUrl.isBlank()) {
            streamingUrl = SnowflakeJdbc.hostFrom(url)
                    .map(host -> String.format("https://%s:443", host))
                    .orElseThrow(() -> new ConnectException(
                            "Could not determine the streaming endpoint from "
                                    + SnowflakeSinkConnector.CFG_URL + "; set '"
                                    + SnowflakeSinkConnector.CFG_STREAMING_URL + "' explicitly."));
        }

        databaseName = SnowflakeJdbc.urlParam(url, "db", "database")
                .orElseThrow(() -> new ConnectException(
                        "Could not determine the database: add 'db=<DATABASE>' to the "
                                + SnowflakeSinkConnector.CFG_URL + " connection string."));

        schemaName = config.getString(SnowflakeSinkConnector.CFG_SCHEMA_NAME);
        if (schemaName == null || schemaName.isBlank()) {
            schemaName = SnowflakeJdbc.urlParam(url, "schema")
                    .orElseThrow(() -> new ConnectException(
                            "Could not determine the schema: set the '"
                                    + SnowflakeSinkConnector.CFG_SCHEMA_NAME
                                    + "' config or add 'schema=<SCHEMA>' to the "
                                    + SnowflakeSinkConnector.CFG_URL + " connection string."));
        }

        LOGGER.info("Streaming into {}.{}.{} on account {} via {}",
                databaseName, schemaName, ingestTableName, account, streamingUrl);
    }

    /** Overridable so tests can supply a mocked connection. */
    protected SnowflakeJdbc createJdbc(AbstractConfig config) {
        return new SnowflakeJdbc(
                config.getString(SnowflakeSinkConnector.CFG_URL),
                config.getString(SnowflakeSinkConnector.CFG_USER),
                config.getPassword(SnowflakeSinkConnector.CFG_PASSWORD).value(),
                schemaName);
    }

    /** Overridable so tests can supply a mocked streaming client. */
    protected StreamingChannelManager createChannelManager(AbstractConfig config, Map<String, String> map) {
        var client = StreamingChannelManager.buildClient(
                account,
                streamingUrl,
                config.getString(SnowflakeSinkConnector.CFG_USER),
                config.getPassword(SnowflakeSinkConnector.CFG_PASSWORD).value(),
                config.getString(SnowflakeSinkConnector.CFG_ROLE),
                databaseName,
                schemaName,
                ingestTableName,
                config.getString(SnowflakeSinkConnector.CFG_PIPE),
                config.getInt(SnowflakeSinkConnector.CFG_MAX_CLIENT_LAG_SECONDS));

        var prefix = config.getString(SnowflakeSinkConnector.CFG_CHANNEL_NAME_PREFIX);
        if (prefix == null || prefix.isBlank()) {
            prefix = map.getOrDefault("name", ingestTableName);
        }
        return new StreamingChannelManager(client, prefix,
                config.getInt(SnowflakeSinkConnector.CFG_APPEND_MAX_RETRIES));
    }

    @Override
    public void open(Collection<TopicPartition> partitions) {
        var committed = channels.open(partitions);
        committedAtOpen.putAll(committed);

        // getRowsErrorCount() counts the channel's whole lifetime, not this assignment's. Seed the
        // baseline from what Snowflake already reports, otherwise every restart would replay old
        // rejections as if they were new - and with fail_on_row_error that is a crash loop.
        channels.statuses().forEach((tp, status) -> {
            if (partitions.contains(tp)) {
                rowErrorCounts.put(tp, status.getRowsErrorCount());
                if (status.getRowsErrorCount() > 0) {
                    LOGGER.warn("Channel for {} already reports {} rejected row(s) from before this "
                            + "assignment; only newer rejections will be reported",
                            tp, status.getRowsErrorCount());
                }
            }
        });

        if (!committed.isEmpty()) {
            // resume exactly where Snowflake left off instead of where Kafka thinks we are
            var rewind = new LinkedHashMap<TopicPartition, Long>();
            committed.forEach((tp, offset) -> rewind.put(tp, offset + 1));
            LOGGER.info("Rewinding consumer to Snowflake committed offsets: {}", rewind);
            context.offset(rewind);
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        partitions.forEach(tp -> {
            committedAtOpen.remove(tp);
            rowErrorCounts.remove(tp);
            appendedMax.remove(tp);
        });
        if (appendedMax.isEmpty()) {
            // the whole block went away with the revoked partitions; its rows stay in the ingest
            // table with their block id and are superseded once the records are redelivered
            blockHasUpserts = false;
            blockHasDeletes = false;
        }
        channels.close(partitions);
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        if (collection.isEmpty()) {
            return;
        }
        LOGGER.debug("PUT - {} records", collection.size());

        for (SinkRecord record : collection) {
            var tp = new TopicPartition(record.topic(), record.kafkaPartition());

            var alreadyCommitted = committedAtOpen.get(tp);
            if (alreadyCommitted != null && record.kafkaOffset() <= alreadyCommitted) {
                // Snowflake already has this record; the rewind in open() may not have taken
                // effect for records already buffered in the consumer
                continue;
            }

            var row = rowMapper.toRow(record, currentBlockId);
            if (pks.isEmpty()) {
                pks = rowMapper.extractPk(record);
                LOGGER.info("Primary key columns resolved from record key schema: {}", pks);
            }

            try {
                channels.appendRow(tp, row, record.kafkaOffset());
            } catch (SFException e) {
                if (StreamingChannelManager.isChannelConflict(e)) {
                    throw recoverFromConflict(tp, e);
                }
                throw e;
            }

            if (!ingestionOnly) {
                appendedMax.merge(tp, record.kafkaOffset(), Math::max);
                if (RowMapper.DebeziumOperation.d.name().equalsIgnoreCase(rowMapper.operationOf(record))) {
                    blockHasDeletes = true;
                } else {
                    blockHasUpserts = true;
                }
            }
        }
    }

    /**
     * A 409 means another client took the channel over. Reopen it, rewind to whatever Snowflake
     * has committed and let Connect redeliver.
     */
    private RetriableException recoverFromConflict(TopicPartition tp, SFException cause) {
        LOGGER.warn("Channel conflict on {}, reopening and rewinding", tp, cause);
        var committed = channels.reopen(tp);
        if (committed != null) {
            committedAtOpen.put(tp, committed);
            context.offset(Map.of(tp, committed + 1));
        } else {
            committedAtOpen.remove(tp);
        }
        appendedMax.remove(tp);
        return new RetriableException("Channel conflict on " + tp + ", reopened and rewound", cause);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {

        if (!ingestionOnly) {
            runMergeCycle();
        }

        var statuses = channels.statuses();
        reportRowErrors(statuses);

        var result = new HashMap<TopicPartition, OffsetAndMetadata>();
        channels.committedOffsets(statuses).forEach((tp, committed) -> {
            var current = currentOffsets.get(tp);
            if (current == null) {
                return;
            }
            // never commit past what Connect actually handed us
            var offset = Math.min(committed + 1, current.offset());
            result.put(tp, new OffsetAndMetadata(offset));
        });
        return result;
    }

    /**
     * Waits for the current block to be durable, then applies it to the final table. Bookkeeping
     * is only rotated after both statements succeed, so a failure retries the same block on the
     * next cycle rather than losing it.
     */
    private void runMergeCycle() {
        if (appendedMax.isEmpty()) {
            return;
        }

        var blockId = currentBlockId;
        var pending = Map.copyOf(appendedMax);

        var futures = pending.entrySet().stream()
                .map(entry -> channels.waitForCommit(entry.getKey(), entry.getValue(), commitWaitTimeout))
                .toArray(CompletableFuture[]::new);
        try {
            CompletableFuture.allOf(futures).get(commitWaitTimeout.toMillis() + 5_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted while waiting for Snowflake to commit block " + blockId, e);
        } catch (Exception e) {
            throw new RetriableException("Timed out waiting for Snowflake to commit block " + blockId
                    + "; will retry on the next commit", e);
        }

        if (blockHasUpserts) {
            mergeExecutor.merge(blockId, pks);
        }
        if (blockHasDeletes) {
            mergeExecutor.deleteRows(blockId, pks);
        }

        currentBlockId = UUID.randomUUID().toString();
        appendedMax.clear();
        blockHasUpserts = false;
        blockHasDeletes = false;
    }

    /**
     * The pipe only supports {@code ON_ERROR = CONTINUE}, so rows Snowflake refuses are dropped
     * into the error table instead of failing the append. Without this they are invisible.
     */
    private void reportRowErrors(Map<TopicPartition, ChannelStatus> statuses) {
        var offenders = new ArrayList<String>();
        for (var entry : statuses.entrySet()) {
            var status = entry.getValue();
            var errors = status.getRowsErrorCount();
            var previous = rowErrorCounts.getOrDefault(entry.getKey(), 0L);
            if (errors > previous) {
                rowErrorCounts.put(entry.getKey(), errors);
                LOGGER.error("Snowflake rejected {} row(s) on {} (total {}), last error: {}",
                        errors - previous, entry.getKey(), errors, status.getLastErrorMessage());
                offenders.add(entry.getKey() + " (" + errors + " rows, " + status.getLastErrorMessage() + ")");
            }
        }
        if (failOnRowError && !offenders.isEmpty()) {
            throw new ConnectException("Snowflake rejected rows and fail_on_row_error is set: "
                    + String.join("; ", offenders));
        }
    }

    protected void startCleanUpJob(AbstractConfig config) {
        if (config.getBoolean(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DISABLE)) {
            LOGGER.warn("Cleanup job is disabled, skipping job creation.");
            return;
        }

        var duration = Duration.parse(config.getString(SnowflakeSinkConnector.CFG_JOB_CLEANUP_DURATION));
        LOGGER.info("Cleanup job will run every {} seconds", duration.toSeconds());

        var jobData = new HashMap<String, Object>();
        jobData.put(CleanupJob.SNOWFLAKE_CONNECTION, jdbc.connection());
        jobData.put(CleanupJob.INGEST_TABLE_NAME, ingestTableName);
        jobData.put(CleanupJob.RETENTION_HOURS,
                config.getInt(SnowflakeSinkConnector.CFG_JOB_CLEANUP_RETENTION_HOURS));

        var props = new Properties();
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, String.format("cleanup_%s", UUID.randomUUID()));
        props.setProperty("org.quartz.threadPool.threadCount", "1");

        try {
            scheduler = new StdSchedulerFactory(props).getScheduler();
            var job = JobBuilder.newJob(CleanupJob.class).withIdentity("cleanupjob")
                    .setJobData(new JobDataMap(jobData))
                    .build();
            var trigger = TriggerBuilder.newTrigger().withIdentity("trigger_cleanupjob")
                    .withSchedule(SimpleScheduleBuilder.repeatSecondlyForever((int) duration.getSeconds()))
                    .build();
            scheduler.scheduleJob(job, trigger);
            scheduler.start();
        } catch (SchedulerException e) {
            LOGGER.error("Error starting cleanup job", e);
            throw new ConnectException("Error starting cleanup job", e);
        }
    }

    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOGGER.error("Can not shutdown quartz scheduler", e);
            }
        }
        if (channels != null) {
            channels.close();
        }
        if (jdbc != null) {
            jdbc.close();
        }
    }
}
