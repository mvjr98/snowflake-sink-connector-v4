package br.com.datastreambrasil.v4;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Applies a committed block of the ingest table to the final table, used only when
 * {@code ingestion_only} is false.
 *
 * <p>v3 could rely on its in-memory buffer being keyed by primary key, so a block never held two
 * rows for the same key. v4 streams every event, so the block must be deduplicated in SQL: the
 * {@code QUALIFY ROW_NUMBER()} runs <em>before</em> the {@code ih_op} filter, which both avoids
 * Snowflake's "Duplicate row detected during DML action" error and stops an update from
 * resurrecting a key whose last operation in the block was a delete.
 *
 * <p>The ordering assumes a given primary key always lands in the same Kafka partition, which
 * holds for Debezium topics keyed by primary key.
 */
public class MergeExecutor {

    private static final Logger LOGGER = LogManager.getLogger(MergeExecutor.class);

    private static final String LATEST_PER_PK = """
        SELECT * FROM %s
        WHERE ih_blockid = '%s'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY ih_offset DESC) = 1
        """;

    private final Connection connection;
    private final String tableName;
    private final String ingestTableName;
    private final List<String> columnsFinalTable;
    private final List<String> ingestOnlyColumns;

    public MergeExecutor(Connection connection, String tableName, String ingestTableName,
                         List<String> columnsFinalTable, List<String> ingestOnlyColumns) {
        this.connection = connection;
        this.tableName = tableName;
        this.ingestTableName = ingestTableName;
        this.columnsFinalTable = List.copyOf(columnsFinalTable);
        this.ingestOnlyColumns = List.copyOf(ingestOnlyColumns);
    }

    public void merge(String blockId, List<String> pks) {
        execute(buildMergeSql(blockId, pks), "merge");
    }

    public void deleteRows(String blockId, List<String> pks) {
        execute(buildDeleteSql(blockId, pks), "delete");
    }

    private void execute(String sql, String label) {
        LOGGER.debug("Executing {} statement: {}", label, sql);
        var startTime = System.currentTimeMillis();
        try (var stmt = connection.createStatement()) {
            var affected = stmt.executeLargeUpdate(sql);
            LOGGER.debug("{} affected {} rows in {} ms", label, affected,
                    System.currentTimeMillis() - startTime);
        } catch (SQLException e) {
            LOGGER.error("Error executing {} statement", label, e);
            throw new RuntimeException("Error executing " + label + " statement", e);
        }
    }

    protected String buildMergeSql(String blockId, List<String> pks) {
        return String.format(
                "MERGE INTO %s AS final USING ("
                        + "SELECT * EXCLUDE (%s) FROM (%s) WHERE ih_op in ('c', 'r', 'u')"
                        + ") AS ingest ON %s "
                        + "WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s) "
                        + "WHEN MATCHED THEN UPDATE SET %s",
                tableName,
                buildExcludeColumns(),
                latestPerPk(blockId, pks),
                buildPkWhereClause(pks),
                String.join(",", columnsFinalTable),
                String.join(",", columnsFinalTable.stream().map(c -> "ingest." + c).toList()),
                buildUpdateColumns());
    }

    protected String buildDeleteSql(String blockId, List<String> pks) {
        return String.format(
                "DELETE FROM %s as final USING ("
                        + "SELECT %s FROM (%s) WHERE ih_op = 'd'"
                        + ") AS ingest WHERE %s",
                tableName,
                String.join(",", pks),
                latestPerPk(blockId, pks),
                buildPkWhereClause(pks));
    }

    private String latestPerPk(String blockId, List<String> pks) {
        return String.format(LATEST_PER_PK, ingestTableName, blockId, String.join(",", pks)).trim();
    }

    protected String buildUpdateColumns() {
        return String.join(",", columnsFinalTable.stream()
                .map(column -> String.format("final.%s = ingest.%s", column, column))
                .toList());
    }

    protected String buildExcludeColumns() {
        return String.join(",", ingestOnlyColumns);
    }

    protected String buildPkWhereClause(List<String> pks) {
        return pks.stream()
                .map(col -> String.format("final.%s = ingest.%s", col, col))
                .reduce((a, b) -> String.format("%s and %s", a, b))
                .orElseThrow(() -> new IllegalStateException("No primary key columns to join on"));
    }
}
