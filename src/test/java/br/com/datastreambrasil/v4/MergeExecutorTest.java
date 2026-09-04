package br.com.datastreambrasil.v4;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MergeExecutorTest {

    private static final List<String> FINAL_COLUMNS = List.of("ID", "NAME");
    private static final List<String> INGEST_ONLY_COLUMNS =
            List.of("IH_TOPIC", "IH_PARTITION", "IH_OFFSET", "IH_OP", "IH_DATETIME", "IH_BLOCKID");

    private Connection connection;
    private Statement statement;
    private MergeExecutor executor;

    @BeforeEach
    void setUp() throws SQLException {
        connection = mock(Connection.class);
        statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeLargeUpdate(anyString())).thenReturn(1L);

        executor = new MergeExecutor(connection, "EVENTS", "EVENTS_INGEST",
                FINAL_COLUMNS, INGEST_ONLY_COLUMNS);
    }

    @Test
    void mergeDeduplicatesTheBlockBeforeFilteringByOperation() {
        var sql = executor.buildMergeSql("block-1", List.of("ID"));

        // v4 streams every event, so a block can hold several rows per key. Without this the
        // MERGE fails with "Duplicate row detected during DML action".
        assertTrue(sql.contains("QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ih_offset DESC) = 1"),
                sql);

        // the op filter must come after the dedup, otherwise an update in the same block would
        // resurrect a key whose last operation was a delete
        var qualifyAt = sql.indexOf("QUALIFY ROW_NUMBER()");
        var opFilterAt = sql.indexOf("WHERE ih_op in ('c', 'r', 'u')");
        assertTrue(qualifyAt > 0 && opFilterAt > qualifyAt,
                "ih_op filter must be applied after the dedup: " + sql);
    }

    @Test
    void mergeExcludesIngestOnlyColumnsAndUpdatesEveryFinalColumn() {
        var sql = executor.buildMergeSql("block-1", List.of("ID"));

        assertTrue(sql.contains("EXCLUDE (IH_TOPIC,IH_PARTITION,IH_OFFSET,IH_OP,IH_DATETIME,IH_BLOCKID)"), sql);
        assertTrue(sql.contains("MERGE INTO EVENTS AS final"), sql);
        assertTrue(sql.contains("final.ID = ingest.ID"), sql);
        assertTrue(sql.contains("WHEN NOT MATCHED THEN INSERT (ID,NAME) VALUES (ingest.ID,ingest.NAME)"), sql);
        assertTrue(sql.contains("WHEN MATCHED THEN UPDATE SET final.ID = ingest.ID,final.NAME = ingest.NAME"), sql);
    }

    @Test
    void deleteDeduplicatesTheBlockBeforeFilteringByOperation() {
        var sql = executor.buildDeleteSql("block-1", List.of("ID"));

        var qualifyAt = sql.indexOf("QUALIFY ROW_NUMBER()");
        var opFilterAt = sql.indexOf("WHERE ih_op = 'd'");
        assertTrue(qualifyAt > 0 && opFilterAt > qualifyAt, sql);
        assertTrue(sql.contains("DELETE FROM EVENTS as final"), sql);
        assertTrue(sql.contains("ih_blockid = 'block-1'"), sql);
    }

    @Test
    void supportsCompositePrimaryKeys() {
        var sql = executor.buildMergeSql("block-1", List.of("ID", "TENANT"));

        assertTrue(sql.contains("PARTITION BY ID,TENANT ORDER BY ih_offset DESC"), sql);
        assertTrue(sql.contains("final.ID = ingest.ID and final.TENANT = ingest.TENANT"), sql);
    }

    @Test
    void executesTheStatements() throws SQLException {
        executor.merge("block-1", List.of("ID"));
        executor.deleteRows("block-1", List.of("ID"));

        verify(statement).executeLargeUpdate(matches("(?s)MERGE INTO EVENTS.*"));
        verify(statement).executeLargeUpdate(matches("(?s)DELETE FROM EVENTS.*"));
        verify(connection, times(2)).createStatement();
    }
}
