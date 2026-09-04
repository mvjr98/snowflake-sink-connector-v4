package br.com.datastreambrasil.v4;

import br.com.datastreambrasil.v4.exception.InvalidStructException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

/**
 * Turns a Debezium CDC {@link SinkRecord} into the row map Snowpipe Streaming expects.
 *
 * <p>Unlike v3 - which built a positional CSV line ordered by the ingest table's ordinal
 * positions - the pipe matches by column name, so this produces a
 * {@code Map<columnName, value>}. Keys are the column names exactly as they come back from
 * Snowflake's metadata (upper case for unquoted identifiers), which keeps the mapping correct
 * whether or not the pipe is case sensitive.
 */
public class RowMapper {

    private static final Logger LOGGER = LogManager.getLogger(RowMapper.class);

    protected static final String AFTER = "after";
    protected static final String BEFORE = "before";
    protected static final String OP = "op";
    protected static final String IHTOPIC = "ih_topic";
    protected static final String IHOFFSET = "ih_offset";
    protected static final String IHPARTITION = "ih_partition";
    protected static final String IHOP = "ih_op";
    protected static final String IHDATETIME = "ih_datetime";
    protected static final String IHBLOCKID = "ih_blockid";

    /** Debezium operation codes: delete, create, update, read (snapshot). */
    public enum DebeziumOperation {
        d, c, u, r
    }

    private final List<String> ingestColumns;
    private final List<String> timestampFieldsConvert;
    private final List<String> dateFieldsConvert;
    private final List<String> timeFieldsConvert;

    public RowMapper(List<String> ingestColumns,
                     List<String> timestampFieldsConvert,
                     List<String> dateFieldsConvert,
                     List<String> timeFieldsConvert) {
        this.ingestColumns = List.copyOf(ingestColumns);
        this.timestampFieldsConvert = normalize(timestampFieldsConvert);
        this.dateFieldsConvert = normalize(dateFieldsConvert);
        this.timeFieldsConvert = normalize(timeFieldsConvert);
    }

    private static List<String> normalize(List<String> values) {
        var out = new ArrayList<String>();
        if (values != null) {
            for (String v : values) {
                if (v != null && !v.isBlank()) {
                    out.add(v.trim().toUpperCase(Locale.ROOT));
                }
            }
        }
        return List.copyOf(out);
    }

    /** Debezium operation carried by the record. Throws when the envelope is malformed. */
    public String operationOf(SinkRecord record) {
        var fieldOP = record.valueSchema().field(OP);
        if (fieldOP == null) {
            LOGGER.error("Field '{}' not found in value schema for record: {}", OP, record);
            throw new InvalidStructException("Field '" + OP + "' not found in value schema");
        }

        var valueOP = ((Struct) record.value()).getString(fieldOP.name());
        if (valueOP == null) {
            LOGGER.error("Value for field '{}' is null in record: {}", OP, record);
            throw new InvalidStructException("Value for field '" + OP + "' is null");
        }
        return valueOP;
    }

    /** Primary key column names, taken from the record's key schema like v3 did. */
    public List<String> extractPk(SinkRecord record) {
        var pks = new ArrayList<String>();
        for (Field field : record.keySchema().fields()) {
            pks.add(field.name());
        }
        if (pks.isEmpty()) {
            throw new InvalidStructException("Record key schema has no fields, cannot derive primary key");
        }
        return List.copyOf(pks);
    }

    public void validate(SinkRecord record) {
        if (record.keySchema() == null || record.valueSchema() == null
                || !(record.key() instanceof Struct) || !(record.value() instanceof Struct)) {
            LOGGER.error("Key and value must be Structs with schemas. Key: {}, Value: {}",
                    record.key(), record.value());
            throw new InvalidStructException("Invalid record structure or schema");
        }

        if (record.topic() == null || record.kafkaPartition() == null) {
            LOGGER.error("Null values for topic or kafkaPartition. Topic {}, KafkaPartition {}",
                    record.topic(), record.kafkaPartition());
            throw new InvalidStructException("Invalid record structure or schema");
        }
    }

    /**
     * Builds the row for one record. Only columns that exist in the ingest table are emitted;
     * a null or absent field is left out of the map entirely, which lands as NULL.
     */
    public Map<String, Object> toRow(SinkRecord record, String blockId) {
        validate(record);

        var valueRecord = (Struct) record.value();
        var op = operationOf(record);
        var payload = DebeziumOperation.d.name().equalsIgnoreCase(op)
                ? valueRecord.getStruct(BEFORE)
                : valueRecord.getStruct(AFTER);

        if (payload == null) {
            LOGGER.error("Record has no '{}' payload for operation '{}': {}",
                    DebeziumOperation.d.name().equalsIgnoreCase(op) ? BEFORE : AFTER, op, record);
            throw new InvalidStructException("Missing payload struct for operation '" + op + "'");
        }

        // case-insensitive lookup of the Debezium fields, mirroring v3's equalsIgnoreCase match
        var fieldsByName = new HashMap<String, Object>();
        for (Field field : payload.schema().fields()) {
            fieldsByName.put(field.name().toUpperCase(Locale.ROOT), payload.get(field.name()));
        }

        var row = new LinkedHashMap<String, Object>(ingestColumns.size());
        for (String column : ingestColumns) {
            if (column.equalsIgnoreCase(IHBLOCKID)) {
                row.put(column, blockId);
            } else if (column.equalsIgnoreCase(IHOP)) {
                row.put(column, op);
            } else if (column.equalsIgnoreCase(IHTOPIC)) {
                row.put(column, record.topic());
            } else if (column.equalsIgnoreCase(IHDATETIME)) {
                row.put(column, LocalDateTime.now(ZoneOffset.UTC).toString());
            } else if (column.equalsIgnoreCase(IHPARTITION)) {
                row.put(column, record.kafkaPartition());
            } else if (column.equalsIgnoreCase(IHOFFSET)) {
                row.put(column, record.kafkaOffset());
            } else {
                var value = fieldsByName.get(column.toUpperCase(Locale.ROOT));
                if (value == null) {
                    // absent or null: leaving the key out makes the pipe write NULL
                    continue;
                }
                row.put(column, convert(column, value));
            }
        }

        return row;
    }

    private Object convert(String column, Object value) {
        var upper = column.toUpperCase(Locale.ROOT);

        // same semantics as v3: the source sends epoch millis / epoch days / nanos-of-day and the
        // configured column lists say how to read them. Kept on the default JVM timezone so a
        // v3 -> v4 migration does not shift existing values.
        if (timestampFieldsConvert.contains(upper)) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Number) value).longValue()),
                    TimeZone.getDefault().toZoneId()).toString();
        }
        if (dateFieldsConvert.contains(upper)) {
            var daysInSeconds = ((Number) value).longValue() * 24L * 60L * 60L;
            return LocalDate.ofInstant(Instant.ofEpochSecond(daysInSeconds),
                    TimeZone.getDefault().toZoneId()).toString();
        }
        if (timeFieldsConvert.contains(upper)) {
            return LocalTime.ofNanoOfDay(((Number) value).longValue()).toString();
        }

        if (value instanceof ByteBuffer buffer) {
            var bytes = new byte[buffer.remaining()];
            buffer.duplicate().get(bytes);
            return bytes;
        }
        if (value instanceof byte[] || value instanceof Number || value instanceof Boolean
                || value instanceof String || value instanceof BigDecimal) {
            return value;
        }
        if (value instanceof Date date) {
            return LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).toString();
        }
        if (value instanceof LocalDateTime || value instanceof LocalDate || value instanceof LocalTime) {
            return value.toString();
        }

        // anything else (nested Structs, arrays) keeps v3's behaviour of stringifying
        return value.toString();
    }
}
