package com.clickhouse.kafka.connect.sink.data;

import com.clickhouse.kafka.connect.sink.data.convert.EmptyRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.RecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemalessRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.SchemaRecordConvertor;
import com.clickhouse.kafka.connect.sink.data.convert.StringRecordConvertor;
import com.clickhouse.kafka.connect.sink.kafka.OffsetContainer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.platform.commons.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class Record {
    private static final Logger log = LoggerFactory.getLogger(Record.class);
    @Getter
    private OffsetContainer recordOffsetContainer = null;
    private Object value;
    @Getter
    private Map<String, Data> jsonMap = null;
    @Getter
    private List<Field> fields = null;
    @Getter
    private SchemaType schemaType;
    @Getter
    private SinkRecord sinkRecord = null;
    @Getter
    private String database = null;

    public Record(SchemaType schemaType, OffsetContainer recordOffsetContainer, List<Field> fields, Map<String, Data> jsonMap, String database, SinkRecord sinkRecord) {
        this.recordOffsetContainer = recordOffsetContainer;
        this.fields = fields;
        this.jsonMap = jsonMap;
        this.sinkRecord = sinkRecord;
        this.schemaType = schemaType;
        this.database = database;
    }

    public String getTopicAndPartition() {
        return recordOffsetContainer.getTopicAndPartitionKey();
    }

    public String getTopic() {
        return recordOffsetContainer.getTopic();
    }

    private static final RecordConvertor schemaRecordConvertor = new SchemaRecordConvertor();
    private static final RecordConvertor schemalessRecordConvertor = new SchemalessRecordConvertor();
    private static final RecordConvertor emptyRecordConvertor = new EmptyRecordConvertor();
    private static final RecordConvertor stringRecordConvertor = new StringRecordConvertor();

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

    private static RecordConvertor getConvertor(Schema schema, Object data) {
        if (data == null ) {
            return emptyRecordConvertor;
        }
        if (schema != null && data instanceof Struct) {
            return schemaRecordConvertor;
        }
        if (data instanceof Map) {
            return schemalessRecordConvertor;
        }
        if (data instanceof String) {
            return stringRecordConvertor;
        }
        throw new DataException(String.format("No converter was found due to unexpected object type %s", data.getClass().getName()));
    }

    public static Record convert(SinkRecord sinkRecord, boolean splitDBTopic, String dbTopicSeparatorChar,String database) {
        RecordConvertor recordConvertor = getConvertor(sinkRecord.valueSchema(), sinkRecord.value());
        return recordConvertor.convert(sinkRecord, splitDBTopic, dbTopicSeparatorChar, database);
    }

    public static Record convert(SinkRecord sinkRecord, boolean splitDBTopic, String dbTopicSeparatorChar,String database, String source, String dbType, String idField) {
        String topic = sinkRecord.topic();
        if (splitDBTopic) {
            String[] parts = topic.split(Pattern.quote(dbTopicSeparatorChar));
            if (parts.length == 2) {
                database = parts[0];
                topic = parts[1];
            }
        }
        int partition = sinkRecord.kafkaPartition().intValue();
        long offset = sinkRecord.kafkaOffset();
        List<Field> fields = new ArrayList<>();
        Map<?,?> map = getMapFromSinkRecord(sinkRecord, source, dbType, idField);
        Map<String, Data> data = new HashMap<>();
        int index = 0;
        map.forEach((key,val) -> {
            fields.add(new Field(key.toString(), index, Schema.STRING_SCHEMA));
            data.put(key.toString(), new Data(Schema.STRING_SCHEMA, val == null ? null : val.toString()));
        });
        return new Record(SchemaType.SCHEMA, new OffsetContainer(topic, partition, offset), fields, data, database, sinkRecord);
    }

    private static Map<?,?> getMapFromSinkRecord(SinkRecord sinkRecord, String source, String dbType, String idField) {
        Map<String,Object> map = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();
        try {
            if ("cdc".equals(source)) {
                JsonNode rootNode = null;
                if ("mongo".equals(dbType)) {
                    rootNode = mapper.readTree(sinkRecord.value().toString());
                    map.put("_id", rootNode.get("_id").asText());
                } else if ("mysql".equals(dbType)) {
                    rootNode = mapper.valueToTree(sinkRecord.value());
                    if (rootNode.has("id")) {
                        map.put("id", rootNode.get("id").asText());
                    } else if (!StringUtils.isBlank(idField) && rootNode.has(idField)) {
                        map.put("id", rootNode.get(idField).asText());
                    } else {
                        map.put("id", UUID.randomUUID().toString());
                    }
                }
                map.put("data", mapper.writeValueAsString(rootNode));
                map.put("partition_key", System.currentTimeMillis());
            } else if ("pes".equals(source)) {
                String jsonString = mapper.writeValueAsString(sinkRecord.value()); // Convert to valid JSON string
                JsonNode rootNode = mapper.readTree(jsonString);
                map.put("id", rootNode.get("id").asText());
                JsonNode event = rootNode.get("event");
                map.put("event", mapper.writeValueAsString(event));
                map.put("eventTimestamp", rootNode.get("eventTimestamp").asLong());
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public static Record newRecord(SchemaType schemaType, String topic, int partition, long offset, List<Field> fields, Map<String, Data> jsonMap, String database, SinkRecord sinkRecord) {
        return new Record(schemaType, new OffsetContainer(topic, partition, offset), fields, jsonMap, database, sinkRecord);
    }

}
