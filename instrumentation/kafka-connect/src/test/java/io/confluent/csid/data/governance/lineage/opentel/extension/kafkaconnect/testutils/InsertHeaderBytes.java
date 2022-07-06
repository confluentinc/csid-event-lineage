/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkaconnect.testutils;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

/**
 * Simple SMT that inserts hardcoded header into message - serialized as byte[].
 *
 * @param <R> - ConnectRecord subtype (i.e. SourceRecord or SinkRecord depending on connector used)
 */
public class InsertHeaderBytes<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
      "Add a header to each record.";

  public static final String HEADER_FIELD = "header";
  public static final String VALUE_LITERAL_FIELD = "value.literal";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(HEADER_FIELD, ConfigDef.Type.STRING,
          NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
          ConfigDef.Importance.HIGH,
          "The name of the header.")
      .define(VALUE_LITERAL_FIELD, ConfigDef.Type.STRING,
          NO_DEFAULT_VALUE, new ConfigDef.NonNullValidator(),
          ConfigDef.Importance.HIGH,
          "The literal value that is to be set as the header value on all records.");

  private String header;

  private SchemaAndValue literalValue;

  @Override
  public R apply(R record) {
    Headers updatedHeaders = record.headers().duplicate();
    updatedHeaders.add(header, literalValue);
    return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
        record.key(),
        record.valueSchema(), record.value(), record.timestamp(), updatedHeaders);
  }


  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    header = config.getString(HEADER_FIELD);
    literalValue = new SchemaAndValue(Schema.BYTES_SCHEMA,
        config.getString(VALUE_LITERAL_FIELD).getBytes(
            StandardCharsets.UTF_8));
  }
}


