package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.FieldDefaults;

@Data
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ServiceMetadata {
  String serviceName;
}
