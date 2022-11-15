package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.FieldDefaults;

/**
 * Data object class to combine different service metadata related attributes for passing around the code / context.
 * @see ServiceNameHolder
 */
@Data
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class ServiceMetadata {

  String clusterId;
  String serviceName;
}
