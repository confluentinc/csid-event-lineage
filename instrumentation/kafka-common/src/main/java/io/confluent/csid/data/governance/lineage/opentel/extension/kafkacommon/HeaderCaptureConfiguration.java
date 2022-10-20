/*
 * Copyright 2022 Confluent Inc.
 */
package io.confluent.csid.data.governance.lineage.opentel.extension.kafkacommon;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeaderCaptureConfiguration {

  /**
   * Configuration property for header capture - case-insensitive, comma-separated list of headers
   * that will have their values captured as attributes in Spans generated.
   * <p>
   * Assumed to have String values encoded with encoding specified by "event.lineage.header-charset"
   * property (default UTF-8)
   */
  public static final String HEADER_CAPTURE_WHITELIST_PROP = "event.lineage.header-capture-whitelist";

  /**
   * Configuration property for header propagation - case-insensitive, comma-separated list of
   * headers that will have their values propagated automatically over state-store operations,
   * joins, aggregations where normally they would be lost.
   */
  public static final String HEADER_PROPAGATION_WHITELIST_PROP = "event.lineage.header-propagation-whitelist";

  /**
   * Configuration property for charset used for header capture - used for value conversion from
   * byte[] to String.
   * <p>
   * Default UTF-8
   * <p>
   * Has to be a canonical name or alias of available charset.
   * <p>
   * See also - {@link Charset#forName(String)}
   */
  public static final String HEADER_CHARSET_PROP = "event.lineage.header-charset";

  private Set<String> headerCaptureWhiteList;
  private Set<String> headerPropagationWhiteList;
  private Charset charset;

  public HeaderCaptureConfiguration() {
    loadConfiguration();
  }

  private void loadConfiguration() {
    headerCaptureWhiteList = new HashSet<>();
    headerPropagationWhiteList = new HashSet<>();
    charset = StandardCharsets.UTF_8;
    if (System.getProperties().containsKey(HEADER_CAPTURE_WHITELIST_PROP)) {
      Arrays.stream(System.getProperty(HEADER_CAPTURE_WHITELIST_PROP).split(","))
          .map(String::trim)
          .filter(prop -> prop.length() > 0)
          .forEach(prop -> headerCaptureWhiteList.add(prop.toLowerCase()));
    }
    if (System.getProperties().containsKey(HEADER_PROPAGATION_WHITELIST_PROP)) {
      Arrays.stream(System.getProperty(HEADER_PROPAGATION_WHITELIST_PROP).split(","))
          .map(String::trim)
          .filter(prop -> prop.length() > 0)
          .forEach(prop -> headerPropagationWhiteList.add(prop.toLowerCase()));
    }
    if (System.getProperties().containsKey(HEADER_CHARSET_PROP)) {
      charset = Charset.forName(System.getProperty(HEADER_CHARSET_PROP).trim());
    }
    log.debug(
        "HeaderCaptureConfiguration - CaptureWhitelist: {}, PropagationWhitelist: {}, Charset: {}",
        this.headerCaptureWhiteList, this.headerPropagationWhiteList, this.charset);
  }

  /**
   * Configured header capture whitelist - set of header names to have their values captured as Span
   * attributes - see {@link #HEADER_CAPTURE_WHITELIST_PROP}
   *
   * @return Set of header names
   */
  public Set<String> getHeaderCaptureWhitelist() {
    return headerCaptureWhiteList;
  }

  /**
   * Configured header propagation whitelist - set of header names that are to be automatically
   * propagated - see {@link #HEADER_PROPAGATION_WHITELIST_PROP}
   *
   * @return Set of header names
   */
  public Set<String> getHeaderPropagationWhitelist() {
    return headerPropagationWhiteList;
  }

  /**
   * Configured Charset used for header capture See - {link #HEADER_CHARSET_PROP}
   *
   * @return Charset
   */
  public Charset getHeaderValueEncoding() {
    return charset;
  }

  //Visible for testing - for resetting configuration for different tests
  void reloadConfiguration() {
    loadConfiguration();
  }
}
