package org.apache.flume.sink.syslog;

/**
 * This class heavily references IETF rfc3164.
 */
public class SyslogSinkConfigurationConstants {
  /**
   * defaults to {@code(1)user-level messages}.
   * <p/>
   * <pre>
   * {@code
   *
   * Numerical      Facility
   * Code
   *
   * 0              kernel messages
   * 1              user-level messages
   * 2              mail system
   * 3              system daemons
   * 4              security/authorization messages (note 1)
   * 5              messages generated internally by syslogd
   * 6              line printer subsystem
   * 7              network news subsystem
   * 8              UUCP subsystem
   * 9              clock daemon (note 2)
   * 10             security/authorization messages (note 1)
   * 11             FTP daemon
   * 12             NTP subsystem
   * 13             log audit (note 1)
   * 14             log alert (note 1)
   * 15             clock daemon (note 2)
   * 16             local use 0  (local0)
   * 17             local use 1  (local1)
   * 18             local use 2  (local2)
   * 19             local use 3  (local3)
   * 20             local use 4  (local4)
   * 21             local use 5  (local5)
   * 22             local use 6  (local6)
   * 23             local use 7  (local7)
   *
   *
   * }</pre>
   */
  public static final String FACILITY = "facility";
  /** defaults to (1)user-level messages */
  public static final int DEFAULT_FACILITY = 1;

  /**
   * Defaults to (5)Notice: normal but significant condition.
   *
   * <pre>{@code
   *
   * Numerical         Severity
   * Code
   *
   * 0       Emergency: system is unusable
   * 1       Alert: action must be taken immediately
   * 2       Critical: critical conditions
   * 3       Error: error conditions
   * 4       Warning: warning conditions
   * 5       Notice: normal but significant condition
   * 6       Informational: informational messages
   * 7       Debug: debug-level messages
   *
   * }</pre>
   */
  public static final String SEVERITY = "severity";
  /** defaults to (5)Notice: normal but significant condition */
  public static final int DEFAULT_SEVERITY = 5;

  /** remote server info, must not be null, no defaults */
  public static final String HOST = "host";

  /**
   * If set to true, then one message that exceeded 1024 bytes would be split
   * and transferred in multiple network packets. Defaults to "false".
   */
  public static final String SPLIT = "split";
  public static final boolean DEFAULT_SPLIT = false;

  /** three modes: "copy", "relay", "force_relay", defaults to "copy" */
  public static final String MODE = "mode";
  public static final String DEFAULT_MODE = "copy";

  /** retry interval in seconds when network is unreachable, defaults to 30 */
  public static final String RETRY_INTERVAL = "retryInterval";
  public static final int DEFAULT_RETRY_INTERVAL = 30;

  /** how many message's would be processed in one transaction unit, defaults to 100 */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;
}
