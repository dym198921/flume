package org.apache.flume.sink.syslog;

/**
 * Created by vgu on 4/13/14.
 */
public class SyslogException extends Exception {
  public SyslogException(String message) {
    super(message);
  }

  public SyslogException(Throwable cause) {
    super(cause);
  }

  public SyslogException(String message, Throwable cause) {
    super(message, cause);
  }
}
