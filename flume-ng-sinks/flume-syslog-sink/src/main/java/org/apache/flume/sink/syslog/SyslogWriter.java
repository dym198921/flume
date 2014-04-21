package org.apache.flume.sink.syslog;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

public abstract class SyslogWriter {
  protected InetAddress address;
  protected int port;
  /** default behavior is truncate */
  protected boolean split = false;
  protected Mode mode = Mode.COPY;

  public abstract void write(final byte[] packet)
  throws IOException;

  public abstract void relay(final boolean force,
                             final int facility,
                             final int severity,
                             final Date date,
                             final String hostname,
                             final byte[] msg)
  throws IOException;

  public abstract void close();

  public boolean isSplit() {
    return split;
  }

  public void setSplit(boolean split) {
    this.split = split;
  }

  public InetAddress getAddress() {
    return address;
  }

  public void setAddress(InetAddress address) {
    this.address = address;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }
}

enum Mode {
  COPY, RELAY, FORCE_RELAY
}