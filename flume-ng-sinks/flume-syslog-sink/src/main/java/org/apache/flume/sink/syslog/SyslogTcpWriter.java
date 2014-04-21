package org.apache.flume.sink.syslog;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Date;

/**
 * SyslogTcpWriter is a wrapper around the java.net.Socket class so that it
 * behaves like a java.io.Writer.
 */
class SyslogTcpWriter extends SyslogWriter {
  private final Socket s;

  public SyslogTcpWriter(final InetAddress address, final int port)
  throws IOException {
    this.address = address;
    this.port = port;

    this.s = new Socket(address, port);
  }


  @Override
  public void write(byte[] packet)
  throws IOException {

  }

  @Override
  public void relay(boolean force, int facility, int severity, Date date,
                    String hostname, byte[] msg)
  throws IOException {

  }

  @Override
  public void close() {

  }
}
