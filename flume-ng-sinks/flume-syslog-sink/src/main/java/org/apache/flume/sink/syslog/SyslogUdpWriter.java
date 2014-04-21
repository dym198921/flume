package org.apache.flume.sink.syslog;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.Date;

class SyslogUdpWriter extends SyslogWriter {
  private final DatagramSocket ds;

  public SyslogUdpWriter(final InetAddress address, final int port)
  throws SocketException {
    this.address = address;
    this.port = port;

    this.ds = new DatagramSocket();
  }

  @Override
  public void write(final byte[] packet)
  throws IOException {

    if (this.ds != null && this.address != null) {
      //
      //  syslog packets must be less than 1024 bytes
      //
      int bytesLength = packet.length;
      if (bytesLength >= 1024) {
        bytesLength = 1024;
      }
      DatagramPacket dPacket = new DatagramPacket(packet, bytesLength,
          address, port);
      ds.send(dPacket);
    }
  }

  @Override
  public void relay(final boolean force, final int facility, final int severity,
                    final Date date, final String hostname, final byte[] msg)
  throws IOException {

  }

  private void checkSocket() {
    this.ds.isConnected();
  }

  public void close() {
    if (ds != null) {
      ds.close();
    }
  }
}
