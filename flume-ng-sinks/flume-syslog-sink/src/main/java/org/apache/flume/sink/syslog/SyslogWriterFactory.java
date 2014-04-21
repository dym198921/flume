package org.apache.flume.sink.syslog;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

public class SyslogWriterFactory {
  final static int SYSLOG_PORT = 514;

  /**
   * Constructs a new instance of SyslogWriter.
   *
   * @param syslogHost host name, in the format "[protocol://]host[:port]", may
   *                   not be null.  A protocol may be specified by prepending
   *                   to host name or IPv4 literal address with a colon at the
   *                   end. A port may be specified by following the host name
   *                   or IPv4 literal address with a colon and a decimal port
   *                   number. To specify a port with an IPv6 address, enclose
   *                   the IPv6 address in square brackets before appending the
   *                   colon and decimal port number.
   * @throws org.apache.flume.sink.syslog.SyslogException when something goes
   *                                                      wrong while parsing
   */
  public static SyslogWriter getInstance(final String syslogHost)
      throws SyslogException {
    if (syslogHost == null) {
      throw new NullPointerException("syslogHost");
    }
    String host = syslogHost;

    Protocol protocol = Protocol.UDP;
    InetAddress address;
    int port = -1;

    // try to parse transport protocol
    int index = syslogHost.indexOf("://");
    if (index >= 0) {
      String pStr = syslogHost.substring(0, index).toLowerCase();
      if (pStr.equals("tcp")) {
        protocol = Protocol.TCP;
      } else if (pStr.equals("udp")) {
        protocol = Protocol.UDP;
      } else {
        throw new SyslogException("unsupported transport protocol: " + pStr);
      }
      host = host.substring(index + 3);
    }

    //
    //  If not an unbracketed IPv6 address then
    //      parse as a URL
    //
    if (host.indexOf("[") != -1 || host.indexOf(':') == host.lastIndexOf(':')) {
      try {
        URL url = new URL("http://" + host);
        if (url.getHost() != null) {
          host = url.getHost();
          //   if host is a IPv6 literal, strip off the brackets
          if (host.startsWith("[") && host.charAt(host.length() - 1) == ']') {
            host = host.substring(1, host.length() - 1);
          }
          port = url.getPort();
        }
      } catch (MalformedURLException e) {
        throw new SyslogException("Malformed URL: will attempt to interpret " +
            "as InetAddress.", e);
      }
    }

    if (port == -1) {
      port = SYSLOG_PORT;
    }

    try {
      address = InetAddress.getByName(host);
    } catch (UnknownHostException e) {
      throw new SyslogException("Could not find " + host +
          ". All logging will FAIL.", e);
    }

    SyslogWriter syslogWriter;
    try {
      if (protocol == Protocol.TCP) {
        syslogWriter = new SyslogTcpWriter(address, port);
      } else {
        syslogWriter = new SyslogUdpWriter(address, port);
      }
    } catch (Exception e) {
      throw new SyslogException("error while instantiating network " +
          "transport", e);
    }
    return syslogWriter;
  }
}

enum Protocol {
  TCP, UDP
}