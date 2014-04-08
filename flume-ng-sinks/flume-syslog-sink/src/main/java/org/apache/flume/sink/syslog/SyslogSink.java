package org.apache.flume.sink.syslog;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.net.SyslogAppender;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configurable items:
 * - host: syslog host string, not optional
 * - sink.batchSize: event processing batch size, optional, defaults to 100
 */
public class SyslogSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory
      .getLogger(SyslogSink.class);

  private SinkCounter counter = new SinkCounter(getName());
  private static final int DEFAULT_BATCH_SIZE = 100;
  private int batchSize;
  private SyslogAppender syslog;
  private String host;

  @Override
  public void configure(Context context) {
    host = context.getString("host");
    Preconditions.checkArgument(host != null, "syslog host must not be null");

    batchSize = context.getInteger("sink.batchSize", DEFAULT_BATCH_SIZE);
  }

  @Override
  public synchronized void start() {
    logger.info("syslog sink {} starting...", getName());
    syslog = new SyslogAppender();
    syslog.setSyslogHost(host);
    counter.start();
    super.start();
    logger.info("syslog sink {} started", getName());
  }

  @Override
  public synchronized void stop() {
    logger.info("syslog sink {} stopping...", getName());
    super.stop();
    counter.stop();
    syslog.close();
    logger.info("syslog sink {} stopped", getName());
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;

    try {
      transaction.begin();
      int attemptCounter = 0;
      for (int i = 0; i < batchSize; i++) {
        event = channel.take();
        if (event != null) {
          LoggingEvent loggingEvent = new LoggingEvent("syslog",
              null, null, event.getBody(), null);
        }
      }
    }
  }
}
