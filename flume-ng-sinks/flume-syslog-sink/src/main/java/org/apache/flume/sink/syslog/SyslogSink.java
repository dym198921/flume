package org.apache.flume.sink.syslog;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Priority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Calendar;

/**
 * Configurable items: - host: syslog host string, not optional -
 * sink.batchSize: event processing batch size, optional, defaults to 100
 */
public class SyslogSink extends AbstractSink implements Configurable {
  private static Logger logger;

  private SinkCounter sinkCounter = new SinkCounter(getName());
  private int batchSize;
  private int retryInterval;

  private int facility; // range: 0-23
  private int severity; // range: 0-7
  private Priority priority;
  private String host;
  private boolean split;
  private Mode mode;

  private SyslogWriter syslogWriter;

  @Override
  public void configure(Context context) {
    /* init logger */
    logger = LoggerFactory.getLogger(SyslogSink.class.getName() + "-" +
        getName());

    facility = context.getInteger(SyslogSinkConfigurationConstants.FACILITY,
        SyslogSinkConfigurationConstants.DEFAULT_FACILITY);
    severity = context.getInteger(SyslogSinkConfigurationConstants.SEVERITY,
        SyslogSinkConfigurationConstants.DEFAULT_SEVERITY);
    host = context.getString(SyslogSinkConfigurationConstants.HOST);
    split = context.getBoolean(SyslogSinkConfigurationConstants.SPLIT,
        SyslogSinkConfigurationConstants.DEFAULT_SPLIT);
    String mode = context.getString(SyslogSinkConfigurationConstants.MODE,
        SyslogSinkConfigurationConstants.DEFAULT_MODE);
    retryInterval = context.getInteger(SyslogSinkConfigurationConstants
            .RETRY_INTERVAL,
        SyslogSinkConfigurationConstants.DEFAULT_RETRY_INTERVAL
    );
    batchSize = context.getInteger(SyslogSinkConfigurationConstants.BATCH_SIZE,
        SyslogSinkConfigurationConstants.DEFAULT_BATCH_SIZE);

    Preconditions.checkArgument(facility >= 0 && facility <= 23,
        "facility out of range");
    Preconditions.checkArgument(severity >= 0 && severity <= 7,
        "severity out of range");
    Preconditions.checkNotNull(host, "syslog host must not be null");
    for (Mode m : Mode.values()) {
      this.mode = m.name().equals(mode.toUpperCase()) ? m : this.mode;
    }
    Preconditions.checkArgument(this.mode != null, "unsupported mode: " + mode);
    sinkCounter = new SinkCounter(this.getName());
  }

  @Override
  public synchronized void start() {
    logger.info("syslog sink {} starting...", getName());

    // network transport start
    try {
      syslogWriter = SyslogWriterFactory.getInstance(host);
    } catch (SyslogException e) {
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException("error while retrieving Syslog writer with " +
          "host string: " + host, e);
    }

    // flume sink start
    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();

    logger.info("syslog sink {} started", getName());
  }

  @Override
  public synchronized void stop() {
    logger.info("syslog sink {} stopping...", getName());

    // close SyslogWriter
    if (syslogWriter != null)
      syslogWriter.close();
    sinkCounter.incrementConnectionClosedCount();

    // flume sink stop
    sinkCounter.stop();
    super.stop();

    logger.info("syslog sink {} stopped", getName());
  }

  @Override
  public Status process()
  throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;
    txn.begin();

    try {
      long i = 0;
      for (; i < batchSize; i++) {
        event = channel.take();
        if (event == null) {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          if (i == 0) {
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          sinkCounter.incrementEventDrainAttemptCount();
          switch (mode) {
            case COPY:
              syslogWriter.write(event.getBody());
            case RELAY:
              syslogWriter.relay(false, facility, severity,
                  Calendar.getInstance().getTime(), host, event.getBody());
            case FORCE_RELAY:
              syslogWriter.relay(true, facility, severity,
                  Calendar.getInstance().getTime(), host, event.getBody());
          }
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);

      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(i);
    } catch (IOException e) {
      txn.rollback();
      throw new EventDeliveryException("Failed to process transaction due to " +
          "IO error", e);
    } finally {
      txn.close();
    }

    return result;
  }
}
