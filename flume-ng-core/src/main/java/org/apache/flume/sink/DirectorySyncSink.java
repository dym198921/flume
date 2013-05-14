/*
   Copyright 2013 Vincent.Gu

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package org.apache.flume.sink;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.apache.flume.source.DirectorySyncSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

public class DirectorySyncSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory
      .getLogger(DirectorySyncSink.class);
  private static final int defaultBatchSize = 100;
  private int batchSize = defaultBatchSize;
  private File directory;
  private File cachedFile;
  private OutputStream cachedOutputStream;
  private EventSerializer cachedSerializer;
  private String serializerType;
  private Context serializerContext;
  private SinkCounter sinkCounter;

  public DirectorySyncSink() {
  }

  @Override
  public void configure(Context context) {
    String directory = context.getString("sink.directory");

    serializerType = context.getString("sink.serializer", "TEXT");
    serializerContext =
        new Context(context.getSubProperties(EventSerializer.CTX_PREFIX));

    Preconditions.checkArgument(directory != null, "Directory may not be null");
    Preconditions.checkNotNull(serializerType, "Serializer type is undefined");

    batchSize = context.getInteger("sink.batchSize", defaultBatchSize);

    this.directory = new File(directory);

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @Override
  public void start() {
    logger.info("Starting {}...", this);
    sinkCounter.start();
    super.start();

    logger.info("DirectorySink {} started.", getName());
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    Event event = null;
    Status result = Status.READY;

    try {
      transaction.begin();
      int eventAttemptCounter = 0;
      for (int i = 0; i < batchSize; i++) {
        event = channel.take();
        if (event != null) {
          Map<String, String> headers = event.getHeaders();
          String eventFileStr = headers.get(
              DirectorySyncSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY);
          try {
            File eventFile = new File(directory, eventFileStr);
            Files.createParentDirs(eventFile);
            if (!eventFile.equals(cachedFile)) {
              cachedFile = eventFile;
              if (null != cachedOutputStream)
                cachedOutputStream.close();
              logger.debug("creating new OutputStream for file {}", eventFileStr);
              cachedOutputStream = new FileOutputStream(eventFile, true);
              cachedSerializer = EventSerializerFactory.getInstance(
                  serializerType, serializerContext, cachedOutputStream);
              cachedSerializer.afterCreate();
              sinkCounter.incrementConnectionCreatedCount();
            }
          } catch (IOException e) {
            sinkCounter.incrementConnectionFailedCount();
            throw new EventDeliveryException("Failed to open file "
                + eventFileStr + " while delivering event", e);
          }

          sinkCounter.incrementEventDrainAttemptCount();
          eventAttemptCounter++;
          cachedSerializer.write(event);
          cachedSerializer.flush();
          cachedOutputStream.flush();
        } else {
          // No events found, request back-off semantics from runner
          result = Status.BACKOFF;
          break;
        }
      }
      transaction.commit();
      sinkCounter.addToEventDrainSuccessCount(eventAttemptCounter);
    } catch (Exception ex) {
      transaction.rollback();
      throw new EventDeliveryException("Failed to process transaction", ex);
    } finally {
      transaction.close();
    }

    return result;
  }

  @Override
  public void stop() {
    logger.info("DirectorySyncSink {} stopping...", getName());
    sinkCounter.stop();
    super.stop();

    if (cachedOutputStream != null) {
      logger.debug("Closing file {}", cachedFile);

      try {
        cachedSerializer.flush();
        cachedSerializer.beforeClose();
        cachedOutputStream.flush();
        cachedOutputStream.close();
        sinkCounter.incrementConnectionClosedCount();
      } catch (IOException e) {
        sinkCounter.incrementConnectionFailedCount();
        logger.error("Unable to close output stream. Exception follows.", e);
      }
    }
    logger.info("DirectorySyncSink {} stopped. Event metrics: {}",
        getName(), sinkCounter);
  }

  public File getDirectory() {
    return directory;
  }

  public void setDirectory(File directory) {
    this.directory = directory;
  }
}
