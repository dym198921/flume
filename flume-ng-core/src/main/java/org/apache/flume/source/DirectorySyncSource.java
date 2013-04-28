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

package org.apache.flume.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.client.avro.DirectorySyncFileLineReader;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * DirectorySyncSource is a source that will sync files like spool directory
 * but also copy the directory's original layout. Also unlike spool
 * directory, this source will also track changed files.
 * <p/>
 * For e.g., a file will be identified as finished and stops reading from it
 * if an empty file with suffix  ".done" that present in the same directory of
 * the same name as of the original file.
 */
public class DirectorySyncSource extends AbstractSource implements
    Configurable, EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(DirectorySyncSource.class);
  // Delay used when polling for file changes
  private static int POLL_DELAY_MS = 500;
  /* Config options */
  private Path syncDirectory;
  private String endFileSuffix;
  private String syncingStatsFileSuffix;
  private String syncedStatsFileSuffix;
  private String filenameHeaderKey;
  private int batchSize;
  private ScheduledExecutorService executor;
  private CounterGroup counterGroup;
  private Runnable runner;
  private DirectorySyncFileLineReader reader;

  @Override
  public synchronized void start() {
    logger.info("DirectorySyncSource source starting with directory:{}",
        syncDirectory);

    executor = Executors.newSingleThreadScheduledExecutor();
    counterGroup = new CounterGroup();

    reader = new DirectorySyncFileLineReader(syncDirectory,
        endFileSuffix, syncingStatsFileSuffix, syncedStatsFileSuffix);
    runner = new DirectorySyncRunnable(reader, counterGroup);

    executor.scheduleWithFixedDelay(
        runner, 0, POLL_DELAY_MS, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("DirectorySyncSource source started");
  }

  @Override
  public synchronized void stop() {
    super.stop();
    logger.debug("DirectorySyncSource source stopped");
  }

  @Override
  public void configure(Context context) {
    String syncDirectoryStr = context.getString(
        DirectorySyncSourceConfigurationConstants.SYNC_DIRECTORY);
    Preconditions.checkState(syncDirectoryStr != null,
        "Configuration must specify a sync directory");
    syncDirectory = Paths.get(syncDirectoryStr);

    endFileSuffix = context.getString(
        DirectorySyncSourceConfigurationConstants.END_FILE_SUFFIX,
        DirectorySyncSourceConfigurationConstants.DEFAULT_END_FILE_SUFFIX);
    syncingStatsFileSuffix = context.getString(
        DirectorySyncSourceConfigurationConstants.SYNCING_STATS_FILE_SUFFIX,
        DirectorySyncSourceConfigurationConstants.DEFAULT_SYNCING_STATS_FILE_SUFFIX);
    syncedStatsFileSuffix = context.getString(
        DirectorySyncSourceConfigurationConstants.SYNCED_STATS_FILE_SUFFIX,
        DirectorySyncSourceConfigurationConstants.DEFAULT_SYNCED_STATS_FILE_SUFFIX);
    filenameHeaderKey = context.getString(
        DirectorySyncSourceConfigurationConstants.FILENAME_HEADER_KEY,
        DirectorySyncSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY);
    batchSize = context.getInteger(
        DirectorySyncSourceConfigurationConstants.BATCH_SIZE,
        DirectorySyncSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
  }

  private Event createEvent(String lineEntry, String filename) {
    Event out = EventBuilder.withBody(lineEntry.getBytes());
    out.getHeaders().put(filenameHeaderKey, filename);
    return out;
  }

  private class DirectorySyncRunnable implements Runnable {
    private DirectorySyncFileLineReader reader;
    private CounterGroup counterGroup;

    public DirectorySyncRunnable(DirectorySyncFileLineReader reader,
                                 CounterGroup counterGroup) {
      this.reader = reader;
      this.counterGroup = counterGroup;
    }

    @Override
    public void run() {
      try {
        while (true) {
          List<String> strings = reader.readLines(batchSize);
          if (strings.size() == 0) {
            break;
          }
          String file = syncDirectory.relativize(reader.getLastFileRead()).toString();
          List<Event> events = Lists.newArrayList();
          for (String s : strings) {
            counterGroup.incrementAndGet("syncdir.lines.read");
            events.add(createEvent(s, file));
          }
          getChannelProcessor().processEventBatch(events);
          reader.commit();
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in Runnable", t);
        if (t instanceof Error) {
          throw (Error) t;
        }
      }
    }
  }
}
