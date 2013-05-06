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

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class TestDirectorySyncSource {
  static DirectorySyncSource source;
  static MemoryChannel channel;
  private Path tmpDir;

  @Before
  public void setUp() {
    source = new DirectorySyncSource();
    channel = new MemoryChannel();

    Context memContext = new Context();
    memContext.put("capacity", "5000");
    Configurables.configure(channel, memContext);

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    tmpDir = Paths.get("/home/vgu/tmp/src");
  }

  @After
  public void tearDown() throws IOException {
    //Files.walkFileTree(tmpDir, new SimpleFileVisitor<Path>() {
    //  @Override
    //  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
    //      throws IOException {
    //    Files.delete(file);
    //    return FileVisitResult.CONTINUE;
    //  }
    //
    //  @Override
    //  public FileVisitResult postVisitDirectory(Path dir, IOException exc)
    //      throws IOException {
    //    Files.delete(dir);
    //    return FileVisitResult.CONTINUE;
    //  }
    //});
  }

  @Test
  public void testPutFilenameHeader() throws IOException, InterruptedException {
    Context context = new Context();
    Path f1 = tmpDir.resolve("file1");

    String line = "file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n";
    Files.write(f1, line.getBytes());

    context.put(DirectorySyncSourceConfigurationConstants.SYNC_DIRECTORY,
        tmpDir.toString());
    context.put(DirectorySyncSourceConfigurationConstants.BATCH_SIZE,
        "100");

    Configurables.configure(source, context);
    source.start();
    Thread.sleep(500);
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    for (int i = 0; i < 100 && null != e; i++) {
      System.out.println(e.getHeaders().get(DirectorySyncSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY)
          + " body: " + e.getBody());
      e = channel.take();
    }
    txn.commit();
    txn.close();
    System.out.println("out");
  }

}
