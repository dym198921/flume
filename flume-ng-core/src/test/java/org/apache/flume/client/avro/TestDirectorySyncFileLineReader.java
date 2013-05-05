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

package org.apache.flume.client.avro;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;

public class TestDirectorySyncFileLineReader {
  private Logger logger = LoggerFactory.getLogger
      (TestDirectorySyncFileLineReader.class);
  private Path tmpDir1;

  @Before
  public void setUp() throws Exception {
    tmpDir1 = Files.createTempDirectory(null);
    logger.trace("temporary directory created: {}", tmpDir1.toAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    // clean up temporary files
    logger.trace("cleaning up temp dir: {}", tmpDir1.toAbsolutePath());
    Files.walkFileTree(tmpDir1, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
          throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc)
          throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Test
  public void testEmptyDirectory() throws IOException {
    DirectorySyncFileLineReader reader = new DirectorySyncFileLineReader(tmpDir1,
        ".done", ".FLUME-INCOMPLETE", ".FLUME-COMPLETED");
    String line = reader.readLine();
    Assert.assertNull(line);
    reader.close();
  }

  @Test
  public void testDirectorySync() throws IOException {
    Path file1 = Paths.get(tmpDir1.resolve("file1").toString());
    Path file2 = Paths.get(tmpDir1.resolve("file2").toString());
    List<String> lines = new LinkedList<String>();
    lines.add("line1\n");
    lines.add("line_2\r\n");
    lines.add("line__3\r");
    lines.add("line__4\r\n");
    lines.add("line_5\r");
    lines.add("line6");

    BufferedWriter writer = Files.newBufferedWriter(file1, Charset.forName("UTF-8"));
    for (String line : lines) {
      writer.write(line);
    }
    writer.close();
    writer = Files.newBufferedWriter(file2, Charset.forName("UTF-8"));
    for (String line : lines) {
      writer.write(line);
    }
    writer.close();
    //Files.createFile(Paths.get(file1 + ".done"));
    //Files.createFile(Paths.get(file2 + ".done"));

    DirectorySyncFileLineReader reader = new DirectorySyncFileLineReader(tmpDir1,
        ".done", ".FLUME-INCOMPLETE", ".FLUME-COMPLETED");
    List<String> exactLines = new LinkedList<String>();
    String line;
    while ((line = reader.readLine()) != null) {
      exactLines.add(line);
      reader.commit();
    }
    reader.close();

    for (int i = 0; i < exactLines.size(); i++) {
      Assert.assertEquals(lines.get(i % 6).replaceAll("\r", "").replaceAll("\n", ""),exactLines.get(i));
    }
  }
}
