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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class TestDirectorySyncFileLineReader {
  private Logger logger = LoggerFactory.getLogger
      (TestDirectorySyncFileLineReader.class);
  private Path tmpDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = Files.createTempDirectory(null);
    logger.trace("temporary directory created: {}", tmpDir.toAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    // clean up temporary files
    logger.trace("cleaning up temp dir: {}", tmpDir.toAbsolutePath());
    Files.walkFileTree(tmpDir, new SimpleFileVisitor<Path>() {
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
  public void testFileReaderByteCount() {
    Path testSourceFile = tmpDir.resolve("testBufferedReader.file");

    try {
      // generate source file
      Charset charset = Charset.forName("UTF-8");
      BufferedWriter writer = Files.newBufferedWriter(testSourceFile, charset);
      String line1 = "line1\r";
      String line2 = "line2 line2中文\r\n";
      String line3 = "line3 line3 中文 line3\n";
      writer.append(line1);
      writer.append(line2);
      writer.append(line3);
      writer.close();

      // initiate reader
      FileInputStream in = new FileInputStream(testSourceFile.toFile());
      InputStreamReader ir = new InputStreamReader(in, Charset.defaultCharset
          ());
      BufferedReader reader = new BufferedReader(ir, 1);
      long pos = 0;
      // read out a new lines and mark position
      String line;
      line = reader.readLine();
      pos += line.getBytes().length + 1;
      line = reader.readLine();
      pos += line.getBytes().length + 1;
      // next non-empty line is out target
      for (; ; ) {
        line = reader.readLine();
        if (line.length() > 0)
          break;
      }
      logger.debug("one line read, pos: {} \"{}\"", pos, line);
      reader.close();
      in.close();

      in = new FileInputStream(testSourceFile.toFile());
      reader = new BufferedReader(new InputStreamReader(in,
          Charset.defaultCharset()), 1);
      // fast forward and test the line
      in.getChannel().position(pos);
      String newLine;
      for (; ; ) {
        newLine = reader.readLine();
        if (newLine.length() > 0)
          break;
      }
      logger.debug("resume to previous position with {} bytes skipped: {}",
          pos, newLine);
      reader.close();
      in.close();

      logger.trace("comparing two lines:\n {}\n {}", line, newLine);
      org.junit.Assert.assertEquals("failure - strings not equal", line,
          newLine);
    } catch (IOException e) {
      logger.error("", e);
    }
  }
}
