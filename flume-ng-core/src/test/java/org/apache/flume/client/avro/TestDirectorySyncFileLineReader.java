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

import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

public class TestDirectorySyncFileLineReader {
  private Logger logger = LoggerFactory.getLogger
      (TestDirectorySyncFileLineReader.class);
  private File tmpDir1;

  @Before
  public void setUp() throws Exception {
    tmpDir1 = Files.createTempDir();
    logger.trace("temporary directory created: {}", tmpDir1.getAbsolutePath());
  }

  @After
  public void tearDown() throws Exception {
    // clean up temporary files
    logger.trace("cleaning up temp dir: {}", tmpDir1.getAbsolutePath());
    FileUtils.deleteDirectory(tmpDir1);
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
    File file1 = new File(tmpDir1, "file1");
    File file2 = new File(tmpDir1, "file2");
    List<String> lines = new LinkedList<String>();
    lines.add("line1\n");
    lines.add("line_2\r\n");
    lines.add("line__3\r");
    lines.add("line__4\r\n");
    lines.add("line_5\r");
    lines.add("line6");

    OutputStreamWriter writer = new OutputStreamWriter(new FileOutputStream(file1, false),
        Charset.forName("UTF-8"));
    for (String line : lines) {
      writer.write(line);
    }
    writer.close();
    writer = new OutputStreamWriter(new FileOutputStream(file2, false),
        Charset.forName("UTF-8"));
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
      Assert.assertEquals(lines.get(i % 6).replaceAll("\r", "").replaceAll("\n", ""), exactLines.get(i));
    }
  }
}
