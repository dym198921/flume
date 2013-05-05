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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class TestResumableUTF8FileReader {
  Path srcFile;

  @Before
  public void setUp() throws Exception {
    srcFile = Files.createTempFile(null, null);
  }

  @After
  public void tearDown() throws Exception {
    Files.delete(srcFile);
  }

  @Test
  public void testFileReading() throws IOException {
    List<String> lines = new LinkedList<String>();
    lines.add("line1\n");
    lines.add("line_2\r\n");
    lines.add("line__3\r");
    lines.add("line__4\r\n");
    lines.add("line_5\r");
    lines.add("line6\n");
    BufferedWriter bufferedWriter = Files.newBufferedWriter(srcFile, Charset.forName("UTF-8"));
    for (String line : lines) {
      bufferedWriter.write(line);
    }
    bufferedWriter.flush();
    bufferedWriter.close();

    // test reading
    for (int i = 0; i < lines.size(); i++) {
      ResumableUTF8FileReader reader = new ResumableUTF8FileReader(srcFile, false,
          ".FLUME-STATS", ".FLUME-COMPLETED");
      for (int j = 0; j < i; j++) {
        String incomingLine = reader.readLine();
        Assert.assertNotNull(lines.get(j));
        Assert.assertEquals(lines.get(j).replaceAll("\\r", "").replaceAll("\\n", ""), incomingLine);
      }
      reader.commit();
      reader.close();

      reader = new ResumableUTF8FileReader(srcFile, false,
          ".FLUME-STATS", ".FLUME-COMPLETED");
      for (int j = i; j < lines.size(); j++) {
        String incomingLine = reader.readLine();
        Assert.assertNotNull(lines.get(j));
        Assert.assertEquals(lines.get(j).replaceAll("\\r", "").replaceAll("\\n", ""), incomingLine);
      }
      reader.commit();
      reader.close();

      Files.delete(Paths.get(srcFile + ".FLUME-STATS"));
    }

    // test End of File sealing
    ResumableUTF8FileReader reader = new ResumableUTF8FileReader(srcFile, true,
        ".FLUME-STATS", ".FLUME-COMPLETED");
    while (reader.readLine() != null)
      continue;
    reader.commit();
    reader.close();
    Assert.assertTrue(Files.exists(Paths.get(srcFile + ".FLUME-COMPLETED")));
  }
}
