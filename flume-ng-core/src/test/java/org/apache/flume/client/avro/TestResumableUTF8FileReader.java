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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

public class TestResumableUTF8FileReader {
  File srcFile;

  @Before
  public void setUp() throws Exception {
    srcFile = File.createTempFile("res", null);
  }

  @After
  public void tearDown() throws Exception {
    srcFile.delete();
  }

  @Test
  public void testFileReading() throws IOException {
    List<String> lines = new LinkedList<String>();
    lines.add("line1\r");
    lines.add("line_2\r\n");
    lines.add("line__3\n");
    lines.add("\n");
    lines.add("\n");
    lines.add("line__4\r\n");
    lines.add("line_5\r");
    lines.add("line6\n");
    OutputStreamWriter osWriter = new OutputStreamWriter(new FileOutputStream(srcFile, false),
        Charset.forName("UTF-8"));
    for (String line : lines) {
      osWriter.write(line);
    }
    osWriter.flush();
    osWriter.close();

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

      new File(srcFile.getPath() + ".FLUME-STATS").delete();
    }

    // test End of File sealing
    ResumableUTF8FileReader reader = new ResumableUTF8FileReader(srcFile, true,
        ".FLUME-STATS", ".FLUME-COMPLETED");
    while (reader.readLine() != null)
      continue;
    reader.commit();
    reader.close();
    Assert.assertTrue(new File(srcFile.getPath() + ".FLUME-COMPLETED").exists());
  }
}
