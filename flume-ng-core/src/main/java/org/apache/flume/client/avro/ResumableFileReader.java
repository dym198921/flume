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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/** A class with information about a file being processed. */
public class ResumableFileReader extends Reader {
  private static final Logger logger = LoggerFactory.getLogger(ResumableFileReader
      .class);
  private Path file;
  private FileInputStream in;
  private BufferedReader reader;
  private boolean fileEnded;
  private Path statsFile;
  private Path finishedStatsFile;
  private BufferedWriter statsFileWriter;
  private long markPos = 0;
  private long readingPos = 0;
  private boolean finished = false;

  /**
   * @param file                    to read
   * @param fileEnded               hinted by revoker, if true, then this file
   *                                should be treated as ended, no more reading
   *                                after this batch. if false, always reading
   *                                from this file
   * @param statsFileSuffix
   * @param finishedStatsFileSuffix
   * @throws IOException
   */
  public ResumableFileReader(Path file,
                             boolean fileEnded,
                             String statsFileSuffix,
                             String finishedStatsFileSuffix) throws IOException {
    this.file = file;
    in = new FileInputStream(file.toFile());
    reader = new BufferedReader(
        new InputStreamReader(in, Charset.defaultCharset()));
    this.fileEnded = fileEnded;

    /* stats file */
    statsFile = Paths.get(file + statsFileSuffix);
    try {
      statsFileWriter = Files.newBufferedWriter(statsFile,
          Charset.defaultCharset());
    } catch (IOException ioe) {
      throw new IOException("cannot create stats file for log file '" +
          file.toAbsolutePath() + "', this class needs stats file to " +
          "function normally", ioe);
    }
    finishedStatsFile = Paths.get(file + finishedStatsFileSuffix);

    /* get previous reading position */
    retrieveStats();
    /* set reading position */
    in.getChannel().position(readingPos);

  }

  /**
   * Reads a line of text.  A line is considered to be terminated by any one of
   * a line feed ('\n'), a carriage return ('\r'), or a carriage return
   * followed
   * immediately by a linefeed.
   *
   * @return A String containing the contents of the line, not including any
   *         line-termination characters, or null if the end of the stream has
   *         been reached
   * @throws IOException If an I/O error occurs
   */
  public String readLine() throws IOException {
    /* this file was already marked as finished, EOF now */
    if (finished) return null;

    String line = reader.readLine();
    if (null != line) {
      readingPos += line.getBytes().length + 1;
      /* It's possible that the last read took us just to a file boundary and
         committed away. If so, the next resume read would just return EOF if
         the byte position is beyond file's actual size. */
    } else {
      // handle EOF
      readingPos -= 1;
    }
    return line;
  }

  @Deprecated
  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public void close() throws IOException {
    logger.debug("file '{}': closing...", file);
    reader.close();
    in.close();
    statsFileWriter.close();
    logger.debug("file '{}': closed", file);
  }

  /** record the position of current reading into stats file */
  public void commit() throws IOException {
    /* this file was already marked as finished, return now */
    if (finished) return;

    logger.debug("committing reading progress for file '{}'", file);
    logger.debug("stats file '{}': trying to flush to disk...", statsFile);
    statsFileWriter.write(String.valueOf(readingPos));
    statsFileWriter.flush();
    logger.debug("stats file '{}': written", statsFile);
    if (fileEnded) {
      logger.debug("file reading finished, moving stats file from '{}' to '{}'",
          statsFile, finishedStatsFile);
      Files.move(file, finishedStatsFile);
      finished = true;
    }
    logger.info("file '{}': successfully transferred", file);
  }

  /**
   * Rewind reading position to previous one.
   *
   * @throws IOException
   */
  public void reset() throws IOException {
    /* nothing changed since last read, EOF now */
    if (finished) return;

    in.getChannel().position(markPos);
    reader.close();
    reader = new BufferedReader(new InputStreamReader(in,
        Charset.defaultCharset()));
    readingPos = markPos;
    logger.info("file '{}': reverted to last read position [{}], " +
        "retry in next batch", file, String.valueOf(markPos));
  }

  public void retrieveStats() throws IOException {
    finished = Files.exists(finishedStatsFile);
    if (finished) {
      logger.debug("file '{}' marked as finished, no more processing needed",
          file);
      return;
    }

    List<String> lines = Files.readAllLines(statsFile,
        Charset.defaultCharset());
    try {
      readingPos = Long.valueOf(lines.get(0));
    } catch (NumberFormatException e) {
      logger.warn("stats file format error! file:{}",
          file.toAbsolutePath());
      throw e;
    } finally {
      logger.debug("file '{}': read in stats file '{}'", file, statsFile);
    }
  }

  public Path getFile() {
    return file;
  }
}