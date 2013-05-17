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

package org.apache.flume.input;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/** A class with information about a file being processed. */
public class ResumableFileLineReader {
  private static final Logger logger = LoggerFactory.getLogger(ResumableFileLineReader
      .class);
  private File file;
  private FileChannel ch;
  private ByteBuffer bb;
  private ByteArrayOutputStream lineOut;
  private boolean skipLF;
  private boolean fileEnded;
  private File statsFile;
  private File finishedStatsFile;
  private FileOutputStream statsFileOut;
  private long markedPosition = 0;
  private long readingPosition = 0;
  private boolean eof = false;
  private boolean finished = false;
  private boolean damaged = false;

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
  public ResumableFileLineReader(File file,
                                 boolean fileEnded,
                                 String statsFilePrefix,
                                 String statsFileSuffix,
                                 String finishedStatsFileSuffix) throws IOException {
    this.file = file;
    if (file.isDirectory())
      throw new IOException("file '" + file + "' is a directory");
    ch = new FileInputStream(file).getChannel();
    bb = ByteBuffer.allocateDirect(128 * 1024); // 128K
    bb.limit(0);
    lineOut = new ByteArrayOutputStream(200);
    this.skipLF = false;
    this.fileEnded = fileEnded;

    /* stats file */
    statsFile = new File(file.getParent(), statsFilePrefix + file.getName() + statsFileSuffix);
    finishedStatsFile = new File(file.getParent(), statsFilePrefix + file.getName() + finishedStatsFileSuffix);

    /* get previous line position */
    retrieveStats();
  }

  /** Retrieve previous line position. */
  private void retrieveStats() throws IOException {
    logger.debug("retrieving status for file '{}'", file);
    finished = finishedStatsFile.exists();
    if (finished) {
      logger.debug("found stats file: '{}', no more reading needed", finishedStatsFile);
      return;
    }
    if (statsFile.exists()) {
      logger.debug("found stats file: '{}'", statsFile);
      BufferedReader reader = new BufferedReader(new FileReader(statsFile));
      List<String> lines = new ArrayList<String>();
      try {
        String line = null;
        while ((line = reader.readLine()) != null) {
          lines.add(line);
        }
      } finally {
        reader.close();
      }
      if (lines.size() == 0 || lines.get(0).length() == 0) {
        damaged = true;
        logger.error("stats file '{}' damaged, aborting...",
            statsFile);
        return;
      }
      try {
        readingPosition = markedPosition = Long.valueOf(lines.get(0));
        ch.position(markedPosition);
      } catch (NumberFormatException e) {
        damaged = true;
        logger.warn("stats file '{}' format error, aborting...",
            file.getAbsolutePath());
        throw e;
      }
    }
    logger.debug("opened stats file '{}', got line number '{}'", statsFile, markedPosition);
  }

  private void ensureOpen() throws IOException {
    if (!ch.isOpen()) {
      throw new IOException("the channel of file '" + file + "' is closed");
    }
  }

  public byte[] readLine() throws IOException {
    /* this file was already marked as finished, EOF now */
    if (finished || damaged || eof) return null;
    ensureOpen();

    lineOut.reset();
    while (true) {
      if (!bb.hasRemaining()) {
        bb.clear();
        if (ch.read(bb) == -1) {
          eof = true;
          if (lineOut.size() > 0)
            return lineOut.toByteArray();
          else
            return null;
        }
        bb.flip();
      }
      while (bb.hasRemaining()) {
        byte b = bb.get();
        char c = (char) b;
        readingPosition++;
        if (skipLF) {
          skipLF = false;
          if (c != '\n') {
            bb.position(bb.position() - 1);
            readingPosition--;
          }
          return lineOut.toByteArray();
        }
        if (c == '\n') {
          return lineOut.toByteArray();
        }
        if (c == '\r') {
          skipLF = true;
          continue;
        }
        lineOut.write(b);
      }
    }
  }

  public void close() throws IOException {
    logger.debug("file '{}': closing...", file);
    if (null != ch)
      ch.close();
    if (null != statsFileOut)
      statsFileOut.close();
    logger.debug("file '{}': closed", file);
  }

  /** Record the position of current reading file into stats file. */
  public void commit() throws IOException {
    if (finished || damaged) return;

    logger.debug("committing '{}'", statsFile);
    /* open stat file for write */
    try {
      if (null == statsFileOut)
        statsFileOut = new FileOutputStream(statsFile, false);
    } catch (IOException ioe) {
      damaged = true;
      throw new IOException("cannot create stats file for log file '" + file +
          "', this class needs stats file to function normally", ioe);
    }
    statsFileOut.getChannel().position(0);
    statsFileOut.write(String.valueOf(readingPosition).getBytes());
    statsFileOut.flush();
    markedPosition = readingPosition;
    logger.debug("'{}': written", statsFile);
    if (fileEnded && eof) {
      logger.debug("sealing stats file, renaming from '{}' to '{}'",
          statsFile, finishedStatsFile);
      statsFileOut.close();
      statsFile.renameTo(finishedStatsFile);
      logger.debug("sealed");
      finished = true;
    }
  }

  /** Rewind reading position to previous recorded position. */
  public void reset() throws IOException {
    if (finished || damaged) return;

    bb.clear();
    ch.position(readingPosition);
    readingPosition = markedPosition;
    logger.info("file '{}': reverted to previous position: [{}]",
        file, String.valueOf(readingPosition));
  }

  public File getFile() {
    return file;
  }
}

