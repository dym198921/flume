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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.Files.createTempFile;

/**
 * A {@link LineReader} which reads log data from files stored in a
 * syncDirectory or subdirectories and put an empty mark file for each file
 * when
 * sync were done(through {@link #readLine()} calls). The user must {@link
 * #commit()} each read, to indicate that the lines have been fully processed.
 */
public class DirectorySyncFileLineReader implements LineReader {
  private static final Logger logger = LoggerFactory.getLogger(
      DirectorySyncFileLineReader.class);
  private Path directory;
  private String endFileSuffix;
  private String statsFileSuffix;
  private String finishedStatsFileSuffix;
  private List<Path> files = new ArrayList<Path>();
  private Iterator<Path> filesIterator;
  private Optional<ResumableUTF8FileReader> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. * */
  private Optional<ResumableUTF8FileReader> lastFileRead = Optional.absent();
  private boolean committed = true;
  /** A flag to signal an un-recoverable error has occured. */
  private boolean disabled = false;

  /**
   * Create a DirectorySyncFileLineReader to watch the given syncDirectory.
   *
   * @param directory               The syncDirectory to watch
   * @param endFileSuffix           The suffix to append to completed files
   * @param statsFileSuffix
   * @param finishedStatsFileSuffix
   */
  public DirectorySyncFileLineReader(Path directory,
                                     final String endFileSuffix,
                                     final String statsFileSuffix,
                                     final String finishedStatsFileSuffix) {
    // Verify syncDirectory exists and is readable/writable
    Preconditions.checkNotNull(directory);
    Preconditions.checkState(Files.exists(directory),
        "Directory does not exist: " + directory.toAbsolutePath());
    Preconditions.checkState(Files.isDirectory(directory),
        "Path is not a directory: " + directory.toAbsolutePath());
    this.directory = directory;

    // Do a canary test to make sure we have access to test syncDirectory
    try {
      Path tmpFile = createTempFile("flume", "test");
      Files.write(tmpFile, "testing flume file permissions\n".getBytes(),
          StandardOpenOption.WRITE);
      Files.readAllLines(tmpFile, Charset.defaultCharset());
      Files.delete(tmpFile);
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
          " in the sync syncDirectory: " + directory, e);
    }
    this.endFileSuffix = endFileSuffix;
    this.statsFileSuffix = statsFileSuffix;
    this.finishedStatsFileSuffix = finishedStatsFileSuffix;
  }

  /**
   * Return the relative file filePath which generated the data from the last
   * successful {@link #readLine} or {@link #readLines(int)} call. Returns null
   * if called before any file contents are read.
   */
  public Path getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getFile();
  }

  /** Commit the last lines which were read. */
  public void commit() throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    currentFile.get().commit();
    committed = true;
  }

  @Override
  public String readLine() throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    List<String> lines = readLines(1);
    if (lines.size() == 0) {
      return null;
    }
    return lines.get(0);
  }

  @Override
  public List<String> readLines(int n) throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    if (!committed) {
      if (!currentFile.isPresent()) {
        throw new IllegalStateException("File should not roll when " +
            "commit is outstanding.");
      }
      logger.info("Last read was never committed - resetting mark position.");
      currentFile.get().reset();
      committed = true;
    }

    // Check if new files have arrived since last call
    if (!currentFile.isPresent()) {
      currentFile = getNextFile();
    }
    // Return empty list if no new files
    if (!currentFile.isPresent()) {
      return Collections.emptyList();
    }

    /* It's possible that the last read took us just up to a file boundary.
     * If so, try to roll to the next file, if there is one. */
    String outLine;
    while ((outLine = currentFile.get().readLine()) == null) {
      retireCurrentFile();
      currentFile = getNextFile();
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
    }
    List<String> out = Lists.newArrayList();
    while (outLine != null) {
      out.add(outLine);
      if (out.size() == n) {
        break;
      }
      outLine = currentFile.get().readLine();
    }

    committed = false;
    lastFileRead = currentFile;
    return out;
  }

  /**
   * If these operations fail in a way that may cause duplicate log entries, an
   * error is logged but no exceptions are thrown. If these operations fail in
   * a
   * way that indicates potential misuse of the spooling syncDirectory, a
   * FlumeException will be thrown.
   *
   * @throws FlumeException if files do not conform to spooling assumptions
   */
  private void retireCurrentFile() throws IOException {
    Preconditions.checkState(currentFile.isPresent());

    logger.debug("file '{}': retiring...", currentFile.get().getFile());
    currentFile.get().commit();
    currentFile.get().close();
  }

  @Override
  public void close() throws IOException {
    if (currentFile.isPresent())
      currentFile.get().close();
    if (null != files)
      files.clear();
  }

  /**
   * Find the next file in the directory by walking through directory tree.
   *
   * @return the next file
   */
  private Optional<ResumableUTF8FileReader> getNextFile() throws IOException {
    if (null != filesIterator && !filesIterator.hasNext()) {
      filesIterator = null;
      files.clear();
      return Optional.absent();
    }
    if (null == filesIterator) {
      try {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (attrs.isRegularFile()) {
              String fileStr = file.toString();
              if (!(fileStr.endsWith(endFileSuffix) ||
                  fileStr.endsWith(statsFileSuffix) ||
                  fileStr.endsWith(finishedStatsFileSuffix))) {
                if (!Files.exists(file.resolveSibling(file.getFileName() + finishedStatsFileSuffix)))
                  files.add(file);
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });
        filesIterator = files.iterator();
      } catch (IOException e) {
        logger.error("unable to start reading from directory '{}'", directory);
        throw new IllegalStateException(e);
      }
    }

    Path nextFile;
    boolean fileEnded;
    if (!filesIterator.hasNext())
      return Optional.absent();
    /* checking file's reading progress, skip if needed */
    nextFile = filesIterator.next();
    logger.debug("treating next file: {}", nextFile);
    fileEnded = Files.exists(
        nextFile.resolveSibling(nextFile.getFileName() + endFileSuffix));
    logger.debug("file {} marked as ended", nextFile);
    try {
      ResumableUTF8FileReader file = new ResumableUTF8FileReader(nextFile, fileEnded,
          statsFileSuffix, finishedStatsFileSuffix);
      return Optional.of(file);
    } catch (IOException e) {
      disabled = true;
      logger.error("Exception opening file: " + nextFile, e);
      return Optional.absent();
    }
  }
}
