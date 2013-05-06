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

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/** A class with information about a file being processed. */
public class ResumableUTF8FileReader extends Reader {
  private static final Logger logger = LoggerFactory.getLogger(ResumableUTF8FileReader
      .class);
  private Path file;
  private FileChannel ch;
  private LineOrientedUTF8Decoder decoder;
  private ByteBuffer bb;
  private CharBuffer cb;
  private boolean fileEnded;
  private Path statsFile;
  private Path finishedStatsFile;
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
  public ResumableUTF8FileReader(Path file,
                                 boolean fileEnded,
                                 String statsFileSuffix,
                                 String finishedStatsFileSuffix) throws IOException {
    this.file = file;
    if (Files.isDirectory(file))
      throw new IOException("file '" + file + "' is a directory");
    ch = (FileChannel) Files.newByteChannel(file);
    decoder = new LineOrientedUTF8Decoder();
    bb = ByteBuffer.allocateDirect(128 * 1024); // 128K
    bb.limit(0);
    cb = CharBuffer.allocate(1024);
    this.fileEnded = fileEnded;

    /* stats file */
    statsFile = Paths.get(file + statsFileSuffix);
    finishedStatsFile = Paths.get(file + finishedStatsFileSuffix);

    /* get previous line position */
    retrieveStats();
  }

  /** Retrieve previous line position. */
  private void retrieveStats() throws IOException {
    logger.debug("retrieving status for file '{}'", file);
    finished = Files.exists(finishedStatsFile);
    if (finished) {
      logger.debug("found stats file: '{}', no more reading needed", finishedStatsFile);
      return;
    }
    if (Files.exists(statsFile)) {
      logger.debug("found stats file: '{}'", statsFile);
      List<String> lines = Files.readAllLines(statsFile, Charset.defaultCharset());
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
            file.toAbsolutePath());
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
    if (finished || damaged || eof) return null;
    ensureOpen();

    StringBuffer sb = new StringBuffer();
    while (true) {
      // our customized decoder returns with EOL were encountered
      int bytePos = bb.position();
      CoderResult cr = decoder.decode(bb, cb, eof);
      readingPosition += bb.position() - bytePos;
      if (cr.isUnderflow()) {
        if (eof)
          break;
        if (!cb.hasRemaining())
          break;
        if (!bb.hasRemaining()) {
          bb.compact();
        }
        int n = ch.read(bb);
        bb.flip();
        if (n < 0)
          eof = true;
        continue;
      }
      if (cr.isOverflow()) {
        cb.position(cb.position() - 1);
        char c = cb.get();
        if (c == '\n') {
          // skip line feed
          cb.position(cb.position() - 1);
          break;
        }
        cb.clear();
        continue;
      }
      if (cr.isMalformed()) {
        throw new IOException("file '" + file + "' is not a UTF-8 encoded file");
      }
      if (cr.isUnmappable() || cr.isError()) {
        throw new IOException("file '" + file + "' decoding error");
      }
    }

    cb.flip();
    if (cb.length() == 0)
      return null;
    sb.append(cb);
    cb.clear();
    return sb.toString();
  }

  @Deprecated
  @Override
  /**
   * Do not call this method, disabled.
   */
  public int read(char[] cbuf, int off, int len) throws IOException {
    return 0;
  }

  public void close() throws IOException {
    logger.debug("file '{}': closing...", file);
    if (null != decoder)
      decoder.reset();
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
        statsFileOut = new FileOutputStream(statsFile.toFile(), false);
    } catch (IOException ioe) {
      damaged = true;
      throw new IOException("cannot create stats file for log file '" + file +
          "', this class needs stats file to function normally", ioe);
    }
    statsFileOut.getChannel().position(0);
    statsFileOut.write(String.valueOf(readingPosition).getBytes());
    statsFileOut.flush();
    logger.debug("'{}': written", statsFile);
    if (fileEnded && eof) {
      logger.debug("sealing stats file, renaming from '{}' to '{}'",
          statsFile, finishedStatsFile);
      statsFileOut.close();
      Files.move(statsFile, finishedStatsFile);
      logger.debug("sealed");
      finished = true;
    }
  }

  /** Rewind reading position to previous recorded position. */
  public void reset() throws IOException {
    if (finished || damaged) return;

    ch.position(readingPosition);
    readingPosition = markedPosition;
    logger.info("file '{}': reverted to previous position: [{}]",
        file, String.valueOf(readingPosition));
  }

  public Path getFile() {
    return file;
  }

  private static class LineOrientedUTF8Decoder extends CharsetDecoder {
    private boolean skipLF = false;

    private LineOrientedUTF8Decoder() {
      super(Charset.forName("UTF-8"), 1.0f, 1.0f);
    }

    private static boolean isNotContinuation(int b) {
      return (b & 0xc0) != 0x80;
    }

    //  [C2..DF] [80..BF]
    private static boolean isMalformed2(int b1, int b2) {
      return (b1 & 0x1e) == 0x0 || (b2 & 0xc0) != 0x80;
    }

    //  [E0]     [A0..BF] [80..BF]
    //  [E1..EF] [80..BF] [80..BF]
    private static boolean isMalformed3(int b1, int b2, int b3) {
      return (b1 == (byte) 0xe0 && (b2 & 0xe0) == 0x80) ||
          (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80;
    }

    //  [F0]     [90..BF] [80..BF] [80..BF]
    //  [F1..F3] [80..BF] [80..BF] [80..BF]
    //  [F4]     [80..8F] [80..BF] [80..BF]
    //  only check 80-be range here, the [0xf0,0x80...] and [0xf4,0x90-...]
    //  will be checked by Character.isSupplementaryCodePoint(uc)
    private static boolean isMalformed4(int b2, int b3, int b4) {
      return (b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80 ||
          (b4 & 0xc0) != 0x80;
    }

    private static CoderResult lookupN(ByteBuffer src, int n) {
      for (int i = 1; i < n; i++) {
        if (isNotContinuation(src.get()))
          return CoderResult.malformedForLength(i);
      }
      return CoderResult.malformedForLength(n);
    }

    private static CoderResult malformedN(ByteBuffer src, int nb) {
      switch (nb) {
        case 1:
          int b1 = src.get();
          if ((b1 >> 2) == -2) {
            // 5 bytes 111110xx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
            if (src.remaining() < 4)
              return CoderResult.UNDERFLOW;
            return lookupN(src, 5);
          }
          if ((b1 >> 1) == -2) {
            // 6 bytes 1111110x 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx 10xxxxxx
            if (src.remaining() < 5)
              return CoderResult.UNDERFLOW;
            return lookupN(src, 6);
          }
          return CoderResult.malformedForLength(1);
        case 2:                    // always 1
          return CoderResult.malformedForLength(1);
        case 3:
          b1 = src.get();
          int b2 = src.get();    // no need to lookup b3
          return CoderResult.malformedForLength(
              ((b1 == (byte) 0xe0 && (b2 & 0xe0) == 0x80) ||
                  isNotContinuation(b2)) ? 1 : 2);
        case 4:  // we don't care the speed here
          b1 = src.get() & 0xff;
          b2 = src.get() & 0xff;
          if (b1 > 0xf4 ||
              (b1 == 0xf0 && (b2 < 0x90 || b2 > 0xbf)) ||
              (b1 == 0xf4 && (b2 & 0xf0) != 0x80) ||
              isNotContinuation(b2))
            return CoderResult.malformedForLength(1);
          if (isNotContinuation(src.get()))
            return CoderResult.malformedForLength(2);
          return CoderResult.malformedForLength(3);
        default:
          assert false;
          return null;
      }
    }

    private static CoderResult malformed(ByteBuffer src, int sp,
                                         CharBuffer dst, int dp,
                                         int nb) {
      src.position(sp - src.arrayOffset());
      CoderResult cr = malformedN(src, nb);
      updatePositions(src, sp, dst, dp);
      return cr;
    }

    private static CoderResult malformed(ByteBuffer src,
                                         int mark, int nb) {
      src.position(mark);
      CoderResult cr = malformedN(src, nb);
      src.position(mark);
      return cr;
    }

    private static CoderResult xflow(Buffer src, int sp, int sl,
                                     Buffer dst, int dp, int nb) {
      updatePositions(src, sp, dst, dp);
      return (nb == 0 || sl - sp < nb)
          ? CoderResult.UNDERFLOW : CoderResult.OVERFLOW;
    }

    private static CoderResult xflow(Buffer src, int mark, int nb) {
      CoderResult cr = (nb == 0 || src.remaining() < (nb - 1))
          ? CoderResult.UNDERFLOW : CoderResult.OVERFLOW;
      src.position(mark);
      return cr;
    }

    /** for using with EOL */
    private static CoderResult overflow(Buffer src, int mark) {
      return CoderResult.OVERFLOW;
    }

    static final void updatePositions(Buffer src, int sp,
                                      Buffer dst, int dp) {
      src.position(sp - src.arrayOffset());
      dst.position(dp - dst.arrayOffset());
    }

    private CoderResult decodeArrayLoop(ByteBuffer src,
                                        CharBuffer dst) {
      // This method is optimized for ASCII input.
      byte[] sa = src.array();
      int sp = src.arrayOffset() + src.position();
      int sl = src.arrayOffset() + src.limit();

      char[] da = dst.array();
      int dp = dst.arrayOffset() + dst.position();
      int dl = dst.arrayOffset() + dst.limit();
      int dlASCII = dp + Math.min(sl - sp, dl - dp);

      while (sp < sl) {
        int b1 = sa[sp];
        if (b1 < 0 && skipLF) {
          // stop processing when previous line ends with '\r'
          skipLF = false;
          if (dp >= dlASCII)
            return xflow(src, sp, 1); // overflow
          da[dp++] = '\n';
          src.position(sp);           // rollback
          return overflow(src, sp);
        }

        if (b1 >= 0) {
          // 1 byte, 7 bits: 0xxxxxxx
          if (dp >= dl)
            return xflow(src, sp, sl, dst, dp, 1);
          char c = (char) b1;
          // handle EOL
          if (c == '\n' && skipLF) {
            skipLF = false;
            da[dp++] = c;
            return overflow(src, sp);
          }
          if (c == '\n') {
            da[dp++] = c;
            return overflow(src, sp);
          }
          if (c == '\r') {
            skipLF = true;
          } else {
            if (skipLF) {
              skipLF = false;
              da[dp++] = '\n';
              src.position(sp);           // rollback
              return overflow(src, sp);
            }
            skipLF = false;
            da[dp++] = c;
          }
          sp++;
        } else if ((b1 >> 5) == -2) {
          // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
          if (sl - sp < 2 || dp >= dl)
            return xflow(src, sp, sl, dst, dp, 2);
          int b2 = sa[sp + 1];
          if (isMalformed2(b1, b2))
            return malformed(src, sp, dst, dp, 2);
          da[dp++] = (char) (((b1 << 6) ^ b2)
              ^
              (((byte) 0xC0 << 6) ^
                  ((byte) 0x80 << 0)));
          sp += 2;
        } else if ((b1 >> 4) == -2) {
          // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
          if (sl - sp < 3 || dp >= dl)
            return xflow(src, sp, sl, dst, dp, 3);
          int b2 = sa[sp + 1];
          int b3 = sa[sp + 2];
          if (isMalformed3(b1, b2, b3))
            return malformed(src, sp, dst, dp, 3);
          da[dp++] = (char)
              ((b1 << 12) ^
                  (b2 << 6) ^
                  (b3 ^
                      (((byte) 0xE0 << 12) ^
                          ((byte) 0x80 << 6) ^
                          ((byte) 0x80 << 0))));
          sp += 3;
        } else if ((b1 >> 3) == -2) {
          // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
          if (sl - sp < 4 || dl - dp < 2)
            return xflow(src, sp, sl, dst, dp, 4);
          int b2 = sa[sp + 1];
          int b3 = sa[sp + 2];
          int b4 = sa[sp + 3];
          int uc = ((b1 << 18) ^
              (b2 << 12) ^
              (b3 << 6) ^
              (b4 ^
                  (((byte) 0xF0 << 18) ^
                      ((byte) 0x80 << 12) ^
                      ((byte) 0x80 << 6) ^
                      ((byte) 0x80 << 0))));
          if (isMalformed4(b2, b3, b4) ||
              // shortest form check
              !Character.isSupplementaryCodePoint(uc)) {
            return malformed(src, sp, dst, dp, 4);
          }
          da[dp++] = Character.highSurrogate(uc);
          da[dp++] = Character.lowSurrogate(uc);
          sp += 4;
        } else
          return malformed(src, sp, dst, dp, 1);
      }
      return xflow(src, sp, sl, dst, dp, 0);
    }

    private CoderResult decodeBufferLoop(ByteBuffer src,
                                         CharBuffer dst) {
      int mark = src.position();
      int limit = src.limit();
      while (mark < limit) {
        int b1 = src.get();

        if (b1 < 0 && skipLF) {
          // stop processing when previous line ends with '\r'
          skipLF = false;
          if (dst.remaining() < 1)
            return xflow(src, mark, 1); // overflow
          dst.put('\n');
          src.position(mark);           // rollback
          return overflow(src, mark);
        }
        if (b1 >= 0) {
          // 1 byte, 7 bits: 0xxxxxxx
          if (dst.remaining() < 1)
            return xflow(src, mark, 1); // overflow
          char c = (char) b1;
          // handle EOL
          if (c == '\n') {
            skipLF = false;
            dst.put(c);
            return overflow(src, mark);
          }
          if (c == '\r') {
            skipLF = true;
          } else {
            if (skipLF) {
              skipLF = false;
              dst.put('\n');
              src.position(src.position() - 1);
              return overflow(src, mark);
            }
            skipLF = false;
            dst.put(c);
          }
          mark++;
        } else if ((b1 >> 5) == -2) {
          // 2 bytes, 11 bits: 110xxxxx 10xxxxxx
          if (limit - mark < 2 || dst.remaining() < 1)
            return xflow(src, mark, 2);
          int b2 = src.get();
          if (isMalformed2(b1, b2))
            return malformed(src, mark, 2);
          dst.put((char) (((b1 << 6) ^ b2)
              ^
              (((byte) 0xC0 << 6) ^
                  ((byte) 0x80 << 0))));
          mark += 2;
        } else if ((b1 >> 4) == -2) {
          // 3 bytes, 16 bits: 1110xxxx 10xxxxxx 10xxxxxx
          if (limit - mark < 3 || dst.remaining() < 1)
            return xflow(src, mark, 3);
          int b2 = src.get();
          int b3 = src.get();
          if (isMalformed3(b1, b2, b3))
            return malformed(src, mark, 3);
          dst.put((char)
              ((b1 << 12) ^
                  (b2 << 6) ^
                  (b3 ^
                      (((byte) 0xE0 << 12) ^
                          ((byte) 0x80 << 6) ^
                          ((byte) 0x80 << 0)))));
          mark += 3;
        } else if ((b1 >> 3) == -2) {
          // 4 bytes, 21 bits: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
          if (limit - mark < 4 || dst.remaining() < 2)
            return xflow(src, mark, 4);
          int b2 = src.get();
          int b3 = src.get();
          int b4 = src.get();
          int uc = ((b1 << 18) ^
              (b2 << 12) ^
              (b3 << 6) ^
              (b4 ^
                  (((byte) 0xF0 << 18) ^
                      ((byte) 0x80 << 12) ^
                      ((byte) 0x80 << 6) ^
                      ((byte) 0x80 << 0))));
          if (isMalformed4(b2, b3, b4) ||
              // shortest form check
              !Character.isSupplementaryCodePoint(uc)) {
            return malformed(src, mark, 4);
          }
          dst.put(Character.highSurrogate(uc));
          dst.put(Character.lowSurrogate(uc));
          mark += 4;
        } else {
          return malformed(src, mark, 1);
        }
      }
      return xflow(src, mark, 0);
    }

    protected CoderResult decodeLoop(ByteBuffer src,
                                     CharBuffer dst) {
      if (src.hasArray() && dst.hasArray())
        return decodeArrayLoop(src, dst);
      else
        return decodeBufferLoop(src, dst);
    }
  }
}

