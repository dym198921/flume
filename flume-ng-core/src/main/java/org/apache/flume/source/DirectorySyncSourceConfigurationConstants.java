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

public class DirectorySyncSourceConfigurationConstants {
    /** Directory to sync. */
    public static final String SYNC_DIRECTORY = "syncDir";

    /** Suffix appended to files when they are finished being sent. */
    public static final String SYNCED_FILE_SUFFIX = "fileSuffix";
    public static final String DEFAULT_SYNCED_FILE_SUFFIX = ".COMPLETED";

    /** Header in which to put relative filename. */
    public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
    public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

    /** Whether to include filename in a header. */
    public static final String FILENAME_HEADER = "fileHeader";
    public static final boolean DEFAULT_FILE_HEADER = false;

    /** What size to batch with before sending to ChannelProcessor. */
    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 10;

    /** Maximum number of lines to buffer between commits. */
    public static final String BUFFER_MAX_LINES = "bufferMaxLines";
    public static final int DEFAULT_BUFFER_MAX_LINES = 100;

    /** Maximum length of line (in characters) in buffer between commits. */
    public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
    public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;
}
