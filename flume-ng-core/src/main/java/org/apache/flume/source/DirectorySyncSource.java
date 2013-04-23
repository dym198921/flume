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

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;

public class DirectorySyncSource extends AbstractSource implements
        Configurable, EventDrivenSource {

    private String syncDirectory;
    private String syncedFileSuffix;
    private String filenameHeaderKey;
    private boolean filenameHeader;
    private int batchSize;
    private int bufferMaxLines;
    private int bufferMaxLineLength;

    @Override
    public synchronized void start() {
//        TODO
        super.start();
    }

    @Override
    public synchronized void stop() {
//        TODO
        super.stop();
    }

    @Override
    public void configure(Context context) {
        syncDirectory = context.getString(
                DirectorySyncSourceConfigurationConstants.SYNC_DIRECTORY);
        Preconditions.checkState(syncDirectory != null,
                "Configuration must specify a sync directory");

        syncedFileSuffix = context.getString(
                DirectorySyncSourceConfigurationConstants.SYNCED_FILE_SUFFIX,
                DirectorySyncSourceConfigurationConstants.DEFAULT_SYNCED_FILE_SUFFIX);
        filenameHeaderKey = context.getString(
                DirectorySyncSourceConfigurationConstants.FILENAME_HEADER_KEY,
                DirectorySyncSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY);
        filenameHeader = context.getBoolean(
                DirectorySyncSourceConfigurationConstants.FILENAME_HEADER,
                DirectorySyncSourceConfigurationConstants.DEFAULT_FILE_HEADER);
        batchSize = context.getInteger(
                DirectorySyncSourceConfigurationConstants.BATCH_SIZE,
                DirectorySyncSourceConfigurationConstants.DEFAULT_BATCH_SIZE);
        bufferMaxLines = context.getInteger(
                DirectorySyncSourceConfigurationConstants.BUFFER_MAX_LINES,
                DirectorySyncSourceConfigurationConstants.DEFAULT_BUFFER_MAX_LINES);
        bufferMaxLineLength = context.getInteger(
                DirectorySyncSourceConfigurationConstants.BUFFER_MAX_LINE_LENGTH,
                DirectorySyncSourceConfigurationConstants.DEFAULT_BUFFER_MAX_LINE_LENGTH);
    }
}
