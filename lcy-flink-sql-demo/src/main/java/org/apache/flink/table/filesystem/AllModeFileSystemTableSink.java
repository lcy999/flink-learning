package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FileSystemFormatFactory;

import javax.annotation.Nullable;

/**
 * @author: lcy
 * @date: 2023/9/7
 **/
public class AllModeFileSystemTableSink extends FileSystemTableSink {
    AllModeFileSystemTableSink(DynamicTableFactory.Context context, @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat, @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat, @Nullable FileSystemFormatFactory formatFactory, @Nullable EncodingFormat<BulkWriter.Factory<RowData>> bulkWriterFormat, @Nullable EncodingFormat<SerializationSchema<RowData>> serializationFormat) {
        super(context, bulkReaderFormat, deserializationFormat, formatFactory, bulkWriterFormat, serializationFormat);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }
}
