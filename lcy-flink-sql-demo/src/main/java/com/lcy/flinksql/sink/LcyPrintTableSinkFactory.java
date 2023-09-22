package com.lcy.flinksql.sink;

import org.apache.flink.api.common.functions.util.PrintSinkOutputWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * @author: lcy
 * @date: 2023/9/8
 **/
public class LcyPrintTableSinkFactory implements DynamicTableSinkFactory {

    public static final String IDENTIFIER = "lcy-print";

    public static final ConfigOption<String> PRINT_IDENTIFIER =
            key("print-identifier")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Message that identify print and is prefixed to the output of the value.");

    public static final ConfigOption<Boolean> STANDARD_ERROR =
            key("standard-error")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "True, if the format should print to standard error instead of standard out.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PRINT_IDENTIFIER);
        options.add(STANDARD_ERROR);
        options.add(FactoryUtil.SINK_PARALLELISM);
        return options;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(DynamicTableFactory.Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new PrintSink(
                context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType(),
                options.get(PRINT_IDENTIFIER),
                options.get(STANDARD_ERROR),
                options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private static class PrintSink implements DynamicTableSink {

        private final DataType type;
        private final String printIdentifier;
        private final boolean stdErr;
        private final @Nullable
        Integer parallelism;

        private PrintSink(
                DataType type, String printIdentifier, boolean stdErr, Integer parallelism) {
            this.type = type;
            this.printIdentifier = printIdentifier;
            this.stdErr = stdErr;
            this.parallelism = parallelism;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return requestedMode;
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(DynamicTableSink.Context context) {
            DynamicTableSink.DataStructureConverter converter = context.createDataStructureConverter(type);
            return SinkFunctionProvider.of(
                    new RowDataPrintFunction(converter, printIdentifier, stdErr), parallelism);
        }

        @Override
        public DynamicTableSink copy() {
            return new PrintSink(type, printIdentifier, stdErr, parallelism);
        }

        @Override
        public String asSummaryString() {
            return "Print to " + (stdErr ? "System.err" : "System.out");
        }
    }

    /**
     */
    private static class RowDataPrintFunction extends RichSinkFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final DynamicTableSink.DataStructureConverter converter;
        private final PrintSinkOutputWriter<String> writer;

        private RowDataPrintFunction(
                DynamicTableSink.DataStructureConverter converter, String printIdentifier, boolean stdErr) {
            this.converter = converter;
            this.writer = new PrintSinkOutputWriter<>(printIdentifier, stdErr);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
            writer.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
        }

        @Override
        public void invoke(RowData value, SinkFunction.Context context) {
            Object data = converter.toExternal(value);
            assert data != null;
            writer.write(data.toString());
        }
    }
}
