package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DecodingFormatFactory;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.EncodingFormatFactory;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FileSystemFormatFactory;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * @author: lcy
 * @date: 2023/9/7
 **/
public class AllModeFileSystemTableFactory extends FileSystemTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {
    public static final String IDENTIFIER = "allmode_filesystem";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        super.createDynamicTableSink(context);

        System.out.println("打印FileSystemTableSink的构造。。。。");
        printConstructors(FileSystemTableSink.class);

        return new AllModeFileSystemTableSink(
                context,
                discoverDecodingFormat(context, BulkReaderFormatFactory.class),
                discoverDecodingFormat(context, DeserializationFormatFactory.class),
                discoverFormatFactory(context),
                discoverEncodingFormat(context, BulkWriterFormatFactory.class),
                discoverEncodingFormat(context, SerializationFormatFactory.class));
    }

    public static void printConstructors(Class c) {
        //返回包含Constructor对象的数组，其中包含了Class对象所描述的类的所有构造器
        Constructor[] constructors = c.getDeclaredConstructors();

        //遍历构造器（构造方法）
        for (Constructor constructor : constructors) {
            //获取构造方法名
            String name = constructor.getName();
            System.out.print("  ");
            //获取modifiers中位设置的修饰符的字符串表示（public private)
            String modifies = Modifier.toString(constructor.getModifiers());
            if (modifies.length() > 0) {
                System.out.print(modifies + " ");
            }
            System.out.print(name + "(");

            //获取参数类型的Class对象数组
            Class[] paramTypes = constructor.getParameterTypes();
            for (int i = 0; i < paramTypes.length; i++) {
                if (i > 0) {
                    System.out.print(", ");
                }
                System.out.print(paramTypes[i].getName());
            }
            System.out.println(");");
        }
    }

    private <I, F extends EncodingFormatFactory<I>> EncodingFormat<I> discoverEncodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverEncodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private <I, F extends DecodingFormatFactory<I>> DecodingFormat<I> discoverDecodingFormat(
            Context context, Class<F> formatFactoryClass) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        if (formatFactoryExists(context, formatFactoryClass)) {
            return helper.discoverDecodingFormat(formatFactoryClass, FactoryUtil.FORMAT);
        } else {
            return null;
        }
    }

    private boolean formatFactoryExists(Context context, Class<?> factoryClass) {
        Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
        String identifier = options.get(FactoryUtil.FORMAT);
        if (identifier == null) {
            throw new ValidationException(
                    String.format(
                            "Table options do not contain an option key '%s' for discovering a format.",
                            FactoryUtil.FORMAT.key()));
        }

        final List<Factory> factories = new LinkedList<>();
        ServiceLoader.load(Factory.class, context.getClassLoader())
                .iterator()
                .forEachRemaining(factories::add);

        final List<Factory> foundFactories =
                factories.stream()
                        .filter(f -> factoryClass.isAssignableFrom(f.getClass()))
                        .collect(Collectors.toList());

        final List<Factory> matchingFactories =
                foundFactories.stream()
                        .filter(f -> f.factoryIdentifier().equals(identifier))
                        .collect(Collectors.toList());

        return !matchingFactories.isEmpty();
    }


    private FileSystemFormatFactory discoverFormatFactory(Context context) {
        if (formatFactoryExists(context, FileSystemFormatFactory.class)) {
            Configuration options = Configuration.fromMap(context.getCatalogTable().getOptions());
            String identifier = options.get(FactoryUtil.FORMAT);
            return FactoryUtil.discoverFactory(
                    Thread.currentThread().getContextClassLoader(),
                    FileSystemFormatFactory.class,
                    identifier);
        } else {
            return null;
        }
    }
}
