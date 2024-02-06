package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGenericUDFSetValue {

    @Test
    public void testSimpleStruct() throws Exception {
        try (GenericUDFSetValue udf = new GenericUDFSetValue()) {
            ObjectInspector[] arguments = {
                    ObjectInspectorFactory.getStandardStructObjectInspector(asList("name", "age"),
                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                    PrimitiveObjectInspectorFactory.writableIntObjectInspector)),
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10} --> {"name": "haha", "age", 20}
            List<?> input = asList(new Text("haha"), new IntWritable(10));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(input),
                    new GenericUDF.DeferredJavaObject(new Text("age")),
                    new GenericUDF.DeferredJavaObject(new IntWritable(20))});


            // result
            assertTrue(res instanceof List<?>);
            List<?> o = (List<?>) res;
            assertEquals(2, o.size());
            assertEquals(new Text("haha"), o.get(0));
            assertEquals(new IntWritable(20), o.get(1));

            // original (값 유지 되는지 여부)
            assertEquals(2, input.size());
            assertEquals(new Text("haha"), input.get(0));
            assertEquals(new IntWritable(10), input.get(1)); // -- 값이 변경되도 기존 object 는 유지되어야한다.
        }
    }

    @Test
    public void testChildStruct() throws Exception {
        try (GenericUDFSetValue udf = new GenericUDFSetValue()) {
            ObjectInspector[] arguments = {
                    ObjectInspectorFactory.getStandardStructObjectInspector(
                            // key
                            asList("name", "age", "address"),
                            // value type
                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                    PrimitiveObjectInspectorFactory.writableIntObjectInspector,
                                    ObjectInspectorFactory.getStandardStructObjectInspector(
                                            // sub key
                                            asList("city", "is_asia"),
                                            // sub value type
                                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                                    PrimitiveObjectInspectorFactory.writableBooleanObjectInspector)

                                    )
                            )

                    ),
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector, // key   : address.city
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector, // value : new york -> seoul
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector, // key   : address.is_asia
                    PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,// value : false -> true
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10, "address": {"city": "new york", "is_asia", false}}
            // {"name": "haha", "age", 10, "address": {"city": "seoul", "is_asia", true}}
            Object input = asList(new Text("haha"), new IntWritable(10),
                    asList(new Text("new york"), new BooleanWritable(false)));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(input),
                    new GenericUDF.DeferredJavaObject(new Text("address.city")),
                    new GenericUDF.DeferredJavaObject(new Text("seoul")),
                    new GenericUDF.DeferredJavaObject(new Text("address.is_asia")),
                    new GenericUDF.DeferredJavaObject(new BooleanWritable(true))
            });

            // result
            assertTrue(res instanceof List<?>);
            List<?> o = (List<?>) res;
            assertEquals(3, o.size());
            assertEquals(new Text("haha"), o.get(0));
            assertEquals(new IntWritable(10), o.get(1));

            List<?> address = (List<?>) o.get(2);
            assertEquals(2, address.size());
            assertEquals(new Text("seoul"), address.get(0));
            assertEquals(new BooleanWritable(true), address.get(1));
        }

    }
}
