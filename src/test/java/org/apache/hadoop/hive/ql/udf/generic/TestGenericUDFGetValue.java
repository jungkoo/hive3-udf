package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGenericUDFGetValue {

    @Test
    public void testSimpleStruct() throws Exception {
        try (GenericUDFGetValue udf = new GenericUDFGetValue()) {
            ObjectInspector[] arguments = {
                    ObjectInspectorFactory.getStandardStructObjectInspector(asList("name", "age"),
                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                    PrimitiveObjectInspectorFactory.writableIntObjectInspector)),
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10}
            // "age" --> 10
            Object input = asList(new Text("haha"), new IntWritable(10));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(input),
                    new GenericUDF.DeferredJavaObject(new Text("age"))});

            assertTrue(res instanceof IntWritable);
            assertEquals(10, ((IntWritable)res).get());
        }
    }

    @Test
    public void testChildStruct() throws Exception {
        try (GenericUDFGetValue udf = new GenericUDFGetValue()) {
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
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10, "address": {"city": "new york", "is_asia", false}}
            // address.city -> new york
            Object input = asList(new Text("haha"), new IntWritable(10),
                    asList(new Text("new york"), new BooleanWritable(false)));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(input),
                    new GenericUDF.DeferredJavaObject(new Text("address.city"))});

            assertTrue(res instanceof Text);
            assertEquals("new york", res.toString());
        }

    }
}
