package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestGenericUDFUpdateValue {

    @Test
    public void testSimpleStruct() throws Exception {
        try (GenericUDFUpdateValue udf = new GenericUDFUpdateValue()) {
            ObjectInspector[] arguments = {
                    ObjectInspectorFactory.getStandardStructObjectInspector(asList("name", "age"),
                            asList(PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                                    PrimitiveObjectInspectorFactory.writableIntObjectInspector)),
                    PrimitiveObjectInspectorFactory.writableStringObjectInspector,
                    PrimitiveObjectInspectorFactory.writableIntObjectInspector
            };
            udf.initialize(arguments);

            // {"name": "haha", "age", 10} --> {"name": "haha", "age", 20}
            Object i1 = asList(new Text("haha"), new IntWritable(10));
            Object res = udf.evaluate(new GenericUDF.DeferredObject[]{
                    new GenericUDF.DeferredJavaObject(i1),
                    new GenericUDF.DeferredJavaObject(new Text("age")),
                    new GenericUDF.DeferredJavaObject(new IntWritable(20))});


            assertTrue(res instanceof List<?>);
            List<?> o = (List<?>) res;
            assertEquals(new Text("haha"), o.get(0));
            assertEquals(new IntWritable(20), o.get(1));
        }
    }
}
