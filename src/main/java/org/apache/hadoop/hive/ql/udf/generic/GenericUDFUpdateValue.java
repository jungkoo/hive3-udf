package org.apache.hadoop.hive.ql.udf.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader;
import org.apache.hadoop.hive.serde2.json.HiveJsonWriter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Description(name = "update_value",
        value = "_FUNC_(named_struct(...), key, value))",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(f, \"status.is_delete\", true) FROM src LIMIT 1;\n"
                + "  {\"name\": \"haha\", \"status\": {\"is_delete\": true}")
public class GenericUDFUpdateValue extends GenericUDF {

    private TextConverter keysConverter;
    private HiveJsonReader jsonReader;

    private HiveJsonWriter jsonWriter;

    private ObjectMapper objectMapper;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 3, 3);
        checkArgPrimitive(arguments, 1); // KEY
        checkArgPrimitive(arguments, 2); // NEW VALUE
        if(arguments[0].getCategory() != ObjectInspector.Category.STRUCT) {
            throw new UDFArgumentException("first argument is not struct type : " + arguments[0].getTypeName());
        }
        keysConverter  = new TextConverter((PrimitiveObjectInspector) arguments[1]);
        String typeStr =arguments[0].getTypeName(); // struct<...>
        try {
            final TypeInfo t = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
            final ObjectInspector oi = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(t);
            jsonReader = new HiveJsonReader(oi);
            jsonReader.enable(HiveJsonReader.Feature.PRIMITIVE_TO_WRITABLE);
            jsonWriter = new HiveJsonWriter();
            objectMapper = new ObjectMapper();
        } catch (Exception e) {
            throw new UDFArgumentException(getFuncName() + ": Error parsing typestring: " + e.getMessage());
        }
        return jsonReader.getObjectInspector();
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object source = arguments[0].get();
        final Iterator<String> keys = Arrays.asList(keysConverter.convert(arguments[1].get()).toString().split("\\.")).iterator();
        final Object newValue = arguments[2].get();

        if (source == null) {
            return null;
        }

        try {
            final String json = jsonWriter.write(source, jsonReader.getObjectInspector());
            // update - set value
            final JsonNode root = objectMapper.reader().readTree(json);
            if (root == null) {
                return null;
            }

            JsonNode current = root;
            while(keys.hasNext()) {
                final String k = keys.next();
                if (!current.has(k)) {
                    System.out.println("[WARN] 해당 key 없음 , 오리지널 값 리턴");
                    return source;
                }
                if (keys.hasNext()) {
                    current = current.get(k);
                } else {
                    if (!(current instanceof ObjectNode)) {
                        throw new RuntimeException("update failed !!!");
                    }

                    if (newValue == null) {
                        ((ObjectNode)current).putNull(k);
                    } else if (newValue instanceof BooleanWritable) {
                        ((ObjectNode)current).put(k, ((BooleanWritable) newValue).get());
                    } else if (newValue instanceof Text) {
                        ((ObjectNode)current).put(k, newValue.toString());
                    } else if (newValue instanceof IntWritable) {
                        ((ObjectNode)current).put(k, ((IntWritable)newValue).get());
                    } else if (newValue instanceof LongWritable) {
                        ((ObjectNode)current).put(k, ((LongWritable) newValue).get());
                    } else if (newValue instanceof FloatWritable) {
                        ((ObjectNode)current).put(k, ((FloatWritable)newValue).get());
                    } else if (newValue instanceof DoubleWritable) {
                        ((ObjectNode)current).put(k, ((DoubleWritable) newValue).get());
                    } else {
                        throw new RuntimeException("unknown type : " + newValue.getClass());
                    }
                }
            }

            return jsonReader.parseStruct(root.toString());
        } catch (Exception e) {
            throw new HiveException("Error parsing json: " + e.getMessage(), e);
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("update_value", children);
    }
}
