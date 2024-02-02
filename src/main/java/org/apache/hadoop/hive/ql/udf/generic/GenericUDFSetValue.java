package org.apache.hadoop.hive.ql.udf.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader;
import org.apache.hadoop.hive.serde2.json.HiveJsonWriter;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

@Description(name = "set_value",
        value = "_FUNC_(named_struct(...), key, value))",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(f, \"status.is_delete\", true) FROM src LIMIT 1;\n"
                + "  {\"name\": \"haha\", \"status\": {\"is_delete\": true}")
public class GenericUDFSetValue extends GenericUDF {

    private ObjectInspector sourceInspector;
    private TextConverter keysConverter;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 3, 3);
        checkArgPrimitive(arguments, 1); // KEY
        checkArgPrimitive(arguments, 2); // NEW VALUE
        sourceInspector = arguments[0];

        if (sourceInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("primitive is not support type.");
        }
        keysConverter  = new TextConverter((PrimitiveObjectInspector) arguments[1]);
        return sourceInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object source = arguments[0].get();
        if (source == null) {
            return null;
        }

        String[] keys = keysConverter.convert(arguments[1].get()).toString().split("\\.");
        Object newValue = arguments[2].get();

        switch (sourceInspector.getCategory()) {
            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector)sourceInspector;
                return setValue(soi, source, keys, newValue);
            default:
                throw new HiveException("not support type : " + sourceInspector.getTypeName());
        }
    }

    private Object setValue(StructObjectInspector soi, Object source, String[] keys, Object newValue) {
        List<Object> currentList = (List<Object>)source;
        Iterator<String> keyIterator = Arrays.asList(keys).iterator();

        while(keyIterator.hasNext()) {
            final String k = keyIterator.next();

            final List<? extends StructField> sfs = soi.getAllStructFieldRefs();
            final int size = sfs.size();

            for(int idx = 0; idx<size; idx++) {
                StructField sf = sfs.get(idx);
                if (k.equals(sf.getFieldName())) {

                    // last match
                    if (keyIterator.hasNext()==false) {
                        System.out.println(currentList.get(idx));
                        System.out.println(newValue);
                        currentList.set(idx, newValue);
                        return source;
                    } else {
                        System.out.println("loop -> " + k);
                        currentList = (List<Object>) currentList.get(idx);
                        continue;
                    }
                }
            }
        }
        return source;
    }


    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("update_value", children);
    }
}
