package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

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

        if (sourceInspector.getCategory() != STRUCT) {
            throw new UDFArgumentException("struct type only support! " + sourceInspector.getTypeName());
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
        Object cloneSource = deepCopyList((List<Object>)source);
        StructObjectInspector soi = (StructObjectInspector)sourceInspector;
        Iterator<String> keyIterator = Arrays.asList(keys).iterator();
        return setValue(soi, cloneSource, keyIterator, newValue);
    }



    private Object setValue(StructObjectInspector soi, Object source, Iterator<String> keyIterator, Object newValue) throws HiveException{
        final List<Object> sourceList = (List<Object>)source;
        final List<? extends StructField> sfs = soi.getAllStructFieldRefs();
        final int size = sfs.size();
        final String k;
        if (keyIterator.hasNext()) {
            k = keyIterator.next();
        } else {
            throw new HiveException("not found key ");
        }

        for(int idx = 0; idx<size; idx++) {
            final StructField sf = sfs.get(idx);

            // [SKIP] key match miss
            if (!(k.equals(sf.getFieldName()))) {
                continue;
            }

            // [SUCCESS] key all match
            if (!keyIterator.hasNext()) {
                sourceList.set(idx, newValue);
                break; // OK
            }

            // [NEXT] more key
            final ObjectInspector csf = sf.getFieldObjectInspector();
            final Object child = sourceList.get(idx);
            if (child == null) {
                sourceList.set(idx, null); // null 이라면
                break; // NULL BREAK
            }
            if (!(csf instanceof StructObjectInspector)) {
                throw new HiveException("children node is not StructObjectInspector Type : " + csf.getClass());
            }
            sourceList.set(idx, setValue((StructObjectInspector)csf, child, keyIterator, newValue));
        }
        return source;
    }

    private List<Object> deepCopyList(List<Object> node) {
        if (node == null) {
            return null;
        }
        final int size = node.size();
        final List<Object> clone = new ArrayList<>(size);
        for(int i=0; i<size; i++) {
            Object value = node.get(i);
            if (value == null) {
                clone.add(null);
            } else if (value instanceof List) {
                clone.add(deepCopyList((List<Object>)value));
            } else {
                clone.add(value);
            }
        }
        return clone;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("set_value", children);
    }
}
