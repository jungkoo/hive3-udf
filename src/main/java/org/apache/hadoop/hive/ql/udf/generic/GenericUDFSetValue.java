package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.io.orc.OrcRecordUpdater;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.OrcStructBox;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;

import java.util.*;

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
            throw new UDFArgumentException("source is not struct type : {category:"
                    + sourceInspector.getCategory() +",typeName: "+sourceInspector.getTypeName()+"}");
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

        final String findKey = keysConverter.convert(arguments[1].get()).toString();
        final Object newValue = arguments[2].get();
        final Map<String, Object> matchValue = new LinkedHashMap<>();
        matchValue.put(findKey, newValue);

        final StructObjectInspector soi = (StructObjectInspector) sourceInspector;
        return convertStructObject(source, soi, null, matchValue);
    }

    private Object convertStructObject(Object node, ObjectInspector inspector, String parentFiledName,
                                       Map<String, Object> matchValue)  {
        if (node == null) {
            return null;
        }
        if (!(inspector instanceof StructObjectInspector)) {
            return node;
        }

        final StructObjectInspector soi = (StructObjectInspector)inspector;
        final List<? extends StructField> sfs = soi.getAllStructFieldRefs();
        final int size = sfs.size();


        final StructBox clone;
        if (node instanceof OrcStruct) {
            clone = new OrcStructBox(size);
        } else {
            clone = new ListStructBox(size);
        }

        for(int i=0; i<size; i++) {
            final StructField sf = sfs.get(i);
            final ObjectInspector childInspector = sf.getFieldObjectInspector();
            final String fullKey = fullKey(parentFiledName, sf.getFieldName());
            final Object value;

            if (matchValue.containsKey(fullKey)) { // 맵칭된 값과 동일
                value = matchValue.get(fullKey);
            } else {
                value = soi.getStructFieldData(node, sf);
            }
            clone.set(i, convertStructObject(value, childInspector, fullKey, matchValue));
        }
        return clone.get();
    }

    private String fullKey(String parentFiledName, String fieldName) {
        if (parentFiledName == null || parentFiledName.isEmpty()) {
            return fieldName;
        }
        return String.format("%s.%s", parentFiledName, fieldName);
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("set_value", children);
    }

    public interface StructBox<T> {
        void set(int i, Object value);
        T get();
    }

    private class ListStructBox implements StructBox<List<Object>> {
        private final List<Object> list;
        public ListStructBox(int size) {
            list = Arrays.asList(new Object[size]);
        }

        @Override
        public void set(int i, Object value) {
            list.set(i, value);
        }

        @Override
        public List<Object> get() {
            return list;
        }
    }
}
