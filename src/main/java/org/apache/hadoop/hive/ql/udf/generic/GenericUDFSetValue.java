package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.*;

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
        int numFields = arguments.length;
        if (numFields % 2 == 0) {
            throw new UDFArgumentException(
                    "set_value expects an odd number of arguments.");
        }

        // source
        sourceInspector = arguments[0];
        if (sourceInspector.getCategory() != STRUCT) {
            throw new UDFArgumentException("source is not struct type : {category:"
                    + sourceInspector.getCategory() +",typeName: "+sourceInspector.getTypeName()+"}");
        }

        // key, value
        for (int f = 1; f < numFields; f+=2) {
            ObjectInspector ki = arguments[f];
            ObjectInspector vi = arguments[f+1];

            if ( ki.getCategory() != PRIMITIVE || TypeInfoFactory.stringTypeInfo.getTypeName() != ki.getTypeName()) {
                throw new UDFArgumentException("Find Key is not string type : " + ki.getTypeName());
            }
            if ( vi.getCategory() != PRIMITIVE) {
                throw new UDFArgumentException("New Value is not Primitive type : " + vi.getTypeName());
            }
        }

        keysConverter  = new TextConverter(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        return sourceInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        final Object source = arguments[0].get();
        if (source == null) {
            return null;
        }

        final int numFields = arguments.length;
        final Map<String, Object> matchValue = new LinkedHashMap<>();
        for (int f = 1; f < numFields; f+=2) {
            final String findKey = keysConverter.convert(arguments[f].get()).toString();
            final Object newValue = arguments[f + 1].get();
            matchValue.put(findKey.toLowerCase(), newValue);
        }

        try {
            final StructObjectInspector soi = (StructObjectInspector) sourceInspector;
            return convertStructObject(source, soi, null, matchValue);
        } catch (HiveException e) {
            throw e;
        } catch (Exception e) {
            throw new HiveException(e);
        }
    }

    private Object convertStructObject(Object node, ObjectInspector inspector, String parentFiledName,
                                       Map<String, Object> matchValue) throws HiveException {
        if (node == null) {
            return null;
        }
        if (!(inspector instanceof StructObjectInspector)) {
            return node;
        }

        final StructObjectInspector soi = (StructObjectInspector)inspector;
        final List<? extends StructField> sfs = soi.getAllStructFieldRefs();
        final int size = sfs.size();
        final StructBox clone = createStructBox(soi);

        for(int i=0; i<size; i++) {
            final StructField sf = sfs.get(i);
            final ObjectInspector childInspector = sf.getFieldObjectInspector();
            final String fullKey = fullKey(parentFiledName, sf.getFieldName());
            final Object value;

            if (matchValue.containsKey(fullKey.toLowerCase())) { // 맵칭된 값과 동일
                value = matchValue.get(fullKey.toLowerCase());
                if (value != null && sf.getFieldObjectInspector().getCategory() != PRIMITIVE) {
                    throw new HiveException(fullKey + " type miss (support primitive only) : "
                            + sf.getFieldObjectInspector().getTypeName());
                }
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

    private static StructBox createStructBox(StructObjectInspector soi) {
        if (soi instanceof SettableStructObjectInspector) {
            return new SettableStructBox((SettableStructObjectInspector)soi);
        } else {
            return new ListStructBox(soi.getAllStructFieldRefs().size());
        }
    }
    public interface StructBox {
        void set(int i, Object value);
        Object get();
    }

    /***
     * ORC 포맷에서 사용되는 경우 create() 로 생성한 데이터로 세팅한다.
     */
    private static class SettableStructBox implements StructBox {
        private final SettableStructObjectInspector inspector;
        private final Object result;
        private final List<? extends StructField> fields;

        public SettableStructBox(SettableStructObjectInspector inspector) {
            this.inspector = inspector;
            this.result = inspector.create();
            this.fields = inspector.getAllStructFieldRefs();
        }
        @Override
        public void set(int i, Object value) {
            this.inspector.setStructFieldData(result, fields.get(i), value);
        }

        @Override
        public Object get() {
            return result;
        }
    }

    /***
     * 기본적으로 struct 를 list-list 형태로 표현된다고 가장된다.
     */
    private static class ListStructBox implements StructBox {
        private final List<Object> list;
        public ListStructBox(int size) {
            list = Arrays.asList(new Object[size]);
        }

        @Override
        public void set(int i, Object value) {
            list.set(i, value);
        }

        @Override
        public Object get() {
            return list;
        }
    }
}
