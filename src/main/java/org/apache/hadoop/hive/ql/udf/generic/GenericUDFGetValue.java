package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

@Description(name = "get_value",
        value = "_FUNC_(named_struct(...), key))",
        extended = "Example:\n"
                + "  > SELECT _FUNC_(f, \"status.is_delete\") FROM src LIMIT 1;\n"
                + "  false")
public class GenericUDFGetValue extends GenericUDF {

    private ObjectInspector sourceInspector;
    private PrimitiveObjectInspectorConverter.TextConverter keysConverter;


    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgsSize(arguments, 2, 2);
        checkArgPrimitive(arguments, 1); // KEY
        sourceInspector = arguments[0];

        if (sourceInspector.getCategory() != STRUCT) {
            throw new UDFArgumentException("struct type only support! " + sourceInspector.getTypeName());
        }
        keysConverter  = new PrimitiveObjectInspectorConverter.TextConverter((PrimitiveObjectInspector) arguments[1]);
        return sourceInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object source = arguments[0].get();
        if (source == null) {
            return null;
        }
        final String[] keys = keysConverter.convert(arguments[1].get()).toString().split("\\.");
        final StructObjectInspector soi = (StructObjectInspector)sourceInspector;
        final Iterator<String> keyIterator = Arrays.asList(keys).iterator();
        return getValue(soi, source, keyIterator);
    }

    private Object getValue(StructObjectInspector soi, Object source, Iterator<String> keyIterator) throws HiveException{
        final List<Object> sourceList = (List<Object>)source;
        final List<? extends StructField> sfs = soi.getAllStructFieldRefs();
        final int size = sfs.size();
        final String k;
        if (keyIterator.hasNext()) {
            k = keyIterator.next();
        } else {
            throw new HiveException("not found key");
        }

        for(int idx = 0; idx<size; idx++) {
            final StructField sf = sfs.get(idx);

            // [SKIP] key match miss
            if (!(k.equals(sf.getFieldName()))) {
                continue;
            }

            // [SUCCESS] key all match
            if (!keyIterator.hasNext()) {
                return sourceList.get(idx);
            }

            // [NEXT] more key
            final ObjectInspector csf = sf.getFieldObjectInspector();
            if (!(csf instanceof StructObjectInspector)) {
                throw new HiveException("children node is not StructObjectInspector Type : " + csf.getClass());
            }
            return getValue((StructObjectInspector)csf, sourceList.get(idx), keyIterator);
        }
        return null;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("get_value", children);
    }
}
