package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSetValue;

public class OrcStructBox implements GenericUDFSetValue.StructBox<OrcStruct> {
    final private OrcStruct orcStruct;

    public OrcStructBox(OrcStruct orcStruct) {
        this.orcStruct = orcStruct;
    }
    @Override
    public void set(int i, Object value) {
        orcStruct.setFieldValue(i, value);
    }

    @Override
    public OrcStruct get() {
        return orcStruct;
    }
}
