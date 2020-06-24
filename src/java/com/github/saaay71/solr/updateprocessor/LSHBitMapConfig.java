package com.github.saaay71.solr.updateprocessor;

import com.github.saaay71.solr.VectorUtils;
import info.debatty.java.lsh.LSHSuperBit;
import org.apache.solr.schema.SchemaField;

public class LSHBitMapConfig {
    public String fieldName;
    public SchemaField field;
    public SchemaField lshField;
    public SchemaField binaryField;
    public LSHSuperBit superBit;
    public int vecDimensions;
    public VectorUtils.VectorType vecType;
}
