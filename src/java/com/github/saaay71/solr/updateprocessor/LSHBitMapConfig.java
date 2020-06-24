package com.github.saaay71.solr.updateprocessor;

import com.github.saaay71.solr.VectorUtils;
import org.apache.solr.schema.SchemaField;

public class LSHBitMapConfig {
    public SchemaField field;
    public SchemaField lshField;
    public SchemaField binaryField;
    public int vecDimensions;
    public VectorUtils.VectorType vecType;
    public LSHFieldConfig fieldConfig;
}
