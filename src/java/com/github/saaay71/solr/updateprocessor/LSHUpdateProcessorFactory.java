package com.github.saaay71.solr.updateprocessor;

import com.github.saaay71.solr.VectorUtils;
import com.github.saaay71.solr.query.LSHUtils;
import info.debatty.java.lsh.LSHSuperBit;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class LSHUpdateProcessorFactory extends UpdateRequestProcessorFactory {

    private static final Random random = new Random();
    public static final String DEFAULT_FIELD_NAME = "vector";
    public static final String DEFAULT_LSH_FIELD_NAME = "_lsh_hash_";
    public static final String DEFAULT_BINARY_FIELD_NAME = "_vector_";

    private String fieldName;
    private String binaryFieldName;
    private String lshField;
    private Long seed;
    private Integer buckets;
    private Integer stages;
    private Integer dimensions;

    @Override
    public void init( NamedList args )
    {
        Object argSeed = args.get("seed");
        seed = argSeed==null? genRandomSeed(): Long.valueOf(argSeed.toString());
        buckets = Integer.valueOf(getArgString(args, "buckets"));
        stages = Integer.valueOf(getArgString(args, "stages"));
        dimensions = Integer.valueOf(getArgString(args, "dimensions"));
        fieldName = (String)Optional.ofNullable(args.get("field")).orElse(DEFAULT_FIELD_NAME);
        binaryFieldName = (String)Optional.ofNullable(args.get("binaryField")).orElse(DEFAULT_BINARY_FIELD_NAME);
        lshField = (String)Optional.ofNullable(args.get("lshField")).orElse(DEFAULT_LSH_FIELD_NAME);
    }

    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new LSHUpdateProcessor(req.getSchema(), fieldName, binaryFieldName, lshField, stages, buckets, dimensions, seed, next);
    }

    private Long genRandomSeed() {
        return random.nextLong();
    }

    private static String getArgString(NamedList args, String fieldName) throws SolrException {
        Object argVal = args.get(fieldName);
        if(argVal == null) {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, LSHUpdateProcessorFactory.class.getName()
                    + " requires arg: \"" + fieldName + "\" which was not configured");
        }
        return argVal.toString();
    }

    public Long getSeed() {
        return seed;
    }

    public Integer getDimensions() {
        return dimensions;
    }

    public Integer getBuckets() {
        return buckets;
    }

    public Integer getStages() {
        return stages;
    }
}

class LSHUpdateProcessor extends UpdateRequestProcessor {

    private final LSHSuperBit superBit;
    private final SchemaField field;
    private final SchemaField lshField;
    private final SchemaField binaryField;
    private final int vecDimensions;
    private final VectorUtils.VectorType vecType;

    public LSHUpdateProcessor(IndexSchema schema, String fieldName, String binaryFieldName, String lshFieldName, int stages, int buckets, int dimensions, Long seed, UpdateRequestProcessor next) {
        super(next);
        superBit = new LSHSuperBit(stages, buckets, dimensions, seed);
        field = schema.getField(fieldName);
        binaryField = schema.getField(binaryFieldName);
        lshField = schema.getField(lshFieldName);
        final String vecTypeArg = ((Map<String, Object>) field.getArgs()).getOrDefault("vectorType","AUTO").toString().toUpperCase();
        vecType = VectorUtils.VectorType.valueOf(vecTypeArg);
        vecDimensions = dimensions;
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument cmdDoc = cmd.getSolrInputDocument();
        if(cmdDoc.containsKey(field.getName())) {
            final Object obj = cmdDoc.getFieldValue(field.getName());
            if (obj instanceof String) {
                final String vectorStr = (String) obj;
                cmdDoc.setField(binaryField.getName(), VectorUtils.encode(vectorStr, vecType).bytes);
                int[] hashValues = superBit.hash(VectorUtils.parseInputVec(vectorStr, vecDimensions));
                List<String> hashStringValues = LSHUtils.getLSHStringStream(hashValues).collect(Collectors.toList());
                cmdDoc.setField(lshField.getName(), hashStringValues);
            } else if (obj instanceof LinkedHashMap) {
                final LinkedHashMap map = (LinkedHashMap) obj;
                final String vectorStr = (String) map.get("set");
                cmdDoc.setField(binaryField.getName(), VectorUtils.encode(vectorStr, vecType).bytes);
                int[] hashValues = superBit.hash(VectorUtils.parseInputVec(vectorStr, vecDimensions));
                List<String> hashStringValues = LSHUtils.getLSHStringStream(hashValues).collect(Collectors.toList());
                cmdDoc.setField(lshField.getName(), hashStringValues);
            }
        }
        super.processAdd(cmd);
    }
}

