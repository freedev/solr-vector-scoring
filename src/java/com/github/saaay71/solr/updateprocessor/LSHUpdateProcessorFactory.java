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
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


public class LSHUpdateProcessorFactory extends UpdateRequestProcessorFactory {

    private static final Logger logger = LoggerFactory.getLogger(LSHUpdateProcessorFactory.class);

    public static final String DEFAULT_VECTOR_TYPE = "AUTO";
    public static final String DEFAULT_FIELD_NAME = "vector";
    public static final String DEFAULT_LSH_FIELD_NAME = "_lsh_hash_";
    public static final String DEFAULT_BINARY_FIELD_NAME = "_vector_";
    private static final Random random = new Random();
    private List<LSHFieldConfig> fieldConfigList = new ArrayList<>();

    private static String getArgString(NamedList args, String fieldName) throws SolrException {
        Object argVal = args.get(fieldName);
        if (argVal == null) {
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, LSHUpdateProcessorFactory.class.getName()
                    + " requires arg: \"" + fieldName + "\" which was not configured");
        }
        return argVal.toString();
    }

    @Override
    public void init(NamedList args) {
        List<NamedList> allLists = (List<NamedList>) args.get("vectors");
        for (NamedList nlst : allLists) {
            LSHFieldConfig config = buildLHSFieldConfig(nlst);
            fieldConfigList.add(config);
            LSHConfigMapFactory.lshFieldConfigMap.put(config.fieldName, config);
        }
    }

    private LSHFieldConfig buildLHSFieldConfig(NamedList nlst) {
        LSHFieldConfig config = new LSHFieldConfig();
        Object argSeed = nlst.get("seed");
        config.seed = argSeed == null ? genRandomSeed() : Long.valueOf(argSeed.toString());
        config.buckets = Integer.valueOf(getArgString(nlst, "buckets"));
        config.stages = Integer.valueOf(getArgString(nlst, "stages"));
        config.dimensions = Integer.valueOf(getArgString(nlst, "dimensions"));
        config.fieldName = (String) Optional.ofNullable(nlst.get("field"))
                                            .orElse(DEFAULT_FIELD_NAME);
        config.binaryFieldName = (String) Optional.ofNullable(nlst.get("binaryField"))
                                                  .orElse(DEFAULT_BINARY_FIELD_NAME);
        config.lshFieldName = (String) Optional.ofNullable(nlst.get("lshField"))
                                               .orElse(DEFAULT_LSH_FIELD_NAME);
        config.vectorType = (String) Optional.ofNullable(nlst.get("vectorType"))
                                             .map(s -> s.toString().toUpperCase())
                                             .orElse(DEFAULT_VECTOR_TYPE);
        return config;
    }

    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
        return new LSHUpdateProcessor(req.getSchema(), fieldConfigList, next);
    }

    private Long genRandomSeed() {
        return random.nextLong();
    }

//    public Long getSeed() {
//        return seed;
//    }
//
//    public Integer getDimensions() {
//        return dimensions;
//    }
//
//    public Integer getBuckets() {
//        return buckets;
//    }
//
//    public Integer getStages() {
//        return stages;
//    }
}

class LSHUpdateProcessor extends UpdateRequestProcessor {

//    private final List<SchemaField> field;
//    private final List<SchemaField> lshField;
//    private final List<SchemaField> binaryField;
//    private final int vecDimensions;
//    private final VectorUtils.VectorType vecType;
    private static final Logger logger = LoggerFactory.getLogger(LSHUpdateProcessor.class);

    public LSHUpdateProcessor(IndexSchema schema, List<LSHFieldConfig> fieldConfigList, UpdateRequestProcessor next) {
        super(next);
        logger.info("created LSHUpdateProcessor");
        for (LSHFieldConfig config : fieldConfigList) {
            LSHBitMapConfig bitMapConfig = new LSHBitMapConfig();
            bitMapConfig.fieldConfig = config;
            bitMapConfig.fieldName = config.fieldName;
            bitMapConfig.field = schema.getField(config.fieldName);
            bitMapConfig.binaryField = schema.getField(config.binaryFieldName);
            bitMapConfig.lshField = schema.getField(config.lshFieldName);
            bitMapConfig.vecType = VectorUtils.VectorType.valueOf(config.vectorType);
            bitMapConfig.vecDimensions = config.dimensions;
            logger.info("created LSHBitMapConfig for field " + config.fieldName);
            LSHConfigMapFactory.bitConfigMapByLSHField.put(config.lshFieldName, bitMapConfig);
            LSHConfigMapFactory.superBitCacheMap.putIfAbsent(config.lshFieldName, new LSHSuperBit(config.stages, config.buckets, config.dimensions, config.seed));
        }
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {
        SolrInputDocument cmdDoc = cmd.getSolrInputDocument();
        Map<String, LSHBitMapConfig> superBitConfigMap = LSHConfigMapFactory.bitConfigMapByLSHField;
        for (Map.Entry e : superBitConfigMap.entrySet()) {
            LSHBitMapConfig config = (LSHBitMapConfig) e.getValue();
            if (cmdDoc.containsKey(config.fieldName)) {
                final Object obj = cmdDoc.getFieldValue(config.fieldName);
                if (obj instanceof String) {
                    final String vectorStr = ((String) obj);
                    cmdDoc.setField(config.binaryField.getName(), VectorUtils.encode(vectorStr, config.vecType).bytes);
                    final LSHSuperBit lshSuperBit = LSHConfigMapFactory.getLSHSuperBitByFieldName(config.fieldName);
                    int[] hashValues = lshSuperBit.hash(VectorUtils.parseInputVec(vectorStr, config.vecDimensions));
                    List<String> hashStringValues = LSHUtils.getLSHStringStream(hashValues)
                                                            .collect(Collectors.toList());
                    cmdDoc.setField(config.lshField.getName(), hashStringValues);
                } else if (obj instanceof LinkedHashMap) {
                    final LinkedHashMap map = (LinkedHashMap) obj;
                    final String vectorStr = (String) map.get("set");
                    cmdDoc.setField(config.binaryField.getName(), VectorUtils.encode(vectorStr, config.vecType).bytes);
                    final LSHSuperBit lshSuperBit = LSHConfigMapFactory.getLSHSuperBitByFieldName(config.fieldName);
                    int[] hashValues = lshSuperBit.hash(VectorUtils.parseInputVec(vectorStr, config.vecDimensions));
                    List<String> hashStringValues = LSHUtils.getLSHStringStream(hashValues)
                                                            .collect(Collectors.toList());
                    cmdDoc.setField(config.lshField.getName(), hashStringValues);
                }
            }
        }
        super.processAdd(cmd);
    }
}

