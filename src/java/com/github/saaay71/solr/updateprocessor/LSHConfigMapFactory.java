package com.github.saaay71.solr.updateprocessor;

import info.debatty.java.lsh.LSHSuperBit;
import org.apache.solr.common.SolrException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class LSHConfigMapFactory {

    public static ConcurrentMap<String, LSHSuperBit> superBitCacheMap = new ConcurrentHashMap<>();
    public static ConcurrentMap<String, LSHFieldConfig> lshFieldConfigMap = new ConcurrentHashMap<>();
    public static ConcurrentMap<String, LSHBitMapConfig> bitConfigMapByLSHField = new ConcurrentHashMap<>();

    private LSHConfigMapFactory() {
    }

//    private static class LazyHolder {
//        private static final LSHConfigMapFactory INSTANCE = new LSHConfigMapFactory();
//    }

    public static LSHSuperBit getLSHSuperBitByFieldName(String fieldName) {
        return LSHConfigMapFactory.superBitCacheMap.computeIfAbsent(fieldName, (f) -> {
            final LSHFieldConfig config = lshFieldConfigMap.get(f);
            if (config == null) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, String
                        .format("'%s' not specified in configuration", fieldName));
            }
            return new LSHSuperBit(config.stages, config.buckets, config.dimensions, config.seed);
        });
    }

//    public static SolrClient getInstance() {
//        return LazyHolder.INSTANCE.solrClientFaces;
//    }

}
