package com.github.saaay71.solr.updateprocessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LSHConfigMapFactory {

    public static Map<String, LSHBitMapConfig> superBitMap = new HashMap<>();
    public static List<LSHFieldConfig> fieldConfigList = new ArrayList<>();

//    private static class LazyHolder {
//        private static final LSHConfigMapFactory INSTANCE = new LSHConfigMapFactory();
//    }

    private LSHConfigMapFactory() {
    }

//    public static SolrClient getInstance() {
//        return LazyHolder.INSTANCE.solrClientFaces;
//    }

}
