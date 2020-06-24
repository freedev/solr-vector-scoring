package com.github.saaay71.solr.query;

import com.github.saaay71.solr.VectorUtils;
import com.github.saaay71.solr.updateprocessor.LSHConfigMapFactory;
import com.github.saaay71.solr.updateprocessor.LSHFieldConfig;
import info.debatty.java.lsh.LSHSuperBit;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VectorQParserPlugin extends QParserPlugin {

    public static final Double DEFAULT_RERANK_WEIGHT = 1.0d;
    private static final Logger logger = LoggerFactory.getLogger(VectorQParserPlugin.class);
    private static final Map<String, LSHSuperBit> superBitCache = new HashMap<>();
    private static final Map<String, Double> stagesScoreCache = new HashMap<>();

    @Override
    public QParser createParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
        return new QParser(qstr, localParams, params, req) {
            @Override
            public Query parse() throws SyntaxError {
                String field = localParams.get(QueryParsing.F);
                String vector = localParams.get("vector");
                boolean cosine = localParams.getBool("cosine", true);

                if (field == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'f' not specified");
                }

                if (vector == null) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "vector missing");
                }

                FieldType ft = req.getCore().getLatestSchema().getFieldType(field);
                String subQueryStr = localParams.get(QueryParsing.V);

                String[] vectorArray = vector.split(",");

                if (ft != null && !localParams.getBool("lsh", false)) {
                    VectorQuery q = new VectorQuery(subQuery(subQueryStr, null).getQuery());
                    q.setQueryString(localParams.toLocalParamsString());
                    query = q;
                } else {

                    final int topNDocs = localParams
                            .getInt(ReRankQParserPlugin.RERANK_DOCS, ReRankQParserPlugin.RERANK_DOCS_DEFAULT);
                    List<LSHFieldConfig> fieldConfigList = LSHConfigMapFactory.fieldConfigList;
                    final LSHFieldConfig config = fieldConfigList.stream()
                                                                 .filter(l -> l.fieldName.endsWith(field))
                                                                 .findFirst()
                                                                 .orElseThrow(() -> new SolrException(SolrException.ErrorCode.BAD_REQUEST, field + " not exists"));
                    String lshQuery = computeLSHQueryString(config, vector, vectorArray);
                    if (subQueryStr != null && !subQueryStr.equals("")) {
                        lshQuery = subQuery(subQueryStr, null).getQuery() + " AND " + lshQuery;
                    }
                    Query luceneQuery = req.getCore().getQueryPlugin("lucene")
                                           .createParser(lshQuery, localParams, params, req).parse();

                    // do not run cosine similarity
                    if (topNDocs == 0) {
                        return luceneQuery;
                    }
                    final double reRankWeight = localParams
                            .getDouble(ReRankQParserPlugin.RERANK_WEIGHT, DEFAULT_RERANK_WEIGHT);
                    SolrParams computedLocalParams = new ModifiableSolrParams(localParams)
                            .set(ReRankQParserPlugin.RERANK_QUERY, "{!vp f=" + field + " vector=\"" + vector + "\" lsh=\"false\"}")
                            .setNonNull(ReRankQParserPlugin.RERANK_WEIGHT, reRankWeight)
                            .set("q", lshQuery);
                    return ((AbstractReRankQuery) req.getCore().getQueryPlugin(ReRankQParserPlugin.NAME)
                                                     .createParser(lshQuery, computedLocalParams, params, req)
                                                     .getQuery()).wrap(luceneQuery);
                }

                List<Double> vectorList = new ArrayList<>();
                for (int i = 0; i < vectorArray.length; i++) {
                    double v = Double.parseDouble(vectorArray[i]);
                    vectorList.add(v);
                }

                return new VectorScoreQuery(query, vectorList, req.getSchema().getField(field), cosine);
            }

            private String computeLSHQueryString(LSHFieldConfig config, String vector, String[] vectorArray) {
                LSHSuperBit superBit = superBitCache.computeIfAbsent(config.fieldName, (k) -> {
                    stagesScoreCache.put(config.fieldName, 1d / (double) config.stages);
                    return new LSHSuperBit(config.stages, config.buckets, config.dimensions, config.seed);
                });
                int[] intHash = superBit.hash(VectorUtils.parseInputVec(vector, vectorArray.length));
                final Double stagesScorePercentage = stagesScoreCache.get(config.fieldName);
                Stream<String> queryStringStream = LSHUtils.getLSHStringStream(intHash)
                                                           .map(x -> String
                                                                   .format("(%s:\"%s\")^=%s", config.lshFieldName, x, stagesScorePercentage));
                return queryStringStream.collect(Collectors.joining(" OR "));
            }
        };
    }

}
