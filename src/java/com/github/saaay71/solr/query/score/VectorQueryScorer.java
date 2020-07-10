package com.github.saaay71.solr.query.score;

import com.github.saaay71.solr.query.VectorQueryType;
import org.apache.lucene.util.BytesRef;

import java.util.List;

@FunctionalInterface
public interface VectorQueryScorer {
    Float score(List<Double> inputVec, VectorQueryType vQType, BytesRef buffer);
}
