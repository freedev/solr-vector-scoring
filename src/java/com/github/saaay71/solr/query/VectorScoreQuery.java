package com.github.saaay71.solr.query;

import java.io.IOException;
import java.util.*;

import com.github.saaay71.solr.VectorUtils;
import com.github.saaay71.solr.query.score.VectorQueryScorerFactory;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.SchemaField;

public class VectorScoreQuery extends CustomScoreQuery {
	private Set<String> FIELDS;
	private List<Double> vector;
	private SchemaField field;
	private SchemaField binaryField;
	boolean cosine;

	public VectorScoreQuery(Query subQuery, List<Double> vector, SchemaField field, SchemaField binaryField, boolean cosine) {
		super(subQuery);
		this.field = field;
		this.binaryField = binaryField;
		this.FIELDS = new HashSet<String>(){{add(binaryField.getName());}};
		this.cosine = cosine;
		this.vector = vector;
	}
	@Override
	protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
		return new CustomScoreProvider(context){
			@Override
			public float customScore(int docID, float subQueryScore, float valSrcScore) throws IOException {
				BytesRef vecBytes = context.reader().document(docID, FIELDS).getBinaryValue(binaryField.getName());
				if(vecBytes == null) {
					throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not find vector for docId: \"" + docID + "\"");
				}
				VectorUtils.VectorType vecType = VectorUtils.VectorType.valueOf(
						(String)((Map<String, Object>)(field.getArgs())).getOrDefault("vectorType", "AUTO")
				);

				return VectorQueryScorerFactory.getScorer(vecType, vecBytes).score(vector, VectorQuery.VectorQueryType.COSINE, vecBytes);
			}
		};
	}
}