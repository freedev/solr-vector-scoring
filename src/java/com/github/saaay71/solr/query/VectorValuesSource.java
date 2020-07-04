package com.github.saaay71.solr.query;

import com.github.saaay71.solr.VectorUtils;
import com.github.saaay71.solr.query.score.VectorQueryScorerFactory;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.CustomScoreProvider;
import org.apache.lucene.queries.CustomScoreQuery;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.docvalues.DocTermsIndexDocValues;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.queries.function.valuesource.BytesRefFieldSource;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueStr;
import org.apache.solr.common.SolrException;
import org.apache.solr.legacy.LegacyField;
import org.apache.solr.schema.BinaryField;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.StrField;
import org.apache.solr.uninverting.FieldCache;
import sun.security.rsa.RSAKeyFactory;

import java.io.IOException;
import java.util.*;

public class VectorValuesSource extends DoubleValuesSource {
	private List<Double> vector;
	private SchemaField binaryField;
	private VectorUtils.VectorType vecType;
	private Set<String> FIELDS;
	private String binaryFieldName;

	public VectorValuesSource(List<Double> vector, SchemaField field, SchemaField binaryField) {
//		super(field.getName());
		this.FIELDS = new HashSet<String>(){{add(binaryField.getName());}};
		this.vector = vector;
		this.binaryField = binaryField;
		this.binaryFieldName = this.binaryField.getName();
		this.vecType = VectorUtils.VectorType.valueOf(
				(String)((Map<String, Object>)(field.getArgs())).getOrDefault("vectorType", "AUTO")
		);
	}
//	@Override
//	protected CustomScoreProvider getCustomScoreProvider(LeafReaderContext context) throws IOException {
//		return new CustomScoreProvider(context){
//			@Override
//			public float customScore(int docID, float subQueryScore, float valSrcScore) throws IOException {
//				BytesRef vecBytes = context.reader().document(docID, FIELDS).getBinaryValue(binaryField.getName());
//				if(vecBytes == null) {
//					throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not find vector for docId: \"" + docID + "\"");
//				}
//				VectorUtils.VectorType vecType = VectorUtils.VectorType.valueOf(
//						(String)((Map<String, Object>)(field.getArgs())).getOrDefault("vectorType", "AUTO")
//				);
//
//				return VectorQueryScorerFactory.getScorer(vecType, vecBytes).score(vector, VectorQuery.VectorQueryType.COSINE, vecBytes);
//			}
//		};
//	}

	@Override
	public DoubleValues getValues(LeafReaderContext ctx, DoubleValues doubleValues) throws IOException {

		FieldInfo fieldInfo = ctx.reader().getFieldInfos().fieldInfo(binaryFieldName);
		return new DoubleValues() {

			double val = 0;
			@Override
			public double doubleValue() throws IOException {
				return val;
			}

			@Override
			public boolean advanceExact(int i) throws IOException {
				if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
					final BinaryDocValues binaryValues = DocValues.getBinary(ctx.reader(), binaryFieldName);
					final BytesRef bytesRef = binaryValues.binaryValue();
					if (bytesRef == null)
						return false;
					val = VectorQueryScorerFactory.getScorer(vecType, bytesRef).score(vector, VectorQuery.VectorQueryType.COSINE, bytesRef);
					return true;
				} else {
					final Document document = ctx.reader().document(i, FIELDS);
					final BytesRef bytesRef = document.getBinaryValue(binaryFieldName);
					if (bytesRef == null)
						return false;
					val = VectorQueryScorerFactory.getScorer(vecType, bytesRef).score(vector, VectorQuery.VectorQueryType.COSINE, bytesRef);
					return true;
				}
			}
		};

	}


	@Override
	public boolean needsScores() {
		return false;
	}

	@Override
	public DoubleValuesSource rewrite(IndexSearcher indexSearcher) throws IOException {
		return this;
	}

	@Override
	public int hashCode() {
		return 0;
	}

	@Override
	public boolean equals(Object o) {
		return false;
	}

	@Override
	public String toString() {
		return null;
	}

	@Override
	public boolean isCacheable(LeafReaderContext leafReaderContext) {
		return true;
	}
}