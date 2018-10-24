package com.github.saaay71.solr.query;

import com.github.saaay71.solr.VectorUtils;
import com.github.saaay71.solr.updateprocessor.LSHUpdateProcessorFactory;
import info.debatty.java.lsh.LSHSuperBit;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.InitParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.*;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VectorQParserPlugin extends QParserPlugin {

	public static final Double DEFAULT_RERANK_WEIGHT = 1.0d;
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
				VectorQuery q = null;

				String[] vectorArray = vector.split(",");

				if(ft != null && !localParams.getBool("lsh", false)) {
					q = new VectorQuery(subQuery(subQueryStr, null).getQuery());
					q.setQueryString(localParams.toLocalParamsString());
					query = q;
				} else {
					final int topNDocs = localParams.getInt(ReRankQParserPlugin.RERANK_DOCS, ReRankQParserPlugin.RERANK_DOCS_DEFAULT);
					String lshQuery = computeLSHQueryString(vector, vectorArray);
					if(subQueryStr != null && !subQueryStr.equals("")) {
						lshQuery = subQuery(subQueryStr, null).getQuery() + " AND " + lshQuery;
					}
					Query luceneQuery = req.getCore().getQueryPlugin("lucene")
							.createParser(lshQuery, localParams, params, req).parse();

					// do not run cosine similarity
					if(topNDocs == 0) {
						return luceneQuery;
					}
					final double reRankWeight = localParams.getDouble(ReRankQParserPlugin.RERANK_WEIGHT, DEFAULT_RERANK_WEIGHT);
					SolrParams computedLocalParams = new ModifiableSolrParams(localParams)
							.set(ReRankQParserPlugin.RERANK_QUERY, "{!vp f=" + field + " vector=\"" +vector + "\" lsh=\"false\"}")
							.setNonNull(ReRankQParserPlugin.RERANK_WEIGHT, reRankWeight)
							.set("q", lshQuery);
					return ((AbstractReRankQuery) req.getCore().getQueryPlugin(ReRankQParserPlugin.NAME)
							.createParser(lshQuery, computedLocalParams, params, req).getQuery()).wrap(luceneQuery);
				}

				List<Double> vectorList = new ArrayList<>();
				for(int i=0;i<vectorArray.length;i++){
					double v = Double.parseDouble(vectorArray[i]);
					vectorList.add(v);
				}

				return new VectorScoreQuery(query, vectorList, req.getSchema().getField(field), cosine);
			}

			private String computeLSHQueryString(String vector, String[] vectorArray) {
				String reqChain = getUpdateChainName(req);
				LSHSuperBit superBit = superBitCache.computeIfAbsent(reqChain, (k) -> {
					LSHUpdateProcessorFactory lshFactory = getLSHProcessorFromChain(req.getCore(), reqChain);
					stagesScoreCache.put(reqChain, 1d / (double)lshFactory.getStages());
					return new LSHSuperBit(lshFactory.getStages(), lshFactory.getBuckets(), lshFactory.getDimensions(), lshFactory.getSeed());
				});
				int[] intHash = superBit.hash(VectorUtils.parseInputVec(vector, vectorArray.length));
				final Double stagesScorePercentage = stagesScoreCache.get(reqChain);
				Stream<String> queryStringStream = LSHUtils.getLSHStringStream(intHash)
						.map(x -> "("+ LSHUpdateProcessorFactory.DEFAULT_LSH_FIELD_NAME + ":\"" + x + "\")^=" + stagesScorePercentage);
				return queryStringStream.collect(Collectors.joining(" OR "));
			}
		};
	}

	private static LSHUpdateProcessorFactory getLSHProcessorFromChain(SolrCore core, String reqChainName) {
		UpdateRequestProcessorChain chain = core.getUpdateProcessingChain(reqChainName);
		if(chain == null) {
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not find processing chain with \"" + LSHUpdateProcessorFactory.class.getName() +
					"\", try setting a default chain or suppling one in the request params");
		}
		List<UpdateRequestProcessorFactory> LSHProcessors = chain.getProcessors().stream().filter(x -> x instanceof LSHUpdateProcessorFactory).collect(Collectors.toList());
		if(LSHProcessors.size() == 0 || LSHProcessors.size() > 1) {
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "found " + LSHProcessors.size() +
					" LSH Update Processors in update chain: \"" + reqChainName + "\" while only 1 is supported");
		}
		return (LSHUpdateProcessorFactory) LSHProcessors.get(0);
	}

	private static String getUpdateChainName(SolrQueryRequest req) {
		String chainName = req.getParams().get("update.chain");
		if(chainName != null) {
			return chainName;
		}
		// try get the default
		try {
			return ((NamedList) req.getCore().getSolrConfig().getPluginInfo(InitParams.class.getName()).initArgs.get("defaults")).get("update.chain").toString();
		} catch (NullPointerException e) {
			throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
					"Could not determine request chain name, try setting one using the update.chain request param");
		}
	}
}
