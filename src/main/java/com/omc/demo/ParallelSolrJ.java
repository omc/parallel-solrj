package com.omc.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelSolrJ {
	private static final int TIMEOUT = 1000;
	public static final Logger log = LoggerFactory
			.getLogger(ParallelSolrJ.class);

	static class QueryCallback extends StreamingResponseCallback {
		private String queryString;

		public QueryCallback(String queryString) {
			this.queryString = queryString;
		}

		@Override
		public void streamSolrDocument(SolrDocument doc) {
			log.info(queryString + " matches " + doc);
		}

		@Override
		public void streamDocListInfo(long numFound, long start, Float maxScore) {
			log.info(numFound + " docs matching " + queryString);
		}
	};

	public static void main(String[] args) throws Exception {

		if (args.length == 0) {
			System.out.println("Expected arguments: SOLR_URL QUERY [QUERY2]");
			System.exit(1);
		}

		String url = args[0];
		final SolrServer server = new HttpSolrServer(url);

		List<String> queries = Arrays.asList(args).subList(1, args.length);
		List<Callable<QueryResponse>> tasks = new ArrayList<Callable<QueryResponse>>();
		for (final String queryString : queries) {
			tasks.add(new Callable<QueryResponse>() {
				public QueryResponse call() throws Exception {
					SolrQuery query = new SolrQuery();
					query.setQuery(queryString);
					return server.queryAndStreamResponse(query,
							new QueryCallback(queryString));
				}
			});
		}

		ExecutorService executor = Executors.newCachedThreadPool();
		executor.invokeAll(tasks, TIMEOUT, TimeUnit.MILLISECONDS);
		executor.shutdown();
	}
}
