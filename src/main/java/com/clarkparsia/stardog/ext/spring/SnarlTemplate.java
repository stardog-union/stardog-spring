/*
* Copyright (c) the original authors
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.clarkparsia.stardog.ext.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.Query;

/**
 * SnarlTemplate
 * 
 * Template design pattern, similar to Spring's jdbcTemplate, jmsTemplate, etc
 * 
 * Moves all boiler plate connection setup, transactions, and resource close outs
 * into the template
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
public class SnarlTemplate {

	final Logger log = LoggerFactory.getLogger(SnarlTemplate.class);

	private DataSource dataSource;

	/**
	 * @return the dataSource
	 */
	public DataSource getDataSource() {
		return dataSource;
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	/**
	 * <code>execute</code>
	 * 
	 * Call back for executing a block of code within the 
	 * context of a connection that has been fully setup,
	 * backed by a connection pool, and uses a tx.
	 * 
	 * TODO: investigate Spring-tx here
	 * 
	 * @param action
	 * @return T - generic type
	 */
	public <T> T execute(ConnectionCallback<T> action) { 
		Connection connection = dataSource.getConnection();
		
		try { 
			connection.begin();
			T t =  action.doWithConnection(connection);
			connection.commit();
			return t;
		} catch (StardogException e) {
			log.error("Error executing ConnectionCallback", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}
	
	/**
	 * <code>query</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a domain class
	 * 
	 * Other methods will overload this for query parameters, limits, etc
	 * 
	 * @param sparql
	 * @param mapper
	 * @return List of results from the RowMapper calls
	 */
	public <T> List<T> query(String sparql, RowMapper<T> mapper) {
		return query(sparql, null, mapper);
	}
	
	/**
	 * <code>query</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a domain class
	 * 
	 * Other methods will overload this for query parameters, limits, etc
	 * 
	 * @param sparql
	 * @param mapper
	 * @return List of results from the RowMapper calls
	 */
	public <T> List<T> query(String sparql, Map<String, String> args, RowMapper<T> mapper) {
		Connection connection = dataSource.getConnection();
		TupleQueryResult result = null;
		try { 
			Query query = connection.query(sparql);
			
			if (args != null) { 
				for (Entry<String, String> arg : args.entrySet()) { 					
					query.parameter(arg.getKey(), arg.getValue());
				}
			}
			
			ArrayList<T> list = new ArrayList<T>();
			
			result = query.executeSelect();
			
			// return empty lists for empty queries
			if (result == null) { 
				return list;
			}
			
			while (result.hasNext()) { 
				list.add(mapper.mapRow(result.next()));
			}
			
			result.close();
			
			return list;
		} catch (StardogException e) {
			log.error("Error sending query to Stardog", e);
			throw new RuntimeException(e);
		} catch (QueryEvaluationException e) {
			log.error("Error evaluating SPARQL query", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}
	
	/**
	 * <code>add</code>
	 * 
	 * Add's a Sesame graph to the graph URI
	 * 
	 * Other add methods delegate to this method
	 * 
	 * @param graph
	 * @param graphUri
	 */
	public void add(Graph graph, String graphUri) { 
		Resource context = null;
		if (graphUri != null) { 
			context = ValueFactoryImpl.getInstance().createURI(graphUri);
		}
		Connection connection = dataSource.getConnection();
		try {
			connection.begin();
			if (context != null) { 
				connection.add().graph(graph, context);
			} else { 
				connection.add().graph(graph);
			}
			connection.commit();
		} catch (StardogException e) {
			log.error("Error adding graph to Stardog", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}
	
	/**
	 * <code>add</code>
	 * @param graph Sesame graph
	 */
	public void add(Graph graph) { 
		add(graph, null);
	}
	
	/**
	 * <code>add</code>
	 * @param subject String subject
	 * @param predicate String predicate
	 * @param object String object - always a plain literal
	 */
	public void add(String subject, String predicate, String object) { 
		Graph graph = new GraphImpl();
		graph.add(new URIImpl(subject), new URIImpl(predicate), new LiteralImpl(object));
		add(graph);
	}
	
	/**
	 * <code>add</code>
	 * @param subject URI subject
	 * @param predicate URI predicate
	 * @param object String object - always a plain literal
	 */
	public void add(java.net.URI subject, java.net.URI predicate, String object) { 
		add(subject.toString(), predicate.toString(), object);
	}
	
	/**
	 * <code>add</code>
	 * @param subject URI subject
	 * @param predicate URI predicate
	 * @param object URI object 
	 */
	public void add(java.net.URI subject, java.net.URI predicate, java.net.URI object) { 
		Graph graph = new GraphImpl();
		graph.add(new URIImpl(subject.toString()), new URIImpl(predicate.toString()), new URIImpl(object.toString()));
		add(graph);
	}
	
	
}
