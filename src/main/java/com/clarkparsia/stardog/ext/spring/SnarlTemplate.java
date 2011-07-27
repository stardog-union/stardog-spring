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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.model.impl.CalendarLiteralImpl;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Adder;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.Getter;
import com.clarkparsia.stardog.api.Query;
import com.clarkparsia.stardog.api.Remover;
import com.clarkparsia.stardog.ext.spring.utils.TypeConverter;
import com.clarkparsia.stardog.util.Iteration;

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
	 * <code>remove</code>
	 * Remove a graph from the store using a CONSTRUCT query
	 * 
	 * @param constructSparql
	 */
	public void remove(String constructSparql) { 
		Connection connection = dataSource.getConnection();
		try {
			connection.begin();
			Query query = connection.query(constructSparql);
			connection.remove().query(query);
			connection.commit();
		} catch (StardogException e) {
			log.error("Error with remove construct query {}", constructSparql, e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
		
	}
	
	public void remove(String subject, String predicate, Object object, String graphUri) { 
		Connection connection = dataSource.getConnection();
		URIImpl subjectResource = null;
		URIImpl predicateResource = null;
		Resource context = null;
		
		if (subject != null) { 
			subjectResource = new URIImpl(subject);
		}
		if (predicate != null) {
			predicateResource = new URIImpl(predicate);
		}
		
		if (graphUri != null) { 
			context = ValueFactoryImpl.getInstance().createURI(graphUri);
		}
		
		Value objectValue = null;
		if (object != null) {
			objectValue = TypeConverter.asLiteral(object);
		}
		
		try {
			connection.begin();
			connection.remove().statements(subjectResource, predicateResource, objectValue, context);
			connection.commit();
		} catch (StardogException e) {
			log.error("Error with remove statement", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}
	
	
	public void singleton(String subject, String predicate, Object object, String graphUri) { 
		Connection connection = dataSource.getConnection();
		
		URIImpl subjectResource = null;
		URIImpl predicateResource = null;
		Resource context = null;
		
		if (subject != null) { 
			subjectResource = new URIImpl(subject);
		}
		if (predicate != null) {
			predicateResource = new URIImpl(predicate);
		}
		
		if (graphUri != null) { 
			context = ValueFactoryImpl.getInstance().createURI(graphUri);
		}
		
		Value objectValue = TypeConverter.asLiteral(object);
		
		try {
			connection.begin();
			connection.remove().statements(subjectResource, predicateResource, null, context);
			connection.add().statement(subjectResource, predicateResource, objectValue, context);
			connection.commit();
		} catch (StardogException e) {
			log.error("Error with remove statement", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}
	
	/**
	 * <code>doWithGetter</code>
	 * @param subject - String representation of a subject URI
	 * @param predicate - String representation of a predicate URI
	 * @param action - callback that will be called
	 * @return - list of return elements
	 */
	public <T> List<T> doWithGetter(String subject, String predicate, GetterCallback<T> action) { 
		
		ArrayList<T> list = new ArrayList<T>();
		if (subject == null && predicate == null) { 
			return list;
		}
		
		Connection connection = dataSource.getConnection();
		Getter getter = null;
		try {
			getter = connection.get();
			
			if (subject != null) { 
				getter.subject(new URIImpl(subject));
			}
			
			if (predicate != null) { 
				getter.predicate(new URIImpl(predicate));
			}
			
			Iteration<Statement, StardogException> iterator = getter.iterator();
			
			while (iterator.hasNext()) { 
				list.add(action.processStatement(iterator.next()));
			}
			
			return list;
		} catch (StardogException e) {
			log.error("Error with getter", e);
			throw new RuntimeException(e);
		} finally { 
			getter = null;
			dataSource.releaseConnection(connection);
		}
		
	}
 	
	/**
	 * <code>doWithAdder</code>
	 * Template's callback interface for working with an Adder, using
	 * a Datasource and transaction safe environment
	 * @param action AdderCallBack, generic type
	 * @return generic type T
	 */
	public <T> T doWithAdder(AdderCallback<T> action) {
		Connection connection = dataSource.getConnection();
		Adder adder = null;
		try {
			connection.begin();
			adder = connection.add();
			T t = action.add(adder);
			connection.commit();
			return t;
		} catch (StardogException e) {
			log.error("Error with adder ", e);
			throw new RuntimeException(e);
		} finally { 
			adder = null;
			dataSource.releaseConnection(connection);
		}
	}
	
	/**
	 * <code>doWithRemover</code>
	 * Template's callback interface for working with a Remover, using
	 * a Datasource and transaction safe environment
	 * @param action RemoverCallback, generic type
	 * @return generic type T
	 */
	public <T> T doWithRemover(RemoverCallback<T> action) {
		Connection connection = dataSource.getConnection();
		Remover remover = null;
		try {
			connection.begin();
			remover = connection.remove();
			T t = action.remove(remover);
			connection.commit();
			return t;
		} catch (StardogException e) {
			log.error("Error with remover ", e);
			throw new RuntimeException(e);
		} finally { 
			remover = null;
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
			context = null;
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
