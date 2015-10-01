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
package com.complexible.stardog.ext.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.complexible.common.openrdf.model.Models2;
import com.complexible.stardog.Contexts;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Adder;
import com.complexible.stardog.api.BooleanQuery;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.Getter;
import com.complexible.stardog.api.GraphQuery;
import com.complexible.stardog.api.Remover;
import com.complexible.stardog.api.SelectQuery;
import com.complexible.stardog.api.UpdateQuery;
import com.complexible.stardog.ext.spring.utils.TypeConverter;

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
	 * @param action callback to run
	 * @param <T> type of callback to run
	 * @return generic type
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
	
	/**
	 * <code>remove</code>
	 * @param graphUri - the context of the graph to remove.  If the context is
     * {@link Contexts#DEFAULT}, this will remove the default graph (no context). 
	 */
	public void remove(String graphUri) {
		Connection connection = dataSource.getConnection();
		
		try {
			connection.begin();
			connection.remove().context(ValueFactoryImpl.getInstance().createURI(graphUri));
			connection.commit();
		} catch (StardogException e) {
			log.error("Error removing graph from Stardog", e);
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
	 * @param <T> - generic type for GetterCallback
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
			
			Stream<Statement> iterator = getter.statements();
			
			list = iterator.map( s -> action.processStatement(s) ).collect(Collectors.toCollection(ArrayList::new));

			//iterator.forEach( s -> list.add(action.processStatement s) );

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
	 * @param <T> generic type of AdderCallback
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
	 * @param <T> Generic type of RemoverCallback
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
	
	public <T> List<T> construct(String sparql, GraphMapper<T> mapper) {
		return construct(sparql, null, mapper);
	}
	
	public <T> List<T> construct(String sparql,  Map<String, Object> args, GraphMapper<T> mapper) { 
		Connection connection = dataSource.getConnection();
		GraphQueryResult result = null;
		try { 
			GraphQuery query = connection.graph(sparql);
			
			if (args != null) { 
				for (Entry<String, Object> arg : args.entrySet()) { 					
					query.parameter(arg.getKey(), arg.getValue());
				}
			}
			
			ArrayList<T> list = new ArrayList<T>();
			
			result = query.execute();
			
			// return empty lists for empty queries
			if (result == null) { 
				return list;
			}
			
			while (result.hasNext()) { 
				list.add(mapper.mapRow(result.next()));
			}

			return list;
		} catch (StardogException e) {
			log.error("Error sending construct query to Stardog", e);
			throw new RuntimeException(e);
		} catch (QueryEvaluationException e) {
			log.error("Error evaluating SPARQL construct query", e);
			throw new RuntimeException(e);
		} finally { 
			if (result != null) {
				try {
					result.close();
				}
				catch (QueryEvaluationException e) { }
			}
			dataSource.releaseConnection(connection);
		}
	}
 	
	/**
	 * <code>query</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a list of domain classses
	 * 
	 * @param sparql the SPARQL query to execute
	 * @param mapper implementation of the RowMapper interface
	 * @param <T> generic type of RowMapper
	 * @return List of results from the RowMapper calls
	 */
	public <T> List<T> query(String sparql, RowMapper<T> mapper) {
		return query(sparql, null, mapper);
	}
	
	/**
	 * <code>query</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a list of domain classes
	 * 
	 * @param sparql the SPARQL query to execute
	 * @param args map of string and object to pass bind as input parameters
	 * @param mapper implementation of the RowMapper interface
	 * @param <T> generic type of RowMapper
	 * @return List of results from the RowMapper calls
	 */
	public <T> List<T> query(String sparql, Map<String, Object> args, RowMapper<T> mapper) {
		Connection connection = dataSource.getConnection();
		TupleQueryResult result = null;
		try { 
			SelectQuery query = connection.select(sparql);
			
			if (args != null) { 
				for (Entry<String, Object> arg : args.entrySet()) { 					
					query.parameter(arg.getKey(), arg.getValue());
				}
			}
			
			ArrayList<T> list = new ArrayList<T>();
			
			result = query.execute();
			
			// return empty lists for empty queries
			if (result == null) { 
				return list;
			}
			
			while (result.hasNext()) { 
				list.add(mapper.mapRow(result.next()));
			}

			return list;
		} catch (StardogException e) {
			log.error("Error sending query to Stardog", e);
			throw new RuntimeException(e);
		} catch (QueryEvaluationException e) {
			log.error("Error evaluating SPARQL query", e);
			throw new RuntimeException(e);
		} finally { 
			if (result != null) {
				try {
					result.close();
				}
				catch (QueryEvaluationException e) { }
			}
			dataSource.releaseConnection(connection);
		}
	}

	/**
	 * <code>queryForObject</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a single domain class
	 * 
	 * @param sparql query string
	 * @param mapper rowmapper to use
	 * @param <T> generic type of RowMapper
	 * @return single result of the RowMapper call
	 */
	public <T> T queryForObject(String sparql, RowMapper<T> mapper) {
		return queryForObject(sparql, null, mapper);
	}
	
	/**
	 * <code>queryForObject</code>
	 * Simple query call for a SPARQL Query and a RowMapper to
	 * map the object to a domain class
	 * 
	 * @param sparql the SPARQL Query
	 * @param args map of string and object
	 * @param mapper implementation of RowMapper
	 * @param <T> generic type of RowMapper
	 * @return single result of the RowMapper call
	 */
	public <T> T queryForObject(String sparql, Map<String, Object> args, RowMapper<T> mapper) {
		Connection connection = dataSource.getConnection();
		TupleQueryResult result = null;
		try { 
			SelectQuery query = connection.select(sparql);
			
			if (args != null) { 
				for (Entry<String, Object> arg : args.entrySet()) { 		
					query.parameter(arg.getKey(), arg.getValue());
				}
			}
			
			result = query.execute();
			T returnObject = null;
			// return null; for empty queries
			if (result == null) { 
				return returnObject;
			}
			
			if (result.hasNext()) { 
				returnObject = mapper.mapRow(result.next());
			}

			return returnObject;
		} catch (StardogException e) {
			log.error("Error sending query to Stardog", e);
			throw new RuntimeException(e);
		} catch (QueryEvaluationException e) {
			log.error("Error evaluating SPARQL query", e);
			throw new RuntimeException(e);
		} finally { 
			if (result != null) {
				try {
					result.close();
				}
				catch (QueryEvaluationException e) { }
			}
			dataSource.releaseConnection(connection);
		}
	}

	/**
	 * <code>ask</code>
	 * Simple ask call for a SPARQL Query
	 * 
	 * @param sparql the SPARQL ask query to execute
	 * @param args map of string and object to pass bind as input parameters
	 * @return boolean if the query matches in the database
	 */
	public boolean ask(String sparql, Map<String, Object> args) {
		Connection connection = dataSource.getConnection();
		Boolean result = null;
		try { 
			BooleanQuery query = connection.ask(sparql);
			
			if (args != null) { 
				for (Entry<String, Object> arg : args.entrySet()) { 					
					query.parameter(arg.getKey(), arg.getValue());
				}
			}

			result = query.execute();
		} catch (StardogException e) {
			log.error("Error sending query to Stardog", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
		return result;
	}
	
	/**
	 * <code>ask</code>
	 * Simple ask call for a SPARQL Query
	 * 
	 * @param sparql the SPARQL ask query to execute
	 * @return boolean if the query matches in the database
	 */
	public boolean ask(String sparql) {
		return ask(sparql, null);
	}
	
	/**
	 * <code>update</code>
	 * Simple update call for a SPARQL Update
	 * 
	 * @param sparql the SPARQL update to execute
	 */
	public void update(String sparql) {
		update(sparql, null);
	}
	
	/**
	 * <code>update</code>
	 * Simple update call for a SPARQL Update
	 * 
	 * @param sparql the SPARQL update to execute
	 * @param args map of string and object to pass bind as input parameters
	 *
	 */
	public void update(String sparql, Map<String, Object> args) {
		Connection connection = dataSource.getConnection();
		try { 
			UpdateQuery query = connection.update(sparql);
			
			if (args != null) { 
				for (Entry<String, Object> arg : args.entrySet()) { 					
					query.parameter(arg.getKey(), arg.getValue());
				}
			}

			query.execute();

		} catch (StardogException e) {
			log.error("Error sending query to Stardog", e);
			throw new RuntimeException(e);
		} finally { 
			dataSource.releaseConnection(connection);
		}
	}

	/**
	 * <code>add</code>
	 * 
	 * Adds a Sesame graph to the graph URI
	 * 
	 * Other add methods delegate to this method
	 * 
	 * @param graph Graph to use for the adder
	 * @param graphUri graph URi to use
	 */
	public void add(Graph graph, String graphUri) { 
		Resource context = (graphUri == null ? null : ValueFactoryImpl.getInstance().createURI(graphUri));
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
	 *
	 */
	public void add(String subject, String predicate, String object) { 
		add(Models2.newModel(new StatementImpl(
				new URIImpl(subject.toString()), 
				new URIImpl(predicate.toString()), 
				new LiteralImpl(object.toString()))));
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
		add(Models2.newModel(new StatementImpl(
				new URIImpl(subject.toString()), 
				new URIImpl(predicate.toString()), 
				new URIImpl(object.toString()))));
	}
	
	
}
