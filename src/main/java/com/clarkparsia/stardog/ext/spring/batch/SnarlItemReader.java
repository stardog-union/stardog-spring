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
package com.clarkparsia.stardog.ext.spring.batch;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.clarkparsia.stardog.ext.spring.DataSource;
import com.clarkparsia.stardog.ext.spring.RowMapper;
import com.clarkparsia.stardog.ext.spring.SnarlTemplate;

/**
 * Implementation of a Spring Batch ItemReader that supports the SNARL API
 * 
 * @author Al Baker
 * @author Clark & Parsia 
 * 
 * @param <T>
 *
 */
public class SnarlItemReader<T> implements ItemReader<T>, InitializingBean, DisposableBean {

	final Logger log = LoggerFactory.getLogger(SnarlItemReader.class);
	
	private SnarlTemplate snarlTemplate;
	
	private DataSource dataSource;
	
	private List<T> results;
	
	private String query;
	
	private RowMapper<T> rowMapper;
	
	
	/* 
	 * Some explanation on this method: in Spring Batch, ItemReader is stateful for the life of
	 * a Batch run, and will be called repeatedly until exhausted and returns null.  There is
	 * an 'ItemStream' interface that provides an open, close, and update method for managing a 
	 * cursor based approach for reading.  However, this includes an execution context since Spring
	 * Batch manages execution state for things like retries and restarts.
	 * 
	 * For the initial implementation - will keep a simple implementation with Stardog where the 
	 * result set is read out and then read out one at a time by Spring Batch.  Therefore, any Spring
	 * Batch retries, or persisting job state to disk, will include the results sets instead of a cursor.
	 * 
	 * The rationale for this is to keep the batch system simple as Stardog moves to 1.0 and keeping 
	 * Stardog connections (ie the cursor) around in Spring Batch will be a post 1.0 refactoring.
	 * 
	 * (non-Javadoc)
	 * @see org.springframework.batch.item.ItemReader#read()
	 */
	@Override
	public T read() throws Exception, UnexpectedInputException, ParseException,
			NonTransientResourceException {
		
		if (results == null) {
			results = snarlTemplate.query(query, rowMapper);
			log.debug("SnarlItemReader finished loading query data");
		}
		
		if (results.size() > 0) { 
			return results.remove(0);
		} else {
			return null;
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		snarlTemplate = new SnarlTemplate();
		snarlTemplate.setDataSource(dataSource);
	}

	@Override
	public void destroy() throws Exception {
		snarlTemplate.setDataSource(null);
		snarlTemplate = null;
	}

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
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the rowMapper
	 */
	public RowMapper<T> getRowMapper() {
		return rowMapper;
	}

	/**
	 * @param rowMapper the rowMapper to set
	 */
	public void setRowMapper(RowMapper<T> rowMapper) {
		this.rowMapper = rowMapper;
	}

}
