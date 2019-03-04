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
package com.complexible.stardog.ext.spring.batch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.stardog.stark.impl.IRIImpl;
import com.stardog.stark.impl.StringLiteral;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import com.stardog.stark.Statement;
import com.stardog.stark.query.BindingSet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Adder;
import com.complexible.stardog.ext.spring.AdderCallback;
import com.complexible.stardog.ext.spring.DataSource;
import com.complexible.stardog.ext.spring.GetterCallback;
import com.complexible.stardog.ext.spring.RowMapper;
import com.complexible.stardog.ext.spring.SnarlTemplate;

/**
 * @author Al Baker
 * @author Clark & Parsia
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"/batch-applicationContext.xml"})
public class TestSpringBatch  {

	@Autowired
	DataSource dataSource;

	@Autowired
	private SnarlTemplate snarlTemplate;
	
	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired
	private Job simpleJob;

	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();

	@After
	public void cleanUpStreams() {
	    System.setOut(null);
	    System.setErr(null);
	}
	
	/**
	 * We'll add 20 triples of the form:
	 *   
	 *   <urn:test:resource> <urn:test:predicate> "lit{0..20}"
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
	    System.setOut(new PrintStream(outContent));
	    System.setErr(new PrintStream(errContent));
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		tmp.doWithAdder(new AdderCallback<Boolean>() {
			@Override
			public Boolean add(Adder adder) throws StardogException {
				String uriA = "urn:test:resource";
				String uriB = "urn:test:predicate";
				
				for (int i = 0; i < 20; i++) {
					adder.statement(new IRIImpl(uriA), new IRIImpl(uriB), new StringLiteral("lit" + i));
				}
				return true;
			} 		
		});
		
	}

	/**
	 * Unit test for SnarlREader
	 * 
	 * Note: The below code is not idiomatic for Stardog Spring Batch developer.  The SnarlItemReader
	 * would be configured in a Spring bean definition file, and then orchestrated by Spring Batch
	 * The below test validates the behavior of the ItemReader interface and integration of the 
	 * SnarlTemplate and DataSource classes
	 * 
	 * Test method for {@link com.complexible.stardog.ext.spring.batch.SnarlItemReader#read()}.
	 */
	@Test
	public void testRead() {
		String sparql = "SELECT ?a ?b WHERE { ?a ?predicate ?b } LIMIT 2";
		SnarlItemReader<String> reader = new SnarlItemReader<String>();
		reader.setDataSource(dataSource);
		reader.setRowMapper(new RowMapper<String>() {
			@Override
			public String mapRow(BindingSet bindingSet) {
				return (bindingSet.iri("a") + ":" + bindingSet.literal("b"));
			} 
		});
		reader.setQuery(sparql);
		
		
		try {
			reader.afterPropertiesSet();
			String resultOne = reader.read();
			String resultTwo = reader.read();
			String resultThree = reader.read();
			assertNotNull(resultOne);
			assertNotNull(resultTwo);
			assertNull(resultThree);
		} catch (UnexpectedInputException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NonTransientResourceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Unit test for SnarlWriter
	 * 
	 * @throws URISyntaxException
	 */
	@Test
	public void testWrite() throws URISyntaxException {
		
		SnarlItemWriter<Object> writer = new SnarlItemWriter<Object>();
		writer.setDataSource(dataSource);
		writer.setCallback(new BatchAdderCallback<Object>() {

			@Override
			public void write(Adder adder, List<? extends Object> items)
					throws StardogException {
				
				String uriA = "urn:test:test";
				String uriB = "urn:test:property";
				for (Object item : items) {
					adder.statement(new IRIImpl(uriA), new IRIImpl(uriB), new StringLiteral((String)item));
				}
			} 
			
			
		});
		

		ArrayList<String> input = new ArrayList<String>();
		input.add("hello");
		input.add("world");
		
		
		try {
			writer.write(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:property", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
			} 
		});
		
		assertEquals(results.size(), 2);	
		
	}
	
	/**
	 * This test provides a functional execution of a full batch run.  There are 20 records added to 
	 * the embedded Stardog database in the Setup method of this test case
	 * 
	 * The batch hooks (TestBatchCallback and TestRowMapper) extract the data, marshal to the TestRecord
	 * bean, and write it back in under a different predicate
	 * 
	 * @throws JobExecutionAlreadyRunningException
	 * @throws JobRestartException
	 * @throws JobInstanceAlreadyCompleteException
	 * @throws JobParametersInvalidException
	 */
	@Test
	public void integrationTest() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {
		
		// Run the batch job
		JobParameters jobParameters = new JobParametersBuilder().addDate("startTime", new Date()).toJobParameters();
		JobExecution jobEx = jobLauncher.run(simpleJob, jobParameters);
		
		// Validate we have created 20 new records with the new predicate
		// this uses the basic functionality in SnarlTemplate
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:propertyUpdate", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
			} 
		});
		
		assertEquals(results.size(), 20);	
		
	}
}
