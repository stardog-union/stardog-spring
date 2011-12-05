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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFFormat;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Adder;
import com.clarkparsia.stardog.ext.spring.DataImporter;
import com.clarkparsia.stardog.ext.spring.DataSource;
import com.clarkparsia.stardog.ext.spring.GetterCallback;
import com.clarkparsia.stardog.ext.spring.RowMapper;
import com.clarkparsia.stardog.ext.spring.SnarlTemplate;

/**
 * @author Al Baker
 * @author Clark & Parsia
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"/test-applicationContext.xml"})
public class TestSpringBatch  {

	@Autowired
	DataSource dataSource;
	
	@Autowired
    private ApplicationContext applicationContext;

	@Autowired
	private SnarlTemplate snarlTemplate;
	

	@Before
	public void setUp() throws Exception {
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		DataImporter importer = new DataImporter();
		importer.setSnarlTemplate(tmp);
		importer.inputFile(RDFFormat.N3, applicationContext.getResource("classpath:sp2b_10k.n3"));
		
	}

	/**
	 * Note: The below code is not idiomatic for Stardog Spring Batch developer.  The SnarlItemReader
	 * would be configured in a Spring bean definition file, and then orchestrated by Spring Batch
	 * The below test validates the behavior of the ItemReader interface and integration of the 
	 * SnarlTemplate and DataSource classes
	 * 
	 * Test method for {@link com.clarkparsia.stardog.ext.spring.batch.SnarlItemReader#read()}.
	 */
	@Test
	public void testRead() {
		String sparql = "SELECT ?a ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 2";
		SnarlItemReader<String> reader = new SnarlItemReader<String>();
		reader.setDataSource(dataSource);
		reader.setRowMapper(new RowMapper<String>() {
			@Override
			public String mapRow(BindingSet bindingSet) {
				return (bindingSet.getValue("a").stringValue() + ":" + bindingSet.getValue("b").stringValue());
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
					adder.statement(new URIImpl(uriA), new URIImpl(uriB), new LiteralImpl((String)item));
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
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 2);	
		
	}
}
