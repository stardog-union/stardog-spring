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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Adder;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.Remover;

/**
 * Test cases for the StardogConnectionFactoryBean
 * 
 * Uses test-applicationContext in src/test/resources
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"/test-applicationContext.xml"})
public class TestDataSourceFactory  {

	@Autowired
	DataSource dataSource;
	
	@Autowired
    private ApplicationContext applicationContext;

	@Autowired
	private SnarlTemplate snarlTemplate;
	
	private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errContent = new ByteArrayOutputStream();



	@After
	public void cleanUpStreams() {
	    System.setOut(null);
	    System.setErr(null);
	}
	
	/**
	 * TODO: perhaps not load 10k triples for each JUnit test
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	    System.setOut(new PrintStream(outContent));
	    System.setErr(new PrintStream(errContent));
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		DataImporter importer = new DataImporter();
		importer.setSnarlTemplate(tmp);
		importer.inputFile(RDFFormat.N3, applicationContext.getResource("classpath:sp2b_10k.n3"));
		
	}

	/**
	 * Test method for {@link com.clarkparsia.stardog.ext.spring.DataSourceFactoryBean#getObject()}.
	 * 
	 * Validate retrieving the application context directly and the injected dataSource test 
	 * fixture are both equivalent - i.e. singletons
	 * 
	 */
	@Test
	public void testGetObjectSingleton() {
		assertNotNull(dataSource);
		DataSource newSource = (DataSource) applicationContext.getBean("dataSource");
		assertEquals(dataSource, newSource);
	}

	/**
	 * Test method for validating connection configured correctly
	 */
	@Test
	public void testBasicSnarl() {
		Connection con = dataSource.getConnection();
		assertNotNull(con);
	}
	
	@Test
	public void testSnarlTemplate() { 
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		String sparql = "SELECT ?a ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 5";
		
		List<Map<String,String>> results = tmp.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			} 
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
	@Test
	public void testQueryForObject() throws URISyntaxException {
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		
		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String litA = "hello world";
		
		URI a = new URI(uriA);
		URI b = new URI(uriB);

		tmp.add(uriA, uriB, litA);
	
		String sparql = "SELECT ?a ?b WHERE { ?a  <urn:test:b> ?b }";
		
		Map<String, String> result = tmp.queryForObject(sparql, new RowMapper<Map<String, String>>() {

			@Override
			public Map<String, String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			}
			
		});
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertTrue(result.get("a").equals(uriA));
		assertTrue(result.get("b").equals(litA));
 		
	}
	
	@Test
	public void testQueryWithParams() throws URISyntaxException {
		
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b } LIMIT 5";
		
		HashMap<String, Object> params = new HashMap<String, Object>() {{ 
			put("c", new URIImpl("http://purl.org/dc/elements/1.1/title")); 
		}};
		
		List<Map<String,String>> results = tmp.query(sparql, params, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			} 
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
	@Test
	public void testLegacyQueryWithParams() throws URISyntaxException {
		
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b } LIMIT 5";
		
		HashMap<String, String> params = new HashMap<String, String>() {{ 
			put("c", "test")); 
		}};
		
		List<Map<String,String>> results = tmp.query(sparql, params, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			} 
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
	@Test
	public void testQueryForObjectWithParams() throws URISyntaxException {
		SnarlTemplate tmp = new SnarlTemplate();
		tmp.setDataSource(dataSource);
		
		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String litA = "hello world";
		
		URI a = new URI(uriA);
		URI b = new URI(uriB);

		tmp.add(uriA, uriB, litA);
	
		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b }";
		HashMap<String, Object> params = new HashMap<String, Object>() {{ 
			put("c", new URIImpl("urn:test:b")); 
		}};
		Map<String, String> result = tmp.queryForObject(sparql, params, new RowMapper<Map<String, String>>() {

			@Override
			public Map<String, String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			}
			
		});
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertTrue(result.get("a").equals(uriA));
		assertTrue(result.get("b").equals(litA));
 		
	}
	
	@Test
	public void testAdd() throws URISyntaxException { 
		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String uriC = "urn:test:c";
		String litA = "hello world";
		
		URI a = new URI(uriA);
		URI b = new URI(uriB);
		URI c = new URI(uriC);
		
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(a,b,c);
		
		String sparql = "SELECT ?a ?b WHERE { ?a  <urn:test:b> ?b } LIMIT 5";
		
		List<Map<String,String>> results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.getValue("a").stringValue());
				map.put("b", bindingSet.getValue("b").stringValue());
				return map;
			} 
			
		});
		
		assertEquals(results.size(), 2);
		
	}
	
	@Test
	public void testDoWithGetter() { 
		String uriA = "urn:test:x";
		String uriB = "urn:test:y";
		String uriC = "urn:test:z";
		String litA = "hello world";
		
		
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(uriC, uriB, litA);
		
		List<String> results = snarlTemplate.doWithGetter(uriA, null, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 1);
		
		List<String> results2 = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results2.size(), 2);		
		
	}
	
	// TODO: Re-enable this test after Stardog 0.9.x investigation complete
	// this currently fails on the commit inside the snarlTemplate.remove()
	//@Test
	public void testRemoveConstruct() { 
		String uriA = "urn:test:d";
		String uriB = "urn:test:e";
		String uriC = "urn:test:f";
		String litA = "hello world";
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(uriC, uriB, litA);
		
		String constructQuery = "CONSTRUCT { ?a <urn:test:e> ?c } WHERE { ?a <urn:test:e> ?c } ";
		snarlTemplate.remove(constructQuery);
		
		List<String> results2 = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results2.size(), 0);		
	}
	
	@Test
	public void testRemoveStatement() { 
		String uriA = "urn:test:g";
		String uriB = "urn:test:h";
		String uriC = "urn:test:i";
		String litA = "hello world";
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(uriC, uriB, litA);
		
		snarlTemplate.remove(uriA, uriB, litA, null);
		
		List<String> results2 = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results2.size(), 1);		
		
		snarlTemplate.remove(null, uriB, null, null);
		
		List<String> results = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 0);	
		
	}
	
	@Test
	public void testSingleton() { 
		String uriA = "urn:test:j";
		String uriB = "urn:test:k";
		String uriC = "urn:test:l";
		String litA = "hello world";
		String litB = "a singleton";
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.singleton(uriA, uriB, litB, null);
		
		List<String> results = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 1);	
		assertEquals(results.get(0), "a singleton");
	}
	
	@Test
	public void testDoWithAdder() { 
		
		snarlTemplate.doWithAdder(new AdderCallback<Boolean>() {
			@Override
			public Boolean add(Adder adder) throws StardogException {
				String uriA = "urn:test:t";
				String uriB = "urn:test:u";
				String litA = "hello world";
				String litB = "goodbye";
				
				adder.statement(new URIImpl(uriA), new URIImpl(uriB), new LiteralImpl(litA));
				adder.statement(new URIImpl(uriA), new URIImpl(uriB), new LiteralImpl(litB));
				return true;
			} 		
		});
		
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:u", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 2);	
		
	}
	
	@Test
	public void testDoWithRemover() { 
		String uriA = "urn:test:m";
		String uriB = "urn:test:n";
		String litA = "hello world";
		snarlTemplate.add(uriA, uriB, litA);
		
		snarlTemplate.doWithRemover(new RemoverCallback<Boolean>() {
			@Override
			public Boolean remove(Remover remover) throws StardogException {
				remover.statements(new URIImpl("urn:test:m"), new URIImpl("urn:test:n"), null);
				return true;
			} 
		});
		
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:n", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.getObject().stringValue();
			} 
		});
		
		assertEquals(results.size(), 0);	
		
	}
	
	@Test
	public void testConstruct() { 
		String uriA = "urn:test:o";
		String uriB = "urn:test:p";
		String litA = "hello world";
		snarlTemplate.add(uriA, uriB, litA);
		
		String sparql = "CONSTRUCT { ?a <urn:test:new> ?b } WHERE { ?a <urn:test:p> ?b }";
		List<Map<String,String>>  results = snarlTemplate.construct(sparql, new GraphMapper<Map<String,String>>() {
			@Override
			public Map<String, String> mapRow(Statement next) {
				Map<String,String> map = new HashMap<String,String>();	
				map.put(next.getSubject().stringValue(), next.getObject().stringValue());
				return map;
			} 
		});
		
		assertEquals(results.size(), 1);
		
	}
	
}
