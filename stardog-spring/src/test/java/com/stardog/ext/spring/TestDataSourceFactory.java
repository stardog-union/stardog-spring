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
package com.stardog.ext.spring;

import com.complexible.stardog.Contexts;
import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Adder;
import com.complexible.stardog.api.Connection;
import com.complexible.stardog.api.Getter;
import com.complexible.stardog.api.Remover;
import com.complexible.stardog.api.admin.AdminConnection;
import com.stardog.ext.spring.mapper.SimpleRowMapper;
import com.stardog.ext.spring.mapper.SingleMapper;
import com.google.common.collect.ImmutableSet;
import com.stardog.stark.*;
import com.stardog.stark.io.RDFFormats;
import com.stardog.stark.query.BindingSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import static org.junit.Assert.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

	private static Logger log = LoggerFactory.getLogger(TestDataSourceFactory.class);

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
		assertNotNull(dataSource);
		snarlTemplate = new SnarlTemplate();
		snarlTemplate.setDataSource(dataSource);
		DataImporter importer = new DataImporter();
		importer.setSnarlTemplate(snarlTemplate);
		importer.inputFile(RDFFormats.N3, applicationContext.getResource("classpath:sp2b_10k.n3"));
		
	}

	/**
	 * Test method for {@link com.stardog.ext.spring.DataSourceFactoryBean#getObject()}.
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
		try (Connection con = dataSource.getConnection()) {
			assertNotNull(con);
		}
	}
	
	@Test
	public void testSnarlTemplate() {
		String sparql = "SELECT ?a ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 5";
		
		List<Map<String,String>> results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				map.put("b", bindingSet.value("b").toString());
				return map;
			} 
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}

	@Test
	public void testSimpleRowMapper() {
		String sparql = "SELECT ?a ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 5";
		
		List<Map<String,String>> results = snarlTemplate.query(sparql, new SimpleRowMapper());
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
		
	@Test
	public void testSingleMapper() {
		String sparql = "SELECT ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 1";
		
		String result = snarlTemplate.queryForObject(sparql, new SingleMapper("b"));
		
		assertNotNull(result);
	}
	
	@Test
	public void testNullSingleMapper() {
		String sparql = "SELECT ?b WHERE { ?a  <http://purl.org/dc/elements/1.1/title> ?b } LIMIT 1";
		// unlike previous test, a is not bound, therefore should find null in Sesame API 
		String result = snarlTemplate.queryForObject(sparql, new SingleMapper("a"));
		
		assertNull(result);
	}
	
	@Test
	public void testRemoveGraph() {
		Set<Statement> g2 = ImmutableSet.of(Values.statement(Values.iri("urn:s2"),
				Values.iri("urn:p2"),
				Values.iri("urn:o2")));

		snarlTemplate.add(g2, "http://example.org/aGraph");

		
		String sparql = "SELECT ?a WHERE { GRAPH <http://example.org/aGraph> { ?a ?b ?c } }";
		List<Map<String,String>> results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>()  {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				return map;
			} 
			
		});
		assertEquals(results.size(), 1);

		snarlTemplate.remove("http://example.org/aGraph");

		sparql = "SELECT ?a WHERE { GRAPH <http://example.org/aGraph> { ?a ?b ?c } }";
		results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				return map;
			} 
			
		});
		assertEquals(results.size(), 0);
		
		// Test remove of default graph
		sparql = "SELECT ?a WHERE { ?a ?b ?c } LIMIT 5";
		results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				return map;
			} 
			
		});
		assertEquals(results.size(), 5);

		snarlTemplate.remove(Contexts.DEFAULT.toString());
		
		sparql = "SELECT ?a WHERE { ?a ?b ?c } LIMIT 5";
		results = snarlTemplate.query(sparql, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				return map;
			} 
			
		});
		assertEquals(results.size(), 0);
	}
	
	@Test
	public void testQueryForObject() throws URISyntaxException {

		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String litA = "hello world";

		snarlTemplate.add(uriA, uriB, litA);

		String sparql = "SELECT ?a ?b WHERE { ?a  <urn:test:b> ?b }";

		Map<String, Value> result = snarlTemplate.queryForObject(sparql, new RowMapper<Map<String, Value>>() {

			@Override
			public Map<String, Value> mapRow(BindingSet bindingSet) {
				Map<String, Value> map = new HashMap<String, Value>();
				map.put("a", bindingSet.iri("a").orElse(null));
				map.put("b", bindingSet.literal("b").orElse(null));
				return map;
			}

		});

		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertEquals(result.get("a").toString(), uriA);
		assertEquals(((Literal)result.get("b")).label(), litA);
	}
	
	@Test
	public void testQueryWithParams() throws URISyntaxException {

		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b } LIMIT 5";
		
		Map<String, Object> params = new HashMap<String, Object>() {{ 
			put("c", Values.iri("http://purl.org/dc/elements/1.1/title"));
		}};
		
		List<Map<String,String>> results = snarlTemplate.query(sparql, params, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				map.put("b", bindingSet.value("b").toString());
				return map;
			}
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
	@Test
	public void testLegacyQueryWithParams() throws URISyntaxException {

		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b } LIMIT 5";
		
		Map<String, Object> params = new HashMap<String, Object>() {{ 
			put("c", Values.iri("http://purl.org/dc/elements/1.1/title"));
		}};
		
		List<Map<String,String>> results = snarlTemplate.query(sparql, params, new RowMapper<Map<String,String>>() {

			@Override
			public Map<String,String> mapRow(BindingSet bindingSet) {
				Map<String,String> map = new HashMap<String,String>();
				map.put("a", bindingSet.value("a").toString());
				map.put("b", bindingSet.value("b").toString());
				return map;
			} 
			
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 5);
	}
	
	@Test
	public void testQueryForObjectWithParams() throws URISyntaxException {
		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String litA = "hello world";

		snarlTemplate.add(uriA, uriB, litA);
	
		String sparql = "SELECT ?a ?b WHERE { ?a ?c ?b }";
		Map<String, Object> params = new HashMap<String, Object>() {{
			put("c", Values.iri("urn:test:b"));
		}};
		Map<String, Value> result = snarlTemplate.queryForObject(sparql, params, new RowMapper<Map<String, Value>>() {

			@Override
			public Map<String, Value> mapRow(BindingSet bindingSet) {
				Map<String,Value> map = new HashMap<String,Value>();
				map.put("a", bindingSet.iri("a").orElse(null));
				map.put("b", bindingSet.literal("b").orElse(null));
				return map;
			}

		});
		assertNotNull(result);
		assertEquals(result.size(), 2);
		assertEquals(result.get("a").toString(), uriA);
		assertEquals(((Literal)result.get("b")).label(), litA);
 		
	}
	
	
	@Test
	public void testUpdateWithParams() {

		final String uriA1 = "urn:testUpdate:a1";
		final String uriB = "urn:testUpdate:b";
		final String litC1 = "aloha world";
		snarlTemplate.add(uriA1, uriB, litC1);
		
		final String uriA2 = "urn:testUpdate:a2";
		final String litC2 = "aloha world";
		snarlTemplate.add(uriA2, uriB, litC2);
		
		String sparql = "DELETE { ?a ?b \"aloha world\" } INSERT { ?a ?b \"shalom world\" } WHERE { ?a ?b \"aloha world\" }";
		
		Map<String, Object> params = new HashMap<String, Object>() {{ 
			put("b", Values.iri(uriB));
		}};

		snarlTemplate.update(sparql, params);
		
		sparql = "SELECT ?a WHERE { ?a ?b \"shalom world\" }";

		List<Map<String, Value>> results = snarlTemplate.query(sparql, params, new RowMapper<Map<String, Value>>() {

			@Override
			public Map<String, Value> mapRow(BindingSet bindingSet) {
				Map<String,Value> map = new HashMap<String,Value>();
				map.put("a", bindingSet.iri("a").orElse(null));
				return map;
			}
		});
		
		assertNotNull(results);
		assertEquals(results.size(), 2);
		
		List<String> resultList = new ArrayList<String>();
		for (Map<String, Value> m : results) {
			resultList.add(m.get("a").toString());
		}
		
		assertTrue(resultList.contains(uriA1));
		assertTrue(resultList.contains(uriA2));
	}
	
	@Test
	public void testAsk() {

		final String uriA1 = "urn:testAsk:a1";
		final String uriB = "urn:testAsk:b";
		final String litC1 = "hello world";
		snarlTemplate.add(uriA1, uriB, litC1);
		
		final String uriA2 = "urn:testAsk:a2";
		final String litC2 = "aloha world";
		snarlTemplate.add(uriA2, uriB, litC2);
		
		String sparql = "ASK { ?a ?b \"aloha world\" }";
		
		Map<String, Object> params = new HashMap<String, Object>() {{ 
			put("b", Values.iri(uriB));
		}};
		
		boolean result = snarlTemplate.ask(sparql, params);
		
		assert(result);
		
		sparql = "ASK { ?a <urn:testAsk:b> ?c }";
		
		result = snarlTemplate.ask(sparql);

		assert(result);
		
		sparql = "ASK { ?a <urn:testAsk:c> ?c }";
		
		result = snarlTemplate.ask(sparql);

		assert(!result);
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
				map.put("a", bindingSet.value("a").toString());
				map.put("b", bindingSet.value("b").toString());
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
				return statement.object().toString();
			} 
		});
		
		assertEquals(results.size(), 1);
		
		List<String> results2 = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
			} 
		});
		
		assertEquals(results2.size(), 2);		
		
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
				return statement.object().toString();
			} 
		});
		
		assertEquals(results2.size(), 1);		
		
		snarlTemplate.remove(null, uriB, null, null);
		
		List<String> results = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
			} 
		});
		
		assertEquals(results.size(), 0);	
		
	}
	
	@Test
	public void testSingleton() { 
		String uriA = "urn:test:j";
		String uriB = "urn:test:k";
		String litA = "hello world";
		String litB = "a singleton";
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.singleton(uriA, uriB, litB, null);
		
		List<Value> results = snarlTemplate.doWithGetter(null, uriB, new GetterCallback<Value>() {
			@Override
			public Value processStatement(Statement statement) {
				return statement.object();
			} 
		});
		
		assertEquals(results.size(), 1);	
		assertEquals(((Literal)results.get(0)).label(), "a singleton");
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
				
				adder.statement(Values.iri(uriA), Values.iri(uriB), Values.literal(litA));
				adder.statement(Values.iri(uriA), Values.iri(uriB), Values.literal(litB));
				return true;
			} 		
		});
		
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:u", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
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
				remover.statements(Values.iri("urn:test:m"), Values.iri("urn:test:n"), null);
				return true;
			} 
		});
		
		List<String> results = snarlTemplate.doWithGetter(null, "urn:test:n", new GetterCallback<String>() {
			@Override
			public String processStatement(Statement statement) {
				return statement.object().toString();
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
				map.put(next.subject().toString(), next.object().toString());
				return map;
			} 
		});
		
		assertEquals(results.size(), 1);
		
	}

	@Test
	public void testSnarlGetReasoningConnection() {
		Getter getter = snarlTemplate.reasoning(true);
		assertNotNull(getter);
	}

	@Test
	public void testSetSnarlConnectionReasoning() {
		snarlTemplate.setReasoning(false);
		try (Connection c = snarlTemplate.getDataSource().getConnection()) {
			assertFalse(c.isReasoningEnabled());
		}
		snarlTemplate.setReasoning(true);
		try (Connection c = snarlTemplate.getDataSource().getConnection()) {
			assertTrue(c.isReasoningEnabled());
		}
	}

	@Test
	public void testSnarlGetConnection() {
		Getter getter = snarlTemplate.get();
		assertNotNull(getter);
	}

	@Test
	public void testSnarlGetDataSource() {
		DataSource ds = snarlTemplate.getDataSource();
		assertNotNull(ds);
	}

	@Test
	public void testAddUriUriString() {
		try { 
			URI a = new URI("urn:test:s");
			URI b = new URI("urn:test:t");
			String c = "Hello World";
			snarlTemplate.add(a, b, c);
			
			String sparql = "SELECT ?a ?c WHERE { ?a  <urn:test:t> ?b } LIMIT 1";
			
			List<Map<String,String>> results = snarlTemplate.query(sparql, new SimpleRowMapper());
			
			assertNotNull(results);
			assertEquals(results.size(), 1);
			
		} catch (Exception e) {
			fail("Caught exception");
		}
	}
}

