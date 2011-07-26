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

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openrdf.query.BindingSet;
import org.openrdf.rio.RDFFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractTransactionalJUnit4SpringContextTests;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.clarkparsia.stardog.api.Connection;

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
	
	/**
	 * TODO: perhaps not load 10k triples for each JUnit test
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
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
	public void testAdd() throws URISyntaxException { 
		String uriA = "urn:test:a";
		String uriB = "urn:test:b";
		String uriC = "urn:test:c";
		String litA = "a";
		
		URI a = new URI(uriA);
		URI b = new URI(uriB);
		URI c = new URI(uriC);
		
		snarlTemplate.add(uriA, uriB, litA);
		snarlTemplate.add(a,b,c);
		
		
	}
	
}
