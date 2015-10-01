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
package com.complexible.stardog.ext.spring.utils;

import static org.junit.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;

/**
 * TestTypeConverter
 * 
 * Unit tests for TypeConverter
 * 
 * @author Al Baker
 * @author Clark & Parsia
 */
public class TestTypeConverter {

	/**
	 * Test method for {@link com.complexible.stardog.ext.spring.utils.TypeConverter#asLiteral(java.lang.Object)}.
	 */
	@Test
	public void testAsLiteralObject() {
		// String, URI, Date, Integer
		String a = "stardog";
		URI b = null;
		try {
			b = new URI("http://stardog.com");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Date c = new Date();
		Integer d = new Integer(5);
		
		Value va = TypeConverter.asLiteral(a);
		Value vb = TypeConverter.asLiteral(b);
		Value vc = TypeConverter.asLiteral(c);
		Value vd = TypeConverter.asLiteral(d);
		
		assertNotNull(va);
		assertNotNull(vb);
		assertNotNull(vc);
		assertNotNull(vd);
		assertTrue(va.toString().contains("stardog"));
		assertTrue(vb.toString().contains("\"http://stardog.com\""));
		assertTrue(vc.toString().contains("http://www.w3.org/2001/XMLSchema#dateTime"));
		assertEquals(vd.toString(), "\"5\"^^<http://www.w3.org/2001/XMLSchema#int>");

	}


	/**
	 * Test method for {@link com.complexible.stardog.ext.spring.utils.TypeConverter#asResource(java.net.URI)}.
	 */
	@Test
	public void testAsResource() {
		URI b = null;
		try {
			b = new URI("http://stardog.com");
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Value resource = TypeConverter.asResource(b);
		assertNotNull(resource);
	}

	
}
