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
package com.stardog.ext.spring.utils;

import com.stardog.stark.Value;
import com.stardog.stark.Values;

import java.net.URI;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;


/**
 * TypeConverter
 * 
 * Conversion methods from standard JDK types (java.net.URI, java.util.Date, etc) to
 * OpenRDF types (URI, Value, etc)
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
public class TypeConverter {

	public static Value asLiteral(Object o) {
		if (o instanceof String) {
			return asLiteral((String)o);
		} else if (o instanceof URI) {
			return asLiteral((URI) o);
		} else if (o instanceof Date) {
			return asLiteral((Date)o);
		} else if (o instanceof Integer) {
			return asLiteral((Integer) o);
		} else {
			return null;
		}
	}
	
	public static Value asLiteral(Date date) { 
		GregorianCalendar c = new GregorianCalendar();
		c.setTime((Date)date);
		XMLGregorianCalendar date2 = null;
		try {
			date2 = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
		} catch (DatatypeConfigurationException e) {
			throw new RuntimeException(e);
		}
		return Values.literal(c);

	}
	
	public static Value asResource(java.net.URI uri) {
		return Values.literal(uri.toString());
	}
	
	public static Value asLiteral(java.net.URI uri) {
		return Values.literal(((java.net.URI)uri).toString());
	}
	
	public static Value asLiteral(Integer i) {
		return Values.literal(Integer.valueOf(i));
	}
	
	public static Value asLiteral(String s) { 
		return Values.literal(s);
	}
	
}
