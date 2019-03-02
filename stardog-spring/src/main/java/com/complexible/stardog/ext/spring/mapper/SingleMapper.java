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
package com.complexible.stardog.ext.spring.mapper;

import com.stardog.stark.Value;
import com.stardog.stark.query.BindingSet;

import com.complexible.stardog.ext.spring.RowMapper;

/**
 * SingleMapper
 * 
 * Returns a simple parameter value out of a BindingSet
 * Used for simplified callbacks in SnarlTemplate
 * 
 * @author Al Baker
 * @author Clark and Parsia
 * 
 *
 */
public class SingleMapper implements RowMapper<String> {

	private String var;
	
	/**
	 * <code>SingleMapper</code>
	 * Constructor - ideal for inline instantiation in snarlTemplate queyrForObject
	 * 
	 * @param name constructor argument for the name of the query parameter to return
	 */
	public SingleMapper(String name) {
		var = name;
	}
	
	@Override
	public String mapRow(BindingSet bindingSet) {
		Value v =  bindingSet.get(var);
		if (v == null) {
			return null;
		} else { 
			return v.toString();
		}
	}

}
