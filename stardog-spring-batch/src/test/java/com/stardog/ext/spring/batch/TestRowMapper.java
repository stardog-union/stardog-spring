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
package com.stardog.ext.spring.batch;

import com.stardog.stark.query.BindingSet;

import com.stardog.ext.spring.RowMapper;

/**
 * Simple rowMapper function to map records in the batch run
 * to the test domain object "TestRecord"
 * 
 * A real Spring Batch run would likely map data to domain classes
 * 
 * @author Al Baker
 * @author Clark & Parsia
 *
 */
public class TestRowMapper implements RowMapper<TestRecord> {

	@Override
	public TestRecord mapRow(BindingSet bindingSet) {
		TestRecord record = new TestRecord(bindingSet.value("a").toString(), bindingSet.value("b").toString());
		return record;
	}

}
