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

import java.util.List;

import com.stardog.stark.impl.IRIImpl;
import com.stardog.stark.impl.StringLiteral;

import com.complexible.stardog.StardogException;
import com.complexible.stardog.api.Adder;

/**
 * An implementation of the BatchADderCallback that processes the list of
 * domain objects, our mock TestRecord, and uses the Snarl Adder to add them
 * to the Stardog database
 * 
 * The SnarlWriter handles transactions, leaving this a simple function applied
 * over a list
 * 
 * @author Al Baker
 * @author Clark & Parsia
 *
 */
public class TestBatchCallback implements BatchAdderCallback<TestRecord> {


	public void write(Adder adder, List<? extends TestRecord> items)
			throws StardogException {
		
		for (TestRecord item : items) {
			adder.statement(new IRIImpl(item.getName()), new IRIImpl("urn:test:propertyUpdate"), new StringLiteral((String) item.getValue() + "update"));
		}
		
	}


}
