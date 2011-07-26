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

import com.clarkparsia.stardog.api.Connection;

/**
 * ConnectionCallback
 * 
 * Allows implementations a prepared, transactional access to a connection
 * without any of the boilerplate.
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
public interface ConnectionCallback<T> {

	T doWithConnection(Connection connection);

}
