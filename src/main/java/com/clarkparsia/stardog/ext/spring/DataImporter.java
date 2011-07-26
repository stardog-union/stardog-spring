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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.openrdf.rio.RDFFormat;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;

/**
 * DataImporter
 * 
 * Given the support in Stardog to add/remove data in bulk via a stream,
 * this class provides a Spring Resource aware 
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
public class DataImporter implements InitializingBean {

	private SnarlTemplate snarlTemplate;

	private Map<RDFFormat, Resource> inputFiles;
	
	/**
	 * @return the snarlTemplate
	 */
	public SnarlTemplate getSnarlTemplate() {
		return snarlTemplate;
	}

	/**
	 * @param snarlTemplate the snarlTemplate to set
	 */
	public void setSnarlTemplate(SnarlTemplate snarlTemplate) {
		this.snarlTemplate = snarlTemplate;
	}
	
	/**
	 * @return the inputFiles
	 */
	public Map<RDFFormat, Resource> getInputFiles() {
		return inputFiles;
	}

	/**
	 * @param inputFiles the inputFiles to set
	 */
	public void setInputFiles(Map<RDFFormat, Resource> inputFiles) {
		this.inputFiles = inputFiles;
	}

	/**
	 * <code>inputfile</code>
	 * 
	 * Ingests a file into Stardog, public API to be used after init time
	 * 
	 * @param format - RDFFormat enum
	 * @param resource - Spring Resource
	 * @return boolean
	 */
	public boolean inputFile(final RDFFormat format, final Resource resource) { 
		return snarlTemplate.execute(new ConnectionCallback<Boolean>() {
			@Override
			public Boolean doWithConnection(Connection connection) {
				try {
					connection.add().io().format(format).stream(resource.getInputStream());
				} catch (StardogException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}		
				return true;
			} 
			
		});
	}

	/** 
	 * Loads any configured resources at init time 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		if (inputFiles == null) { 
			return;
		}
		
		snarlTemplate.execute(new ConnectionCallback<Boolean>() {
			@Override
			public Boolean doWithConnection(Connection connection) {
				try {
					for (Entry<RDFFormat, Resource> entry : inputFiles.entrySet()) { 
						connection.add().io().format(entry.getKey()).stream(entry.getValue().getInputStream());
					}
				} catch (StardogException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return false;
				}		
				return true;
			} 
			
		});
	}
	
	
	
	
}
