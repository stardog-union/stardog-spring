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
package com.complexible.stardog.ext.spring;

import com.complexible.common.base.Pair;
import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.ConnectionPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * StardogConnectionFactoryBean
 * 
 * This class implements the Spring interfaces for FactoryBean for a DataSource,
 * InitializingBean, and DisposableBean, so it is a fully Spring-aware factory
 * 
 * The objective is to configure one of these per Spring application context, and be
 * able to reference DataSource objects in the SnarlTemplate, so a SnarlTemplate always
 * gets a connection from the pool (via DataSource wrapper) injected in to it.
 * 
 * Configuration for this object matches both the parameters in ConnectionConfiguration and
 * ConnectionPoolConfiguration, and then inspects what has been injected to create the 
 * connection pool.  
 * 
 * @author Clark and Parsia, LLC
 * @author Al Baker
 *
 */
public class DataSourceFactoryBean implements FactoryBean<DataSource>, InitializingBean, DisposableBean {

	final Logger log = LoggerFactory.getLogger(DataSourceFactoryBean.class);
	
	/**
	 * Properties used by the ConnectionConfig
	 */
	private String url;
	
	private String username;
	
	private String password;

	private boolean reasoningType = false;
	
	private String to;
	
	private Properties connectionProperties;
	
	/**
	 * Properties used by the ConnectionPoolConfig
	 * 
	 * TimeUnits default to miliseconds, but can be configured in Spring
	 * 
	 */
	private long blockCapacityTime = 900;
	
	private TimeUnit blockCapacityTimeUnit = TimeUnit.SECONDS;
	
	private long expirationTime = 300;
	
	private TimeUnit expirationTimeUnit = TimeUnit.SECONDS;
	
	private boolean failAtCapacity = false;
	
	private boolean growAtCapacity = true;
	
	private int maxIdle = 100;
	
	private int maxPool = 200;
	
	private int minPool = 10;
	
	private boolean noExpiration = false;

    private Provider provider;
	
	
	/**
	 * Private references to the pools and configurations that get
	 *  constructed - see afterPropertiesSet()
	 */
	
	private DataSource dataSource;
		
	@Override
	public DataSource getObject() throws Exception {
		return dataSource;
	}

	@Override
	public Class<?> getObjectType() {
		return DataSource.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	/**
	 * <code>destroy</code>
	 * Called by Spring 
	 */
	public void destroy() { 
		log.debug("Destroying dataSourceFactory bean");
		dataSource.destroy();
		dataSource = null;
	}
	
	/**
	 * <code>afterProperiesSet</code>
	 * 
	 * Spring interface for performing an action after the properties have been set on the bean
	 * 
	 * In this case, all configuration information will be passed to this object, and we can
	 * initialize the connection pool here
	 * 
	 * Alternative method would be to declare a separate init method, and tell Spring about it
	 * 
	 * @throws Exception on Stardog create, e.g. database down
	 */
	@Override
	public void afterPropertiesSet() throws Exception {
		log.debug("Initializing Stardog connection configuration");
		
		ConnectionConfiguration connectionConfig;
		
		ConnectionPoolConfig poolConfig;
		
		connectionConfig = ConnectionConfiguration.to(to);
		
		if (url != null) { 
			connectionConfig = connectionConfig.server(url);
		}


        if (provider != null) {
            provider.execute(to, url, username, password);
        }

		
		if (connectionProperties != null) {
			List<Pair<String, String>> aOptionsList = new ArrayList<Pair<String, String>>();
			for (String key : connectionProperties.stringPropertyNames()) {
				aOptionsList.add(Pair.create((String) key, (String) connectionProperties.getProperty(key)));
			}
			connectionConfig = connectionConfig.with((new OptionParser()).getOptions(aOptionsList));
		}
		

		connectionConfig = connectionConfig.reasoning(reasoningType);

		
		connectionConfig = connectionConfig.credentials(username, password);
		
		poolConfig = ConnectionPoolConfig
				.using(connectionConfig) 
				.minPool(minPool) 
				.maxPool(maxPool) 
				.expiration(expirationTime, expirationTimeUnit) 
				.blockAtCapacity(blockCapacityTime, blockCapacityTimeUnit); 
		
		dataSource = new DataSource(connectionConfig, poolConfig);
		dataSource.afterPropertiesSet();
		
	}

	
	/**********************************************************
	 * Getters and Setters
	 **********************************************************/

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}

	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}

	/**
	 * @return the reasoningType
	 */
	public boolean getReasoningType() {
		return reasoningType;
	}

	/**
	 * @param reasoningType the reasoningType to set
	 */
	public void setReasoningType(boolean reasoningType) {
		this.reasoningType = reasoningType;
	}

	/**
	 * @return the to
	 */
	public String getTo() {
		return to;
	}

	/**
	 * @param to the to to set
	 */
	public void setTo(String to) {
		this.to = to;
	}

	/**
	 * @return the connectionProperties
	 */
	public Properties getConnectionProperties() {
		return connectionProperties;
	}

	/**
	 * @param connectionProperties the connectionProperties to set
	 */
	public void setConnectionProperties(Properties connectionProperties) {
		this.connectionProperties = connectionProperties;
	}

	/**
	 * @return the blockCapacityTime
	 */
	public long getBlockCapacityTime() {
		return blockCapacityTime;
	}

	/**
	 * @param blockCapacityTime the blockCapacityTime to set
	 */
	public void setBlockCapacityTime(long blockCapacityTime) {
		this.blockCapacityTime = blockCapacityTime;
	}

	/**
	 * @return the blockCapacityTimeUnit
	 */
	public TimeUnit getBlockCapacityTimeUnit() {
		return blockCapacityTimeUnit;
	}

	/**
	 * @param blockCapacityTimeUnit the blockCapacityTimeUnit to set
	 */
	public void setBlockCapacityTimeUnit(TimeUnit blockCapacityTimeUnit) {
		this.blockCapacityTimeUnit = blockCapacityTimeUnit;
	}

	/**
	 * @return the expirationTime
	 */
	public long getExpirationTime() {
		return expirationTime;
	}

	/**
	 * @param expirationTime the expirationTime to set
	 */
	public void setExpirationTime(long expirationTime) {
		this.expirationTime = expirationTime;
	}

	/**
	 * @return the expirationTimeUnit
	 */
	public TimeUnit getExpirationTimeUnit() {
		return expirationTimeUnit;
	}

	/**
	 * @param expirationTimeUnit the expirationTimeUnit to set
	 */
	public void setExpirationTimeUnit(TimeUnit expirationTimeUnit) {
		this.expirationTimeUnit = expirationTimeUnit;
	}

	/**
	 * @return the failAtCapacity
	 */
	public boolean isFailAtCapacity() {
		return failAtCapacity;
	}

	/**
	 * @param failAtCapacity the failAtCapacity to set
	 */
	public void setFailAtCapacity(boolean failAtCapacity) {
		this.failAtCapacity = failAtCapacity;
	}

	/**
	 * @return the growAtCapacity
	 */
	public boolean isGrowAtCapacity() {
		return growAtCapacity;
	}

	/**
	 * @param growAtCapacity the growAtCapacity to set
	 */
	public void setGrowAtCapacity(boolean growAtCapacity) {
		this.growAtCapacity = growAtCapacity;
	}

	/**
	 * @return the maxIdle
	 */
	public int getMaxIdle() {
		return maxIdle;
	}

	/**
	 * @param maxIdle the maxIdle to set
	 */
	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	/**
	 * @return the maxPool
	 */
	public int getMaxPool() {
		return maxPool;
	}

	/**
	 * @param maxPool the maxPool to set
	 */
	public void setMaxPool(int maxPool) {
		this.maxPool = maxPool;
	}

	/**
	 * @return the minPool
	 */
	public int getMinPool() {
		return minPool;
	}

	/**
	 * @param minPool the minPool to set
	 */
	public void setMinPool(int minPool) {
		this.minPool = minPool;
	}

	/**
	 * @return the noExpiration
	 */
	public boolean isNoExpiration() {
		return noExpiration;
	}

	/**
	 * @param noExpiration the noExpiration to set
	 */
	public void setNoExpiration(boolean noExpiration) {
		this.noExpiration = noExpiration;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}


    public Provider getProvider() {
        return provider;
    }

    public void setProvider(Provider provider) {
        this.provider = provider;
    }
}
