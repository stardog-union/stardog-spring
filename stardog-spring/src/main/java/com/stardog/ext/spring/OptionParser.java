//Copyright (c) 2010 - 2012 -- Clark & Parsia, LLC. <http://www.clarkparsia.com>
//For more information about licensing and copyright of this software, please contact
//inquiries@clarkparsia.com or visit http://stardog.com

package com.stardog.ext.spring;

import java.util.List;
import java.util.Map;

import com.complexible.common.base.Option;
import com.complexible.common.base.Options;
import com.complexible.common.base.Pair;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
* A registry of options that transforms arbitrary objects into the kind of object the option handles via a transforming function. 
* 
* @author Hector Perez-Urbina
* @version 2.0
*
*/
class OptionParser {

	private final Map<Option<?>, Function<Object, ?>> mOptionsMap;
	
	OptionParser() {
		mOptionsMap = Maps.newHashMap();
	}
	
	/**
	 * Register an option with its associated transforming function.
	 * 
	 * @param theOption
	 * @param theFunction
	 */
	<T> void registerOption(final Option<T> theOption, final Function<Object, T> theFunction) {
		mOptionsMap.put(theOption, theFunction);
	}
	
	/**
	 * Return the value obtained by transforming the given object with the function associated to the given option.
	 * 
	 * @param theOption
	 * @param theValue
	 * @return
	 * @throws IllegalArgumentException if the given option is not registered, if there is a casting exception, or if the transforming function throws an exception or returns null.
	 */
	@SuppressWarnings("unchecked")
 private <T> T getValue(final Option<T> theOption, final Object theValue) throws IllegalArgumentException {
		T result = null;
		
		if (mOptionsMap.containsKey(theOption)) {
			try {
				result = (T) mOptionsMap.get(theOption).apply(theValue);
			}
			catch (Exception e) {
				Throwables.propagateIfInstanceOf(e, IllegalArgumentException.class);
				
				throw new IllegalArgumentException(e);
			}
		}
		
		if (result != null) {
			return result;
		}
		
		throw new IllegalArgumentException();
	}

	/**
	 * Return an Options object by translating the given key-value pairs into options according to the options map.
	 * 
	 * @param theKeyValuePairs
	 * @return
	 */
 Options getOptions(final List<Pair<String, String>> theKeyValuePairs) {
 	Options result = Options.create();
 	
 	for (Pair<String, String> aPair : theKeyValuePairs) {
 		for (Option aRegisteredOption : mOptionsMap.keySet()) {
 			if (aRegisteredOption.toString().equals(aPair.first)) {
 				result.set(aRegisteredOption, getValue(aRegisteredOption, aPair.second));
 			}
 		}
 	}
 	
 	return result;
 }
	
	
}