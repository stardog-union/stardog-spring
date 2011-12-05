/**
 * 
 */
package com.clarkparsia.stardog.ext.spring.batch;

import java.util.List;

import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Adder;

/**
 * @author Al Baker
 * @author Clark & Parsia
 *
 */
public interface BatchAdderCallback<T> {

	void write(Adder adder, List<? extends T> items) throws StardogException;
	
}
