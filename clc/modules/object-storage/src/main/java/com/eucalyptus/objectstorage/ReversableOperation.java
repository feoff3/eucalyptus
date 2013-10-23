package com.eucalyptus.objectstorage;

import java.util.concurrent.Callable;

/**
 * An wrapper for an operation with two phases:
 * call()
 * rollback()
 * 
 * To allow passing of operations (like Callable) with
 * another rollback option if necessary.
 * @author zhill
 *
 */
public interface ReversableOperation<T,R> {
	/**
	 * Do the operation
	 * @return
	 * @throws Exception
	 */
	public abstract T call() throws Exception;
	
	/**
	 * Rollback the previous call.
	 * @return
	 * @throws Exception
	 */
	public abstract R rollback(T arg) throws Exception;
}
