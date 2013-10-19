package com.eucalyptus.objectstorage;

import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.google.common.base.Supplier;

/**
 * Interface for interacting with object metadata (not content directly)
 * @author zhill
 *
 */
public interface ObjectManager {
	
	public abstract boolean exists(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	public abstract ObjectEntity lookupAndClose(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	public abstract void delete(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	/**
	 * Uses the provided supplier to get a versionId since that is dependent on the bucket state
	 * @param bucketName
	 * @param object
	 * @param versionIdSupplier
	 */
	public abstract void create(String bucketName, ObjectEntity object, Supplier<String> versionIdSupplier) throws TransactionException;
	
}
