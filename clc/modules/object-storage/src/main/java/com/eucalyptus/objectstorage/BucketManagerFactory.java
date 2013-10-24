package com.eucalyptus.objectstorage;

/**
 * Manaager factor for bucket metadata handler
 *
 */
public class BucketManagerFactory {
	private static final BucketManager manager = new DbBucketManagerImpl(); 	
	public static BucketManager getInstance() {
		return manager;
	}
}
