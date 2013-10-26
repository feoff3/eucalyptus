package com.eucalyptus.objectstorage;

/**
 * Manaager factor for bucket metadata handler
 *
 */
public class ObjectManagerFactory {
	private static final ObjectManager manager = new DbObjectManagerImpl(); 	
	public static ObjectManager getInstance() {
		return manager;
	}
}
