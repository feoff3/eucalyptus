package com.eucalyptus.objectstorage;

import java.util.NoSuchElementException;

import javax.persistence.EntityNotFoundException;
import javax.persistence.EntityTransaction;

import org.apache.log4j.Logger;

import com.eucalyptus.entities.Entities;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.entities.Transactions;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.google.common.base.Supplier;

public class Objects implements ObjectManager {
	private static final Logger LOG = Logger.getLogger(Objects.class);
	
	@Override
	public boolean exists(String bucketName, String objectKey, String versionId) throws TransactionException {
		return lookupAndClose(bucketName, objectKey, versionId) != null;
	}

	@Override
	public ObjectEntity lookupAndClose(String bucketName, String objectKey, String versionId) throws TransactionException {
		try {
			ObjectEntity objectExample = new ObjectEntity(bucketName, objectKey, versionId);
			ObjectEntity foundObject = Transactions.find(objectExample);		
			return foundObject;
		} catch (TransactionException e) {
			if(e.getCause() instanceof NoSuchElementException) {
				return null;
			} else 
				LOG.error("Error looking up object:" + bucketName + "/" + objectKey + " version " + versionId);
			throw e;
		} 
	}

	@Override
	public void delete(String bucketName, String objectKey, String versionId) throws TransactionException {
		try {
			ObjectEntity objectExample = new ObjectEntity(bucketName, objectKey, versionId);			
			Transactions.delete(objectExample);
		} catch (TransactionException e) {
			if(e.getCause() instanceof NoSuchElementException) {
				//Nothing to do, not found is okay
			} else {
				LOG.error("Error looking up object:" + bucketName + "/" + objectKey + " version " + versionId);
				throw e;
			}
		}
	}

	@Override
	public void create(String bucketName, ObjectEntity object, Supplier<String> versionIdSupplier) throws TransactionException {
		

	}

}
