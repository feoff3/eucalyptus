package com.eucalyptus.objectstorage;

import java.util.List;
import java.util.concurrent.Callable;

import javax.persistence.EntityTransaction;

import org.apache.log4j.Logger;

import com.eucalyptus.entities.Entities;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.entities.Transactions;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketStateException;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties.VersioningStatus;

public enum Buckets implements BucketManager {
	INSTANCE;
	private static final Logger LOG = Logger.getLogger(Buckets.class);

	@Override
	public Bucket get(String bucketName, Callable<Boolean> resourceModifier) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists(String bucketName, Callable<Boolean> resourceModifier) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void create(String bucketName, String ownerCanonicalId, Callable<Boolean> resourceModifier) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(String bucketName, Callable<Boolean> resourceModifier) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Bucket bucketEntity, Callable<Boolean> resourceModifier) throws BucketNotEmptyException {
		try {
			//TODO: add emptiness check
			Transactions.delete(bucketEntity);
		} catch(TransactionException e) {
			
		}
	}

	@Override
	public void updateVersioningState(String bucketName,
			VersioningStatus newState, Callable<Boolean> resourceModifier) throws InvalidBucketStateException, TransactionException {
		
		EntityTransaction db = Entities.get(Bucket.class);
		try {
			Bucket searchBucket = new Bucket(bucketName);
			Bucket bucket = Entities.uniqueResult(searchBucket);
			if(VersioningStatus.Disabled.equals(newState) && !bucket.isVersioningDisabled()) {
				//Cannot set versioning disabled if not already disabled.
				throw new InvalidBucketStateException(bucketName);
			}
			bucket.setVersioning(newState.toString());			
			db.commit();
		} catch (TransactionException e) {
			LOG.error("Error updating versioning status for bucket " + bucketName + " due to DB transaction error", e);
			throw e;
		} finally {
			if(db != null && db.isActive()) {
				db.rollback();
			}
		}
	}

	@Override
	public List<Bucket> list(String ownerCanonicalId, boolean includeHidden, Callable<Boolean> resourceModifier) throws TransactionException {
		Bucket searchBucket = new Bucket();
		searchBucket.setOwnerCanonicalId(ownerCanonicalId);
		searchBucket.setHidden(includeHidden);
		List<Bucket> buckets = null;
		try {
			buckets = Transactions.findAll(searchBucket);
			return buckets;
		} catch (TransactionException e) {
			LOG.error("Error listing buckets for user " + ownerCanonicalId + " due to DB transaction error", e);
			throw e;
		}
	}

}
