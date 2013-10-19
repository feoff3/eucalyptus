package com.eucalyptus.objectstorage;

import org.apache.log4j.Logger;

import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketStateException;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties.VersioningStatus;

public enum Buckets implements BucketManager {
	INSTANCE;
	private static final Logger LOG = Logger.getLogger(Buckets.class);

	@Override
	public Bucket lookupAndClose(String bucketName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean exists(String bucketName) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void createNewBucket(String bucketName, String ownerCanonicalId) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(String bucketName) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(Bucket bucketEntity) throws BucketNotEmptyException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void updateVersioningState(String bucketName,
			VersioningStatus newState) throws InvalidBucketStateException {
		// TODO Auto-generated method stub
		
	}

}
