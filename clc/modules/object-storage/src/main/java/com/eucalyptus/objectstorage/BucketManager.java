package com.eucalyptus.objectstorage;

import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketStateException;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;

/**
 * Interface to operate on buckets
 *
 */
public interface BucketManager {
	/**
	 * Returns a bucket's metadata object. Does NOT preserve the transaction context.
	 * @param bucketName
	 * @return
	 */
	public abstract Bucket lookupAndClose(String bucketName);	
	public abstract boolean exists(String bucketName);
	public abstract void createNewBucket(String bucketName, String ownerCanonicalId);
	
	/**
	 * Delete the bucket by name. Idempotent operation
	 * @param bucketName
	 */
	public abstract void delete(String bucketName);
	
	/**
	 * Delete the bucket represented by the detached entity
	 * This method may use it's own transaction, so the caller is
	 * not required to provide one or pass a loaded or attached entity
	 * @param bucketEntity
	 */
	public abstract void delete(Bucket bucketEntity) throws BucketNotEmptyException;
	
	public abstract void updateVersioningState(String bucketName, ObjectStorageProperties.VersioningStatus newState) throws InvalidBucketStateException; 	
}
