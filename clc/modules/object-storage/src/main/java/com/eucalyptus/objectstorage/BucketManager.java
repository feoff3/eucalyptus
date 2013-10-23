package com.eucalyptus.objectstorage;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketStateException;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.google.common.base.Predicate;

/**
 * Interface to operate on buckets
 * Each operation includes a 'resourceModifier' predicate that can optionally be used to invoke another operation that
 * must succeed in order for the metadata/db operation to be committed (e.g. creating a bucket on a filesystem or backend).
 */
public interface BucketManager {

	/**
	 * Create the bucket
	 * @param bucketName
	 * @param ownerCanonicalId
	 * @param resourceModifier
	 * @return
	 * @throws TransactionException
	 */
	public abstract <T extends Object ,R extends Object> T create(@Nonnull String bucketName, 
			@Nonnull String ownerCanonicalId,
			@Nonnull String ownerIamUserId,
			@Nonnull String acl, 
			@Nonnull String location,			
			@Nullable ReversableOperation<T,R> resourceModifier) throws S3Exception, TransactionException;

	/**
	 * Returns a bucket's metadata object. Does NOT preserve the transaction context.
	 * @param bucketName
	 * @return
	 */
	public abstract Bucket get(@Nonnull String bucketName, @Nullable Callable<Boolean> resourceModifier) throws TransactionException;
	/**
	 * Returns list of buckets owned by id. Buckets are detached from any persistence session.
	 * @param ownerCanonicalId
	 * @param includeHidden
	 * @return
	 */
	public abstract List<Bucket> list(@Nonnull String ownerCanonicalId, boolean includeHidden, @Nullable Callable<Boolean> resourceModifier) throws TransactionException;

	/**
	 * Returns list of buckets owned by user's iam id, in the given account. Buckets are detached from any persistence session.
	 * @return
	 */
	public abstract List<Bucket> listByUser(@Nonnull String userIamId, boolean includeHidden, @Nullable Callable<Boolean> resourceModifier) throws TransactionException;
	
	public abstract long countByUser(@Nonnull String userIamId, boolean includeHidden, Callable<Boolean> resourceModifier) throws ExecutionException;

	/**
	 * Checks if bucket exists.
	 * @param bucketName
	 * @return
	 */
	public abstract boolean exists(@Nonnull String bucketName, @Nullable Callable<Boolean> resourceModifier) throws TransactionException;
	
	/**
	 * Delete the bucket by name. Idempotent operation
	 * @param bucketName
	 */
	public abstract void delete(@Nonnull String bucketName, @Nullable Callable<Boolean> resourceModifier)  throws TransactionException;
	
	/**
	 * Delete the bucket represented by the detached entity
	 * This method may use it's own transaction, so the caller is
	 * not required to provide one or pass a loaded or attached entity
	 * @param bucketEntity
	 */
	public abstract void delete(Bucket bucketEntity, Callable<Boolean> resourceModifier)  throws TransactionException, BucketNotEmptyException;
	
	public abstract void updateVersioningState(String bucketName, ObjectStorageProperties.VersioningStatus newState, Callable<Boolean> resourceModifier) throws InvalidBucketStateException, TransactionException; 	
}
