/*************************************************************************
 * Copyright 2009-2013 Eucalyptus Systems, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; version 3 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 *
 * Please contact Eucalyptus Systems, Inc., 6755 Hollister Ave., Goleta
 * CA 93117, USA or visit http://www.eucalyptus.com/licenses/ if you need
 * additional information or have any questions.
 ************************************************************************/

package com.eucalyptus.objectstorage;

import java.util.List;
import java.util.concurrent.ExecutionException;

import com.eucalyptus.auth.principal.User;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties.VersioningStatus;

/**
 * Interface to operate on buckets
 * Each operation includes a 'resourceModifier' CallableWithRollback that can optionally be used to invoke another operation that
 * must succeed in order for the metadata/db operation to be committed (e.g. creating a bucket on a filesystem or backend).
 * The intent is to allow wrapping the backend operation in a transaction as necessary.
 * 
 * This interface is an enactment mechanism, not a policy checker. Validation on input, beyon what is required for the operation
 * to succeed, is outside the scope for this. e.g. BucketManager will not enforce S3 naming conventions.
 */
public interface BucketManager {

	
	/**
	 * Change bucket size estimate. sizeToChange can be any value. Negatives decrement, positives
	 * increment the size
	 * @param bucketName
	 * @param sizeToChange
	 * @throws TransactionException
	 */
	public abstract void updateBucketSize(String bucketName, long sizeToChange) throws TransactionException;
	
	/**
	 * Create the bucket
	 * @param bucketName
	 * @param ownerCanonicalId
	 * @param resourceModifier
	 * @return
	 * @throws TransactionException
	 */
	public abstract <T extends Object ,R extends Object> T create(String bucketName, 
			 User owner,
			 String acl, 
			 String location,			
			 CallableWithRollback<T,R> resourceModifier) throws Exception, TransactionException;

	/**
	 * Returns a bucket's metadata object. Does NOT preserve the transaction context.
	 * @param bucketName
	 * @return
	 */
	public abstract Bucket get( String bucketName,
			 boolean includeHidden,
			 CallableWithRollback<?,?> resourceModifier) throws S3Exception, TransactionException;
	/**
	 * Returns list of buckets owned by id. Buckets are detached from any persistence session.
	 * @param ownerCanonicalId
	 * @param includeHidden
	 * @return
	 */
	public abstract List<Bucket> list( String ownerCanonicalId, 
			 boolean includeHidden, 
			 CallableWithRollback<?,?> resourceModifier) throws S3Exception, TransactionException;

	/**
	 * Returns list of buckets owned by user's iam id, in the given account. Buckets are detached from any persistence session.
	 * @return
	 */
	public abstract List<Bucket> listByUser( String userIamId, 
			boolean includeHidden,  
			CallableWithRollback<?,?> resourceModifier) throws S3Exception, TransactionException;
	
	/**
	 * Returns count of buckets owned by user's iam id, in the given account. Buckets are detached from any persistence session.
	 * @return
	 */
	public abstract long countByUser( String userIamId, 
			boolean includeHidden, 
			CallableWithRollback<?,?> resourceModifier) throws S3Exception, ExecutionException;
	/**
	 * Returns count of buckets owned by account id, in the given account. Buckets are detached from any persistence session.
	 * @return
	 */	
	public abstract long countByAccount(String canonicalId, 
			boolean includeHidden, 
			CallableWithRollback<?,?> resourceModifier) throws ExecutionException;
	
	/**
	 * Checks if bucket exists.
	 * @param bucketName
	 * @return
	 */
	public abstract boolean exists( String bucketName,  
			CallableWithRollback<?,?> resourceModifier) throws S3Exception, TransactionException;
	
	/**
	 * Delete the bucket by name. Idempotent operation.
	 * Does *NOT* enforce emptiness checks or any other policy on the bucket
	 * @param bucketName
	 */
	public abstract <T> T delete( String bucketName,  
			CallableWithRollback<T,?> resourceModifier)  throws S3Exception, TransactionException;
	
	/**
	 * Delete the bucket represented by the detached entity
	 * This method may use it's own transaction, so the caller is
	 * not required to provide one or pass a loaded or attached entity
	 *
	 * Does *NOT* enforce emptiness checks or any other policy on the bucket
	 * @param bucketEntity
	 */
	public abstract <T> T delete(Bucket bucketEntity, 
			CallableWithRollback<T,?> resourceModifier) throws TransactionException, S3Exception;
		
	public abstract <T> T setAcp(Bucket bucketEntity, String acl, CallableWithRollback<T, ?> resourceModifier)  throws TransactionException, S3Exception;	
	public abstract <T> T setLoggingStatus(Bucket bucketEntity, Boolean loggingEnabled, String destBucket, String destPrefix, CallableWithRollback<T, ?> resourceModifier) throws TransactionException, S3Exception;
	public abstract <T> T setVersioning(Bucket bucketEntity, VersioningStatus newState, CallableWithRollback<T, ?> resourceModifier) throws TransactionException, S3Exception;	
	
	/**
	 * Get a versionId for an object, since this is based on the bucket config
	 * @param bucketEntity
	 * @return
	 * @throws TransactionException
	 * @throws S3Exception
	 */
	public abstract String getVersionId(Bucket bucketEntity) throws TransactionException, S3Exception;
	
	/**
	 * Returns the approximate total size of all objects in all buckets
	 * in the system. This is not guaranteed to be consistent. An approximation
	 * at best.
	 * @return
	 */
	public long totalSizeOfAllBuckets();
	
	public boolean checkBucketName(String bucketName) throws Exception;
}
