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

import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.persistence.EntityTransaction;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.criterion.Example;
import org.hibernate.criterion.Projections;

import com.eucalyptus.entities.Entities;
import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.entities.Transactions;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.BucketAlreadyExistsException;
import com.eucalyptus.objectstorage.exceptions.s3.BucketAlreadyOwnedByYouException;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketNameException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketStateException;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties.VersioningStatus;

public class DbBucketManagerImpl implements BucketManager {
	private static final Logger LOG = Logger.getLogger(DbBucketManagerImpl.class);
	
	/**
	 * Does the bucket contain snapshots...
	 * @param bucketName
	 * @return
	 * @throws Exception
	 */
	private boolean bucketHasSnapshots(String bucketName) throws Exception {
		EntityWrapper<ObjectEntity> dbSnap = null;

		try {
			dbSnap = EntityWrapper.get(ObjectEntity.class);
			ObjectEntity objInfo = new ObjectEntity();
			objInfo.setBucketName(bucketName);
			objInfo.setIsSnapshot(true);

			Criteria snapCount = dbSnap.createCriteria(ObjectEntity.class).add(Example.create(objInfo)).setProjection(Projections.rowCount());
			snapCount.setReadOnly(true);
			Long rowCount = (Long)snapCount.uniqueResult();
			dbSnap.rollback();
			if (rowCount != null && rowCount.longValue() > 0) {
				return true;
			}
			return false;
		} catch(Exception e) {
			if(dbSnap != null) {
				dbSnap.rollback();
			}
			throw e;
		}
	}
	
	@Override
	public Bucket get(@Nonnull String bucketName,
			@Nonnull boolean includeHidden,
			@Nullable ReversibleOperation<?,?> resourceModifier) throws S3Exception, TransactionException {
		try {
			Bucket searchExample = new Bucket(bucketName);
			searchExample.setHidden(includeHidden);
			return Transactions.find(searchExample);
		} catch (TransactionException e) {
			LOG.error("Error querying bucket existence in db",e);
			throw e;
		}		
	}

	@Override
	public boolean exists(@Nonnull String bucketName,
			@Nullable ReversibleOperation<?,?> resourceModifier) throws S3Exception, TransactionException {
		try {
			return (Transactions.find(new Bucket(bucketName)) != null);
		} catch (TransactionException e) {
			LOG.error("Error querying bucket existence in db",e);
			throw e;
		}
	}

	@Override
	public <T,R> T create(@Nonnull String bucketName, 
			@Nonnull String ownerCanonicalId,
			@Nonnull String ownerIamUserId,
			@Nonnull String acl, 
			@Nonnull String location,
			@Nullable ReversibleOperation<T,R> resourceModifier) throws S3Exception, TransactionException {

		Bucket newBucket = new Bucket(bucketName);
		try {
			Bucket foundBucket = Transactions.find(newBucket);
			if(foundBucket != null) {
				if(foundBucket.getOwnerCanonicalId().equals(ownerCanonicalId)) {
					throw new BucketAlreadyOwnedByYouException(bucketName);
				} else {
					throw new BucketAlreadyExistsException(bucketName);
				}
			}
		} catch(NoSuchElementException e) {
			//Expected result, continue	
		} catch(TransactionException e) {
			//Lookup failed.
			LOG.error("Lookup for bucket " + bucketName + " failed during creation checks. Cannot proceed.",e);
			throw new InternalErrorException(bucketName);
		}
		
		newBucket.setOwnerCanonicalId(ownerCanonicalId);
		newBucket.setBucketSize(0L);
		newBucket.setHidden(false);
		newBucket.setAcl(acl);
		newBucket.setLocation(location);
		newBucket.setLoggingEnabled(false);
		newBucket.setOwnerIamUserId(ownerIamUserId);
		newBucket.setVersioning(ObjectStorageProperties.VersioningStatus.Disabled.toString());
		newBucket.setCreationDate(new Date());
		
		T result = null;		
		try {
			if(resourceModifier != null) {
				result = resourceModifier.call();
			}
		} catch(Exception e) {
			LOG.error("Error creating bucket in backend",e);
			throw new InternalErrorException(bucketName);
		}
		
		try {
			Transactions.saveDirect(newBucket);			
		} catch(TransactionException ex) {
			//Rollback the bucket creation.
			LOG.error("Error persisting bucket record for bucket " + bucketName, ex);
			
			//Do backend cleanup here.
			if(resourceModifier != null) {
				try {
					R rollbackResult = resourceModifier.rollback(result);
				} catch(Exception e) {
					LOG.error("Backend rollback of operation failed",e);			
				}
			}
			throw ex;
		}
		
		return result;			
	}
	
	@Override
	public <T> T delete(String bucketName, 
			ReversibleOperation<T,?> resourceModifier) throws S3Exception, TransactionException {
		
		Bucket searchEntity = new Bucket(bucketName);
		try {
			Transactions.find(searchEntity);
		} catch(TransactionException e) {
			LOG.error("Transaction error during bucket lookup for " + bucketName);
			throw e;
		} catch(NoSuchElementException e) {
			LOG.debug("Nothing to do to delete bucket " + bucketName + " not found in db.");
			//Nothing to do. continue to resource modification
		}
		
		return delete(searchEntity, resourceModifier);
	}

	@Override
	public <T> T delete(Bucket bucketEntity, 
			ReversibleOperation<T,?> resourceModifier) throws S3Exception, TransactionException {
		try {			
			Transactions.delete(bucketEntity);
		} catch(TransactionException e) {
			LOG.error("Error deleting bucket in DB",e);
			throw e;
		} catch(NoSuchElementException e) {
			//Ok, continue.			
		}
		
		T result = null;
		try {
			result = resourceModifier.call();
			return result;
		} catch(Throwable e) {
			LOG.error("Error in backend call for delete bucket: " + bucketEntity.getBucketName(), e);
			try {
				resourceModifier.rollback(result);
			} catch(Throwable ex ) {
				LOG.error("Error in rollback after failed delete call on bucket " + bucketEntity.getBucketName(),e);
			}
			throw new InternalErrorException(e.getMessage());
		}
		
	}

	@Override
	public void updateVersioningState(String bucketName,
			VersioningStatus newState, 
			ReversibleOperation<?,?> resourceModifier) throws InvalidBucketStateException, TransactionException {
		
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
	public List<Bucket> list(String ownerCanonicalId, 
			boolean includeHidden, 
			ReversibleOperation<?,?> resourceModifier) throws TransactionException {
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
	
	@Override
	public List<Bucket> listByUser(String userIamId, 
			boolean includeHidden, 
			ReversibleOperation<?,?> resourceModifier) throws TransactionException {
		Bucket searchBucket = new Bucket();
		searchBucket.setHidden(includeHidden);
		searchBucket.setOwnerIamUserId(userIamId);
		List<Bucket> buckets = null;
		try {
			buckets = Transactions.findAll(searchBucket);
			return buckets;
		} catch (TransactionException e) {
			LOG.error("Error listing buckets for user " + userIamId + " due to DB transaction error", e);
			throw e;
		}
	}
	
	@Override
	public long countByUser(String userIamId, 
			boolean includeHidden, 
			ReversibleOperation<?,?> resourceModifier) throws ExecutionException {
		Bucket searchBucket = new Bucket();
		searchBucket.setHidden(includeHidden);
		searchBucket.setOwnerIamUserId(userIamId);
		EntityTransaction db = Entities.get(Bucket.class);
		try {
			return Entities.count(searchBucket);
		} catch (Exception e) {
			LOG.error("Error counting buckets for user " + userIamId + " due to DB transaction error", e);
			throw new ExecutionException(e);
		} finally {
			db.rollback();
		}
	}

	@Override
	public long countByAccount(String canonicalId, 
			boolean includeHidden, 
			ReversibleOperation<?,?> resourceModifier) throws ExecutionException {
		Bucket searchBucket = new Bucket();
		searchBucket.setHidden(includeHidden);
		searchBucket.setOwnerCanonicalId(canonicalId);
		EntityTransaction db = Entities.get(Bucket.class);
		try {
			return Entities.count(searchBucket);
		} catch (Exception e) {
			LOG.error("Error counting buckets for account canonicalId " + canonicalId + " due to DB transaction error", e);
			throw new ExecutionException(e);
		} finally {
			db.rollback();
		}
	}

}
