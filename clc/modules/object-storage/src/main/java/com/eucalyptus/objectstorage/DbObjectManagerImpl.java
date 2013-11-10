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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.persistence.EntityTransaction;

import org.apache.log4j.Logger;
import org.hibernate.Criteria;
import org.hibernate.criterion.Example;
import org.hibernate.criterion.MatchMode;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;

import com.eucalyptus.entities.Entities;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.entities.Transactions;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.util.OSGUtil;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;

/**
 * Database backed implementation of ObjectManager
 *
 */
public class DbObjectManagerImpl implements ObjectManager {
	private static final Logger LOG = Logger.getLogger(DbObjectManagerImpl.class);
	private static final ExecutorService HISTORY_REPAIR_EXECUTOR = Executors.newCachedThreadPool();
	
    	public void start() throws Exception {
    	    //Do nothing
    	}
    	
    	public void stop() throws Exception {
    	    try {
    		List<Runnable> pendingTasks = HISTORY_REPAIR_EXECUTOR.shutdownNow();
    		LOG.info("Stopping ObjectManager... Found " + pendingTasks.size() + " pending tasks at time of shutdown");
    	    } catch(final Throwable f) {
    		LOG.error("Error stopping ObjectManager", f);
    	    }
    	}
    
	@Override
	public <T,F> boolean exists(String bucketName, String objectKey, String versionId,  CallableWithRollback<T, F> resourceModifier) throws Exception {
		try {
			return get(bucketName, objectKey, versionId) != null;
		} catch(NoSuchElementException e) {
			return false;
		} catch(Exception e) {
			LOG.error("Error determining existence of " + bucketName + "/" + objectKey + " , version=" + versionId);
			throw e;
		}
	}

	@Override
	public long count(String bucketName) throws Exception {
		EntityTransaction db = Entities.get(ObjectEntity.class);
		ObjectEntity exampleObject = new ObjectEntity(bucketName, null, null);
		exampleObject.setDeleted(false);
		try {
			return Entities.count(exampleObject);
		} catch(Throwable e) {
			LOG.error("Error getting object count for bucket " + bucketName, e);
			throw new Exception(e);
		} finally {
			db.rollback();
		}		
	}
	
	/**
	 * Returns the list of entities currently pending writes. List returned is in no
	 * particular order. Caller must order if required.
	 * @param bucketName
	 * @param objectKey
	 * @param versionId
	 * @return
	 * @throws TransactionException
	 */
	public List<ObjectEntity> getPendingWrites(String bucketName, String objectKey, String versionId) throws Exception {
		try {
			//Return the latest version based on the created date.
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, versionId);
				Criteria search = Entities.createCriteria(ObjectEntity.class);			
				List results = search.add(Example.create(searchExample))
							.add(Restrictions.isNull("objectModifiedTimestamp"))
							.list();
				db.commit();
				return (List<ObjectEntity>)results;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(NoSuchElementException e) {
			//Nothing, return empty list
			return new ArrayList<ObjectEntity>(0);
		} catch(Exception e) {
			LOG.error("Error fetching pending write records for object " + bucketName + "/" + objectKey + "?versionId=" + versionId);
			throw e;
		}
	}
	
	/**
	 * A more limited version of read-repair, it just modifies the 'islatest' tag, but will not mark any for deletion
	 */
	private static final Predicate<ObjectEntity> SET_LATEST_PREDICATE = new Predicate<ObjectEntity>() {
		public boolean apply(ObjectEntity example) {
			try {
				example.setIsLatest(true);
				Criteria search = Entities.createCriteria(ObjectEntity.class);
				List<ObjectEntity> results = search.add(Example.create(example))
						.add(ObjectEntity.QueryHelpers.getNotDeletingRestriction())
						.add(ObjectEntity.QueryHelpers.getNotPendingRestriction())
						.addOrder(Order.desc("objectModifiedTimestamp"))
						.list();
				
				if(results != null && results.size() > 1) {				
					try {
						//Set all but the first element as not latest
						for(ObjectEntity obj : results.subList(1, results.size())) {
							obj.makeNotLatest();										
						}
					} catch(IndexOutOfBoundsException e) {
						//Either 0 or 1 result, nothing to do
					}
				}
			} catch(NoSuchElementException e) {
				//Nothing to do.
			} catch(Exception e) {
				LOG.error("Error consolidationg Object records for " + example.getBucketName() + "/" + example.getObjectKey());
				return false;
			}
			return true;
		}
	};
		
	/**
	 * This is the proper function to use for doing read-repair operations
	 */
	private static final Predicate<ObjectEntity> REPAIR_OBJECT_HISTORY_PREDICATE = new Predicate<ObjectEntity>() {
		public boolean apply(ObjectEntity example) {
			try {					
				Criteria search = Entities.createCriteria(ObjectEntity.class);
				List<ObjectEntity> results = search.add(Example.create(example))
						.add(ObjectEntity.QueryHelpers.getNotDeletingRestriction())
						.add(ObjectEntity.QueryHelpers.getNotPendingRestriction())
						.addOrder(Order.desc("objectModifiedTimestamp"))
						.list();
				
				ObjectEntity lastViewed = null;
				if(results != null) {
				    try {					
					//Set all but the first element as not latest
					for(ObjectEntity obj : results.subList(1, results.size())) {
					    obj.makeNotLatest();
					    
					    if(obj.isNullVersioned()) {
						if(lastViewed != null && lastViewed.isNullVersioned()) {
						    obj.markForDeletion();
						}
						lastViewed = obj;
					    } else {
						lastViewed = null;
					    }
					}
				    } catch(IndexOutOfBoundsException e) {
					//Either 0 or 1 result, nothing to do
				    }				    
				}
			} catch(NoSuchElementException e) {
				//Nothing to do.
			} catch(Exception e) {
				LOG.error("Error consolidationg Object records for " + example.getBucketName() + "/" + example.getObjectKey());
				return false;
			}
			return true;
		}
	};
	
	/**
	 * Finds all object records and keeps latest, marks rest for deletion if enabledVersioning == false, or just removes isLatest if enabledVersioning == true
	 * @param bucketName
	 * @param objectKey
	 * @return
	 * @throws Exception
	 */
	public void repairObjectLatest(String bucketName, String objectKey) throws Exception {	
		ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, null);
		try {
			Entities.asTransaction(SET_LATEST_PREDICATE).apply(searchExample);
		} catch(final Throwable f) {
			LOG.error("Error in version/null repair",f);
		}
	}
	
	/**
	 * Scans the object for any contiguous "null" versioned records and removes all but most recent.
	 * Only modifies contiguous, non-deleted records where the versionId="null" (as a string).
	 * @param bucketName
	 * @param objectKey
	 * @throws Exception
	 */
	public void doFullRepair(String bucketName, String objectKey) throws Exception {
		ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, null);
		try {
			Entities.asTransaction(REPAIR_OBJECT_HISTORY_PREDICATE).apply(searchExample);
		} catch(final Throwable f) {
			LOG.error("Error in version/null repair",f);
		}
	}
	
	/**
	 * Returns the ObjectEntities that have failed or are marked for deletion
	 */
	@Override
	public List<ObjectEntity> getFailedOrDeleted() throws Exception {
		try {
			//Return the latest version based on the created date.
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				ObjectEntity searchExample = new ObjectEntity();
				Criteria search = Entities.createCriteria(ObjectEntity.class);			
				List results = search.add(Example.create(searchExample))
							.add(Restrictions.or(ObjectEntity.QueryHelpers.getDeletedRestriction(), ObjectEntity.QueryHelpers.getFailedRestriction()))
							.list();
				db.commit();
				return (List<ObjectEntity>)results;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(NoSuchElementException e) {
			//Swallow this exception
		} catch(Exception e) {
			LOG.error("Error fetching failed or deleted object records");
			throw e;
		}
		
		return new ArrayList<ObjectEntity>(0);
	}

	@Override
	public ObjectEntity get(String bucketName, String objectKey, String versionId) throws Exception {
		try {
			//Return the latest version based on the created date.
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, versionId);
				if(versionId == null) {
					searchExample.setIsLatest(true);
				}
				
				Criteria search = Entities.createCriteria(ObjectEntity.class);			
				List<ObjectEntity> results = search.add(Example.create(searchExample))
							.addOrder(Order.desc("objectModifiedTimestamp"))
							.add(ObjectEntity.QueryHelpers.getNotPendingRestriction())
							.add(ObjectEntity.QueryHelpers.getNotDeletingRestriction())
							.list();
				
				if(results == null || results.size() < 1) {
					throw new NoSuchElementException();
				} else if(results.size() > 1) {
					this.repairObjectLatest(bucketName, objectKey);
				}
				
				db.commit();
				return results.get(0);
				
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(NoSuchElementException ex) { 
			throw ex;
		} catch(Exception e) {
			LOG.error("Error getting object entity for " + bucketName + "/" + objectKey + "?version=" + versionId, e);
			throw e;
		}		
	}

	@Override
	public <T, F> void delete(ObjectEntity object, CallableWithRollback<T, F> resourceModifier) throws Exception {
		T result = null;					
		//Set to 'deleting'			
		if(!object.getDeleted()) {
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				object.markForDeletion();				
				Entities.mergeDirect(object);
			} catch(final Throwable f) {
				throw new InternalErrorException(object.getResourceFullName());
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		}
		if(resourceModifier != null) {			
			try {
				result = resourceModifier.call();
			} catch(S3Exception e) {
				LOG.error("S3 Error calling modify resource in object delete", e);
				try {
					resourceModifier.rollback(result);
				} catch(Exception ex) {
					LOG.error("Error in rollback during error recovery of object delete",e);
				}
				throw e;
			} catch(Exception e) {
				LOG.error("Error calling modify resource in object delete", e);
				InternalErrorException intEx = new InternalErrorException(object.getBucketName() + "/" + object.getObjectKey() + (object.getVersionId() == null ? "" : "?versionId=" + object.getVersionId()));
				intEx.initCause(e);
				throw intEx;
			}
		}
		
		//Fully remove the record.
		try {
			Transactions.delete(object);
			
			//Update bucket size
			BucketManagers.getInstance().updateBucketSize(object.getBucketName(), -object.getSize());
		} catch(NoSuchElementException e) {
			//Ok, not found. Can't update what isn't there. May have been updated concurrently.
		} catch (Exception e) {
			LOG.error("Error looking up object:" + object.getBucketName() + "/" + object.getObjectKey() + (object.getVersionId() == null ? "" : "?versionId=" + object.getVersionId()));
			throw e;
		}		
	}

	@Override
	public <T extends PutObjectResponseType, F> T create(final String bucketName, final ObjectEntity object, CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException {
		T result = null;
		try {
			ObjectEntity savedEntity = null;
			//Persist the new record in the 'pending' state.
			try {
				savedEntity = Transactions.saveDirect(object);
			} catch(TransactionException e) {
				//Fail. Could not persist.
				LOG.error("Transaction error creating initial object metadata for " + object.getResourceFullName(), e);
			} catch(Exception e) {
				//Fail. Unknown.
				LOG.error("Error creating initial object metadata for " + object.getResourceFullName(), e);
			}
						
			//Send the data through to the backend
			if(resourceModifier != null) {		
				//This could be a long-lived operation...minutes
				result = resourceModifier.call();
		
				Date updatedDate = null;
				//Update the record and cleanup
				if(result != null) {					
					if(result.getLastModified() != null) {
						updatedDate = OSGUtil.dateFromHeaderFormattedString(result.getLastModified());
						if(updatedDate == null) {							
							updatedDate = OSGUtil.dateFromRFC822FormattedString(result.getLastModified());
						}
					} else {
						updatedDate = new Date();					
					}
					
					savedEntity.finalizeCreation(result.getVersionId(), updatedDate, result.getEtag());
				} else {
					throw new Exception("Backend returned null result");
				}							
			} else {
				//No Callable, so no result, just save the entity as given.
				savedEntity.setLastUpdateTimestamp(new Date());
				savedEntity.seteTag("");
			}
			
			//Update metadata post-call
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {				
				Entities.mergeDirect(savedEntity);

				try {
				    doFullRepair(bucketName, savedEntity.getObjectKey());			    	
				} catch(final Throwable f) {
				    LOG.error("Error during object history consolidation for " + bucketName + "/" + savedEntity.getObjectKey(), f);
				}

				//Update bucket size
				try {
					BucketManagers.getInstance().updateBucketSize(bucketName, savedEntity.getSize());
				} catch(final Throwable f) {
					LOG.warn("Error updating bucket " + bucketName + " total object size. Not failing object put of .", f);
				}	
				
				db.commit();							
			} catch (Exception e) {
				LOG.error("Error saving metadata object:" + bucketName + "/" + object.getObjectKey() + " version " + object.getVersionId());
				throw e;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
			
			//Schedule an async full repair operation.
			final String objectName = savedEntity.getObjectKey();
			try {
			    HISTORY_REPAIR_EXECUTOR.submit(new Runnable() {
				public void run() {
				    try {
					doFullRepair(bucketName, objectName);			    	
				    } catch(final Throwable f) {
					LOG.error("Error during object history consolidation for " + bucketName + "/" + objectName, f);
				    }
				}
			    });
			} catch(final Throwable f) {
				LOG.warn("Error setting object history for " + bucketName + "/" + savedEntity.getObjectKey() + " continuing. Read-repair should fix it later.", f);
			}
			return result;
		} catch(S3Exception e) {
			LOG.error("Error creating object: " + bucketName + "/" + object.getObjectKey());
			try {
				//Call the rollback. It is up to the provider to ensure the rollback is correct for that backend
				if(resourceModifier != null) {
					resourceModifier.rollback(result);
				}
			} catch(Exception ex) {
				LOG.error("Error rolling back object create",ex);
			}
			throw e;
		} catch(Exception e) {
			LOG.error("Error creating object: " + bucketName + "/" + object.getObjectKey());
			
			try {
				if(resourceModifier != null) {
					resourceModifier.rollback(result);
				}
			} catch(Exception ex) {
				LOG.error("Error rolling back object create",ex);
			}			
			throw new InternalErrorException(object.getBucketName() + "/" + object.getObjectKey());
		}
	}
	
	@Override
	public <T extends SetRESTObjectAccessControlPolicyResponseType, F> T setAcp(ObjectEntity object, AccessControlPolicy acp, CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException {
		T result = null;
		try {			
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				if(resourceModifier != null) {
					result = resourceModifier.call();
				}
				
				//Do record swap if existing record is found.
				ObjectEntity extantEntity = null;
				extantEntity = Entities.merge(object);
				extantEntity.setAcl(acp);
				db.commit();
				return result;
			} catch (Exception e) {
				LOG.error("Error updating ACP on object " + object.getBucketName() + "/" + object.getObjectKey() + "?versionId=" + object.getVersionId());
				throw e;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(S3Exception e) {
			LOG.error("Error setting ACP on backend for object: " + object.getBucketName() + "/" + object.getObjectKey());
			try {
				//Call the rollback. It is up to the provider to ensure the rollback is correct for that backend
				if(resourceModifier != null) {
					resourceModifier.rollback(result);
				}
			} catch(Exception ex) {
				LOG.error("Error rolling back object ACP put",ex);
			}
			throw e;
		} catch(Exception e) {
			LOG.error("Error setting ACP on backend for object: " + object.getBucketName() + "/" + object.getObjectKey());
			
			try {
				if(resourceModifier != null) {
					resourceModifier.rollback(result);
				}
			} catch(Exception ex) {
				LOG.error("Error rolling back object ACP put",ex);
			}			
			throw new InternalErrorException(object.getBucketName() + "/" + object.getObjectKey() + "?versionId=" + object.getVersionId());
		}
	}


	@Override
	public PaginatedResult<ObjectEntity> listPaginated(String bucketName, int maxKeys, String prefix, String delimiter, String startKey) throws Exception {
		return listVersionsPaginated(bucketName, maxKeys, prefix, delimiter, startKey, null, true);
		
	}

	@Override
	public PaginatedResult<ObjectEntity> listVersionsPaginated(String bucketName,
			int maxEntries,
			String prefix,
			String delimiter,
			String fromKeyMarker,
			String fromVersionId,
			boolean latestOnly) throws Exception {
		
		EntityTransaction db = Entities.get(ObjectEntity.class);
		try {
			PaginatedResult<ObjectEntity> result = new PaginatedResult<ObjectEntity>();
			HashSet<String> commonPrefixes = new HashSet<String>(); //set of common prefixes found
			
			//Include zero since 'istruncated' is still valid
			if (maxEntries >= 0) {
				final int queryStrideSize = maxEntries+ 1;
				ObjectEntity searchObj = new ObjectEntity();
				searchObj.setBucketName(bucketName);

				//Return latest version, so exclude delete markers as well.
				//This makes listVersion act like listObjects
				if(latestOnly) {
					searchObj.setDeleted(false);
					searchObj.setIsLatest(true);
				}
				
				Criteria objCriteria = Entities.createCriteria(ObjectEntity.class);
				objCriteria.setReadOnly(true);
				objCriteria.setFetchSize(queryStrideSize);
				objCriteria.add(Example.create(searchObj));
				objCriteria.add(ObjectEntity.QueryHelpers.getNotPendingRestriction());
				objCriteria.add(ObjectEntity.QueryHelpers.getNotDeletingRestriction());
				objCriteria.add(ObjectEntity.QueryHelpers.getNotSnapshotRestriction());				
				objCriteria.addOrder(Order.asc("objectKey"));
				objCriteria.addOrder(Order.desc("objectModifiedTimestamp"));
				objCriteria.setMaxResults(queryStrideSize);
				
				if (!Strings.isNullOrEmpty(fromKeyMarker)) {
					objCriteria.add(Restrictions.gt("objectKey", fromKeyMarker));
				} else {
					fromKeyMarker = "";
				}
				
				if (!Strings.isNullOrEmpty(fromVersionId)) {
					objCriteria.add(Restrictions.gt("versionId", fromVersionId));
				} else {
					fromVersionId = "";
				}
				
				if (!Strings.isNullOrEmpty(prefix)) {
					objCriteria.add(Restrictions.like("objectKey", prefix,
							MatchMode.START));
				} else {
					prefix = "";
				}
				
				// Ensure not null.
				if (Strings.isNullOrEmpty(delimiter)) {
					delimiter = "";
				}
				
				List<ObjectEntity> objectInfos = null;
				int resultKeyCount = 0;
				String[] parts = null;
				String prefixString = null;
				boolean useDelimiter = !Strings.isNullOrEmpty(delimiter);
				int pages = 0;
				
				// Iterate over result sets of size maxkeys + 1 since
				// commonPrefixes collapse the list, we may examine many more records than maxkeys + 1
				do {				
					parts = null;
					prefixString = null;
					
					//Skip ahead the next page of 'queryStrideSize' results.
					objCriteria.setFirstResult(pages++*queryStrideSize);
					
					objectInfos = (List<ObjectEntity>) objCriteria.list();
					if(objectInfos == null) {
						//nothing to do.
						break;
					}
					
					for (ObjectEntity objectRecord : objectInfos) {
						if (useDelimiter) {
							// Check if it will get aggregated as a commonprefix
							parts = objectRecord.getObjectKey().substring(prefix.length()).split(delimiter);
							if (parts.length > 1) {
								prefixString = prefix + delimiter + parts[0] + delimiter;
								if (!commonPrefixes.contains(prefixString)) {
									if (resultKeyCount == maxEntries) {
										// This is a new record, so we know we're truncating if this is true
										result.setIsTruncated(true);
										resultKeyCount++;
										break;
									} else {
										//Add it to the common prefix set
										commonPrefixes.add(prefixString);
										result.lastEntry = prefixString;
										// count the unique commonprefix as a single return entry
										resultKeyCount++;
									}
								} else {
									//Already have this prefix, so skip
								}
								continue;
							}
						}
						
						if (resultKeyCount == maxEntries) {
							// This is a new (non-commonprefix) record, so
							// we know we're truncating							
							result.setIsTruncated(true);
							resultKeyCount++;
							break;
						}
						
						result.entityList.add(objectRecord);
						result.lastEntry = objectRecord;
						resultKeyCount++;
					}
					
					if (resultKeyCount <= maxEntries && objectInfos.size() <= maxEntries) {
						break;
					}					
				} while (resultKeyCount <= maxEntries);
				
				// Sort the prefixes from the hashtable and add to the reply
				if (commonPrefixes != null) {	
					result.getCommonPrefixes().addAll(commonPrefixes);
					Collections.sort(result.getCommonPrefixes());
				}
			} else {
				throw new IllegalArgumentException("MaxKeys must be positive integer");
			}
			
			return result;
		} catch(Exception e) {
			LOG.error("Error generating paginated object list of bucket " + bucketName, e);
			throw e;
		} finally {
			db.rollback();
		}
	}

}
