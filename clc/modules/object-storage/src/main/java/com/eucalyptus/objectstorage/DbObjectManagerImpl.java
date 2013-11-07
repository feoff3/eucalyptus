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

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

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
import com.google.common.base.Strings;

/**
 * Database backed implementation of ObjectManager
 *
 */
public class DbObjectManagerImpl implements ObjectManager {
	private static final Logger LOG = Logger.getLogger(DbObjectManagerImpl.class);
	
	@Override
	public <T,F> boolean exists(String bucketName, String objectKey, String versionId,  CallableWithRollback<T, F> resourceModifier) throws TransactionException {
		return get(bucketName, objectKey, versionId) != null;
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
	public List<ObjectEntity> getPendingWrites(String bucketName, String objectKey, String versionId) throws TransactionException {
		try {
			//Return the latest version based on the created date.
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, versionId);
				Criteria search = Entities.createCriteria(ObjectEntity.class);			
				List results = search.add(Example.create(searchExample))
							.addOrder(Order.desc("object_create_date"))
							.add(Restrictions.isNull("object_create_date"))
							.list();
				db.commit();
				return (List<ObjectEntity>)results;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(NoSuchElementException e) {
			//Swallow this exception, return null;
		} catch(Exception e) {
			LOG.error("Error fetching pending write records for object " + bucketName + "/" + objectKey + "?versionId=" + versionId);
			throw e;
		}
		
		return null;
	}

	@Override
	public ObjectEntity get(String bucketName, String objectKey, String versionId) throws TransactionException {
		try {
			//Return the latest version based on the created date.
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				ObjectEntity searchExample = new ObjectEntity(bucketName, objectKey, versionId);
				Criteria search = Entities.createCriteria(ObjectEntity.class);			
				List results = search.add(Example.create(searchExample))
							.addOrder(Order.desc("object_last_modified"))
							.add(ObjectEntity.getNotPendingRestriction())
							.add(ObjectEntity.getNotDeletingRestriction())
							.setMaxResults(1).list();
				db.commit();
				if(results == null || results.size() < 1) {
					return null;
				} else if(results.size() >= 1) {
					return (ObjectEntity)results.get(0);
				}
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
			}
		} catch(NoSuchElementException e) {
			//Swallow this exception, return null;
		} catch(Exception e) {
			LOG.error("Error getting object entity for " + bucketName + "/" + objectKey + "?version=" + versionId);
			throw e;
		}
		
		return null;
	}

	@Override
	public <T, F> void delete(ObjectEntity object, CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException {
		T result = null;
						
		if(resourceModifier != null) {
			//Set to 'deleting'			
			if(!object.getDeleted()) {
				try {
					object.setDeleted(true);
					object.setVersionId(null); //remove version Id.
					Transactions.save(object);
				} catch(TransactionException e) {
					throw new InternalErrorException(object.getResourceFullName());
				}
			}
			
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
		
		try {
			Transactions.delete(object);

			//Update bucket size
			BucketManagers.getInstance().updateBucketSize(object.getBucketName(), -object.getSize());
		} catch (TransactionException e) {
			if(e.getCause() instanceof NoSuchElementException) {
				//Nothing to do, not found is okay
			} else {
				LOG.error("Error looking up object:" + object.getBucketName() + "/" + object.getObjectKey() + (object.getVersionId() == null ? "" : "?versionId=" + object.getVersionId()));
				throw e;
			}
		} catch(NoSuchElementException e) {
			//Ok, not found.
		}		
	}

	@Override
	public <T extends PutObjectResponseType, F> T create(String bucketName, ObjectEntity object, CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException {
		T result = null;
		try {
			
			//Persist the new record in the 'pending' state.
			try {
				object = Transactions.saveDirect(object);
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
					
					object.finalizeCreation(result.getVersionId(), updatedDate, result.getEtag());
				} else {
					throw new Exception("Backend returned null result");
				}							
			} else {
				//No Callable, so no result, just save the entity as given.
				object.setLastUpdateTimestamp(new Date());
			}
			
			//Update metadata post-call
			try {
				Transactions.save(object);
				
				//Update bucket size
				try {
					BucketManagers.getInstance().updateBucketSize(bucketName, object.getSize());
				} catch(Exception e) {
					LOG.warn("Error updating bucket " + bucketName + " total object size. Not failing object put of .", e);
				}
				
				return result;
			} catch (Exception e) {
				LOG.error("Error saving metadata object:" + bucketName + "/" + object.getObjectKey() + " version " + object.getVersionId());
				throw e;
			}			
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
	public PaginatedResult<ObjectEntity> listPaginated(String bucketName, int maxKeys, String prefix, String delimiter, String startKey)
			throws TransactionException, Exception {
		return listVersionsPaginated(bucketName, maxKeys, prefix, delimiter, startKey, null, true);
		
	}

	@Override
	public PaginatedResult<ObjectEntity> listVersionsPaginated(String bucketName,
			int maxEntries,
			String prefix,
			String delimiter,
			String fromKeyMarker,
			String fromVersionId,
			boolean latestOnly)
			throws TransactionException, Exception {
		
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
				} else {
				}
				
				Criteria objCriteria = Entities.createCriteria(ObjectEntity.class);
				objCriteria.setReadOnly(true);
				objCriteria.setFetchSize(queryStrideSize);
				objCriteria.add(Example.create(searchObj));
				objCriteria.addOrder(Order.asc("objectKey"));
				objCriteria.setMaxResults(queryStrideSize);
				objCriteria.add(ObjectEntity.getNotPendingRestriction());
				objCriteria.add(ObjectEntity.getNotDeletingRestriction());
				objCriteria.add(ObjectEntity.getNotSnapshotRestriction());
				
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
				String objectKey = null;
				String[] parts = null;
				String prefixString = null;
				boolean useDelimiter = Strings.isNullOrEmpty(delimiter);
				int pages = 0;
				
				// Iterate over result sets of size maxkeys + 1 since
				// commonPrefixes collapse the list, we may examine many more records than maxkeys + 1
				do {
					objectKey = null;
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
						// Check if it will get aggregated as a commonprefix
						if (useDelimiter) {
							parts = objectKey.substring(prefix.length()).split(delimiter);
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
