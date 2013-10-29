package com.eucalyptus.objectstorage;

import java.util.Collections;
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
import com.google.common.base.Strings;

public class DbObjectManagerImpl implements ObjectManager {
	private static final Logger LOG = Logger.getLogger(DbObjectManagerImpl.class);
	
	@Override
	public <T,F> boolean exists(String bucketName, String objectKey, String versionId,  ReversibleOperation<T, F> resourceModifier) throws TransactionException {
		return lookupAndClose(bucketName, objectKey, versionId) != null;
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
	public <T, F> void delete(String bucketName, String objectKey, String versionId,  ReversibleOperation<T, F> resourceModifier) throws S3Exception, TransactionException {	
		
		if(resourceModifier != null) {
			T result = null;		
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
				InternalErrorException intEx = new InternalErrorException(bucketName + "/" + objectKey + (versionId == null ? "" : "?versionId=" + versionId));
				intEx.initCause(e);
				throw intEx;
			}
		}
		
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
	public <T extends PutObjectResponseType, F> T create(String bucketName, ObjectEntity object, ReversibleOperation<T, F> resourceModifier) throws S3Exception, TransactionException {
		T result = null;
		try {
			if(resourceModifier != null) {
				result = resourceModifier.call();
			} else {
				//nothing to call, so just save
			}
			
			EntityTransaction db = Entities.get(ObjectEntity.class);
			try {
				//Do record swap if existing record is found.
				ObjectEntity extantEntity = null;
				extantEntity = Entities.merge(object);
				
				if(result != null) {
					extantEntity.setVersionId(result.getVersionId());
					extantEntity.setSize(result.getSize());
				}
				db.commit();
				return result;
			} catch (Exception e) {
				LOG.error("Error saving metadata object:" + bucketName + "/" + object.getObjectKey() + " version " + object.getVersionId());
				throw e;
			} finally {
				if(db != null && db.isActive()) {
					db.rollback();
				}
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
	public PaginatedResult<ObjectEntity> listPaginated(String bucketName, int maxKeys, String prefix, String delimiter, String startKey)
			throws TransactionException {
		return listVersionsPaginated(bucketName, maxKeys, prefix, delimiter, startKey, null, true);
		
	}

	@Override
	public PaginatedResult<ObjectEntity> listVersionsPaginated(String bucketName,
			int maxVersions,
			String prefix,
			String delimiter,
			String fromKeyMarker,
			String fromVersionId,
			boolean latestOnly)
			throws TransactionException {
		
		
		EntityTransaction db = Entities.get(ObjectEntity.class);
		try {
			PaginatedResult<ObjectEntity> result = new PaginatedResult<ObjectEntity>();
			HashSet<String> commonPrefixes = new HashSet<String>(); //set of common prefixes found
			//Include zero since 'istruncated' is still valid
			if (maxVersions >= 0) {
				final int queryStrideSize = maxVersions+ 1;
				ObjectEntity searchObj = new ObjectEntity();
				searchObj.setBucketName(bucketName);
				searchObj.setLast(latestOnly);				
				//Return latest version, so exclude delete markers as well.
				//This makes listVersion act like listObjects
				if(latestOnly) {			
					searchObj.setDeleted(false);
				} else {
				}
				
				Criteria objCriteria = Entities.createCriteria(ObjectEntity.class);
				objCriteria.add(Example.create(searchObj));
				objCriteria.addOrder(Order.asc("objectKey"));
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
				String objectKey = null;
				String[] parts = null;
				String prefixString = null;
				boolean useDelimiter = Strings.isNullOrEmpty(delimiter);
				
				// Iterate over result sets of size maxkeys + 1 since
				// commonPrefixes collapse the list, we may examine many more records than maxkeys + 1
				do {
					objectKey = null;
					parts = null;
					prefixString = null;
					if (resultKeyCount > 0) { 
						// Start from end of last round-trip if necessary
						objCriteria.setFirstResult(queryStrideSize);
					}
					
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
									if (resultKeyCount == maxVersions) {
										// This is a new record, so we know we're truncating if this is true
										result.setIsTruncated(true);
										resultKeyCount++;
										break;
									} else {
										//Add it to the common prefix set
										commonPrefixes.add(prefixString);
										// count the unique commonprefix as a single return entry
										resultKeyCount++;
									}
								} else {
									//Already have this prefix, so skip
								}
								continue;
							}
						}
						
						if (resultKeyCount == maxVersions) {
							// This is a new (non-commonprefix) record, so
							// we know we're truncating							
							result.setIsTruncated(true);
							resultKeyCount++;
							break;
						}
						
						result.entityList.add(objectRecord);
						resultKeyCount++;
					}
					
					if (resultKeyCount <= maxVersions && objectInfos.size() <= maxVersions) {
						break;
					}
				} while (resultKeyCount <= maxVersions);
				
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
			throw new InternalError(bucketName);
		} finally {
			db.rollback();
		}
	}

}
