package com.eucalyptus.objectstorage;

import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
import com.google.common.base.Supplier;

/**
 * Interface for interacting with object metadata (not content directly)
 * @author zhill
 *
 */
public interface ObjectManager {
	
	/**
	 * Count of objects in the given bucket
	 * @param bucketName
	 * @return
	 * @throws TransactionException
	 */
	public long count(String bucketName) throws Exception;

	/**
	 * Does specified object exist
	 * @param bucketName
	 * @param objectKey
	 * @param versionId
	 * @return
	 * @throws TransactionException
	 */
	public abstract boolean exists(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	public abstract ObjectEntity lookupAndClose(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	public abstract PaginatedResult<ObjectEntity> listPaginated(String bucketName, int maxRecordCount, String prefix, String delimiter, String startKey) throws TransactionException;

	public abstract PaginatedResult<ObjectEntity> listVersionsPaginated(String bucketName, int maxKeys, String prefix, String delimiter, String startKey, String startVersionId, boolean includeDeleteMarkers) throws TransactionException;
	
	public abstract void delete(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	/**
	 * Uses the provided supplier to get a versionId since that is dependent on the bucket state
	 * @param bucketName
	 * @param object
	 * @param versionIdSupplier
	 */
	public abstract <T extends PutObjectResponseType, F> T create(String bucketName, ObjectEntity object, ReversibleOperation<T,F> resourceModifier) throws S3Exception, TransactionException;
	
}
