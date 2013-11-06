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

import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyResponseType;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;

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
	public abstract <T,F> boolean exists(String bucketName, String objectKey, String versionId,  CallableWithRollback<T, F> resourceModifier) throws TransactionException;
	
	/**
	 * Get the entity record, not the content
	 * @param bucketName
	 * @param objectKey
	 * @param versionId
	 * @return
	 * @throws TransactionException
	 */
	public abstract ObjectEntity get(String bucketName, String objectKey, String versionId) throws TransactionException;
	
	/**
	 * List the objects in the given bucket
	 * @param bucketName
	 * @param maxRecordCount
	 * @param prefix
	 * @param delimiter
	 * @param startKey
	 * @return
	 * @throws TransactionException
	 */
	public abstract PaginatedResult<ObjectEntity> listPaginated(String bucketName, int maxRecordCount, String prefix, String delimiter, String startKey) throws TransactionException;

	/**
	 * List the object versions in the given bucket
	 * @param bucketName
	 * @param maxKeys
	 * @param prefix
	 * @param delimiter
	 * @param startKey
	 * @param startVersionId
	 * @param includeDeleteMarkers
	 * @return
	 * @throws TransactionException
	 */
	public abstract PaginatedResult<ObjectEntity> listVersionsPaginated(String bucketName, int maxKeys, String prefix, String delimiter, String startKey, String startVersionId, boolean includeDeleteMarkers) throws TransactionException;
	
	/**
	 * Delete the object entity
	 * @param bucketName
	 * @param objectKey
	 * @param versionId
	 * @param resourceModifier
	 * @throws S3Exception
	 * @throws TransactionException
	 */
	public abstract <T,F> void delete(ObjectEntity object,  CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException;
	
	/**
	 * Uses the provided supplier to get a versionId since that is dependent on the bucket state
	 * @param bucketName
	 * @param object
	 * @param versionIdSupplier
	 */
	public abstract <T extends PutObjectResponseType, F> T create(String bucketName, ObjectEntity object, CallableWithRollback<T,F> resourceModifier) throws S3Exception, TransactionException;
	
	public abstract <T extends SetRESTObjectAccessControlPolicyResponseType, F> T setAcp(ObjectEntity object, AccessControlPolicy acp, CallableWithRollback<T, F> resourceModifier) throws S3Exception, TransactionException;
	
}
