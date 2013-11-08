package com.eucalyptus.objectstorage;

import java.util.List;

import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.eucalyptus.entities.Transactions;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.msgs.DeleteObjectResponseType;
import com.eucalyptus.objectstorage.msgs.DeleteObjectType;
import com.eucalyptus.util.EucalyptusCloudException;

/**
 * Scans metadata for "deleted" objects and removes them from the backend.
 * Many of these may be running concurrently.
 *
 */
public class ObjectReaperTask implements Runnable {
	private static final Logger LOG = Logger.getLogger(ObjectReaperTask.class);
		
	public ObjectReaperTask() {}
		
	//Does a single scan of the DB and reclaims objects it finds in the 'deleting' state
	@Override
	public void run() {
		long startTime = System.currentTimeMillis();
		try {
			LOG.trace("Initiating object reaper task");
			List<ObjectEntity> entitiesToClean = ObjectManagers.getInstance().getFailedOrDeleted();
			DeleteObjectType deleteRequest = null;
			DeleteObjectResponseType deleteResponse = null;
			ObjectStorageProviderClient client = ObjectStorageProviders.getInstance();
			if(client == null) {
				LOG.error("Provider client for ObjectReaperTask is null. Cannot execute.");
				return;
			}
			LOG.trace("Reaping " + entitiesToClean.size() + " objects from backend");
			for(ObjectEntity obj : entitiesToClean) {
				try {
					LOG.trace("Reaping " + obj.getBucketName() + "/" + obj.getObjectUuid() + ".");
					deleteRequest = new DeleteObjectType();
					deleteRequest.setBucket(obj.getBucketName());
					deleteRequest.setKey(obj.getObjectUuid());
					
					try {
						deleteResponse = client.deleteObject(deleteRequest);

						//Object does not exist on backend, remove record
						Transactions.delete(obj);
					} catch(EucalyptusCloudException ex) {
						//Failed. Keep record so we can retry later
						LOG.trace("Error in response from backend on deletion request for object on backend: " + deleteRequest.getBucket() + "/" + deleteRequest.getKey());
					}
				} catch(final Throwable f) {
					LOG.error("Error during object reaper cleanup for object: " + 
							obj.getBucketName() + "/" + obj.getObjectKey() + "versionId=" + obj.getVersionId() + 
							" uuid= " + obj.getObjectUuid(), f);
				}
			}
		} catch(final Throwable f) {
			LOG.error("Error during object reaper execution. Will retry later", f);			
		} finally {
			try {
				long endTime = System.currentTimeMillis();
				LOG.trace("Object reaper execution task took " + Long.toString(endTime - startTime) + "ms to complete");
			} catch( final Throwable f) {
				//Do nothing, but don't allow exceptions out
			}
		}
	}
}
