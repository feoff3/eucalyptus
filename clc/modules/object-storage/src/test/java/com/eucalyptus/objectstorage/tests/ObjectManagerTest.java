package com.eucalyptus.objectstorage.tests;

import java.util.Date;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mule.util.UUID;

import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.ObjectManager;
import com.eucalyptus.objectstorage.ObjectManagerFactory;
import com.eucalyptus.objectstorage.PaginatedResult;
import com.eucalyptus.objectstorage.entities.ObjectEntity;

/**
 * Tests the ObjectManager implementations.
 * @author zhill
 *
 */
//Manual testing only, for now
@Ignore
public class ObjectManagerTest {
	private static final Logger LOG = Logger.getLogger(ObjectManagerTest.class);
	static ObjectManager objectManager = ObjectManagerFactory.getInstance();
	
	protected static String generateVersion() {
		return UUID.getUUID().replace("-", "");
	}
	
	public static ObjectEntity generateFakePendingEntity(String bucket, String key, boolean useVersioning) {
		String versionId = useVersioning ? generateVersion() : null;
		ObjectEntity obj = new ObjectEntity(bucket, key, versionId);
		obj.setInternalKey(UUID.getUUID());
		obj.setObjectModifiedTimestamp(null); //pending
		return obj;
	}
	
	public static ObjectEntity generateFakeValidEntity(String bucket, String key, boolean useVersioning) {
		String versionId = useVersioning ? generateVersion() : null;
		ObjectEntity obj = new ObjectEntity(bucket, key, versionId);
		obj.setInternalKey(UUID.getUUID());
		obj.setObjectModifiedTimestamp(new Date());
		return obj;
	}
	
	public static ObjectEntity generateFakeDeletingEntity(String bucket, String key, boolean useVersioning) {
		String versionId = useVersioning ? generateVersion() : null;
		ObjectEntity obj = new ObjectEntity(bucket, key, versionId);
		obj.setInternalKey(UUID.getUUID());
		obj.setDeleted(true);
		return obj;
	}
	
	@Test
	public void testObjectListing() {
		LOG.info("Testing object listing");
		
		int entityCount = 10;
		ObjectEntity testEntity = null;
		String key = "objectkey";
		String bucketName = "testbucket_" + UUID.getUUID().replace("-", "");
		
		try {
			//Populate a bunch of fake object entities.
			for(int i = 0 ; i < entityCount ; i++) {
				testEntity = generateFakeValidEntity(bucketName, key + String.valueOf(i), false);
				ObjectManagerFactory.getInstance().create(bucketName, testEntity, null);
			}
						
			PaginatedResult r = ObjectManagerFactory.getInstance().listPaginated(bucketName, 100, null, null, null);
				
		} catch(Exception e) {
			LOG.error("Transaction error", e);
			Assert.fail("Failed getting listing");
			
		} finally {
			for(int i = 0; i < entityCount ; i++) {
				
			}
		}
	}
	
	@Test
	public void testObjectGet() {
		
	}
	
	@Test
	public void testObjectCreate() {
		
	}
	
	@Test
	public void testObjectDelete() {
		
	}	

}
