package com.eucalyptus.objectstorage.tests;

import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.mule.util.UUID;

import com.eucalyptus.objectstorage.CallableWithRollback;
import com.eucalyptus.objectstorage.ObjectManager;
import com.eucalyptus.objectstorage.ObjectManagerFactory;
import com.eucalyptus.objectstorage.PaginatedResult;
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
import com.eucalyptus.objectstorage.util.OSGUtil;

/**
 * Tests the ObjectManager implementations.
 * @author zhill
 *
 */
//Manual testing only, for now
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
		ArrayList<ObjectEntity> testEntities = new ArrayList<ObjectEntity>(entityCount);
		
		CallableWithRollback<PutObjectResponseType, Boolean> fakeModifier = new CallableWithRollback<PutObjectResponseType, Boolean>() {

			@Override
			public PutObjectResponseType call() throws S3Exception, Exception {
				PutObjectResponseType resp = new PutObjectResponseType();
				resp.setLastModified(OSGUtil.dateToFormattedString(new Date()));				
				resp.setVersionId(null); //no versioning
				resp.setEtag(UUID.getUUID().replace("-",""));
				resp.setSize(100L);
				return resp;
			}

			@Override
			public Boolean rollback(PutObjectResponseType arg)
					throws S3Exception, Exception {
				return true;
			}
			
		};
		try {
			//Populate a bunch of fake object entities.
			for(int i = 0 ; i < entityCount ; i++) {
				testEntity = generateFakeValidEntity(bucketName, key + String.valueOf(i), false);
				testEntities.add(testEntity);
				ObjectManagerFactory.getInstance().create(bucketName, testEntity, fakeModifier);
			}
						
			PaginatedResult<ObjectEntity> r = ObjectManagerFactory.getInstance().listPaginated(bucketName, 100, null, null, null);
			
			for(ObjectEntity e : r.getEntityList()) {
				System.out.println(e.toString());
			}
			
			Assert.assertTrue(r.getEntityList().size() == entityCount);
				
		} catch(Exception e) {
			LOG.error("Transaction error", e);
			Assert.fail("Failed getting listing");
			
		} finally {
			for(ObjectEntity obj : testEntities) {
				try {
					ObjectManagerFactory.getInstance().delete(obj, null);
				} catch(Exception e) {
					LOG.error("Error deleteing entity: " + obj.toString(), e);
				}
			}
		}
	}
	
}
