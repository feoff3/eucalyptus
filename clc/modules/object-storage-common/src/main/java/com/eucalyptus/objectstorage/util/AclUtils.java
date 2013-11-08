package com.eucalyptus.objectstorage.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.log4j.Logger;

import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.AuthException;
import com.eucalyptus.storage.msgs.s3.AccessControlList;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.Grant;
import com.eucalyptus.storage.msgs.s3.Grantee;
import com.eucalyptus.storage.msgs.s3.Group;
import com.eucalyptus.util.EucalyptusCloudException;
import com.google.common.base.Function;
import com.google.common.base.Strings;

/**
 * Maps CannedAcls to access control lists
 *
 */
public class AclUtils {
	private static final Logger LOG = Logger.getLogger(AclUtils.class);
	
	/* Use string as key since enum doesn't allow '-' which is in the ACL types. Allows a direct lookup from the msg */
	private static final HashMap<String, Function<OwnerIdPair, List<Grant>>> cannedAclMap = new HashMap<String, Function<OwnerIdPair, List<Grant>>>();

	//A lookup map for quick verification of group uris
	private static final HashMap<String, ObjectStorageProperties.S3_GROUP> groupUriMap = new HashMap<String, ObjectStorageProperties.S3_GROUP>();
	
	static {		
		//Populate the map
		cannedAclMap.put(ObjectStorageProperties.CannedACL.private_only.toString(), PrivateOnlyGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.authenticated_read.toString(), AuthenticatedReadGrantBuilder.INSTANCE);		
		cannedAclMap.put(ObjectStorageProperties.CannedACL.public_read.toString(), PublicReadGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.public_read_write.toString(), PublicReadWriteGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.aws_exec_read.toString(), AwsExecReadGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.bucket_owner_full_control.toString(), BucketOwnerFullControlGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.bucket_owner_read.toString(), BucketOwnerReadGrantBuilder.INSTANCE);
		cannedAclMap.put(ObjectStorageProperties.CannedACL.log_delivery_write.toString(), LogDeliveryWriteGrantBuilder.INSTANCE);
		
		for(ObjectStorageProperties.S3_GROUP g : ObjectStorageProperties.S3_GROUP.values()) {
			groupUriMap.put(g.toString(), g);
		}
	}
	
	/**
	 * Utility class for passing pairs of canonicalIds around without using
	 * something ambiguous like an String-array.
	 * 
	 * @author zhill
	 *
	 */
	public static class OwnerIdPair {
		private String bucketOwner;
		private String objectOwner;
		
		public OwnerIdPair(String bucketOwnerCanonicalId, String objectOwnerCanonicalId) {
			this.bucketOwner = bucketOwnerCanonicalId;
			this.objectOwner = objectOwnerCanonicalId;
		}
		
		public String getBucketOwnerCanonicalId() {
			return this.bucketOwner;
		}
		
		public String getObjectOwnerCanonicalId() {
			return this.objectOwner;
		}
	}

	/**
	 * If the object ownerId is set in the OwnerIdPair then this will assume that
	 * the resource is an object and will return a full-control grant for that user.
	 * If bucket owner only is set then this assumes that the bucket is the resource.
	 * 
	 * @author zhill
	 *
	 */
	protected enum PrivateOnlyGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			ArrayList<Grant> privateGrants = new ArrayList<Grant>();
			Grant ownerFullControl = new Grant();
			Grantee owner = new Grantee();
			String displayName = "";
			String ownerCanonicalId = null;
			if(!Strings.isNullOrEmpty(ownerIds.getObjectOwnerCanonicalId())) {
				ownerCanonicalId = ownerIds.getObjectOwnerCanonicalId();
			} else {
				ownerCanonicalId = ownerIds.getBucketOwnerCanonicalId();
			}
				
			try {				
				displayName = Accounts.lookupAccountByCanonicalId(ownerCanonicalId).getName();
			} catch(AuthException e) {
				displayName = "";
			}
			owner.setCanonicalUser(new CanonicalUser(ownerCanonicalId, displayName));
			owner.setType("CanonicalUser");
			ownerFullControl.setGrantee(owner);
			ownerFullControl.setPermission(ObjectStorageProperties.Permission.FULL_CONTROL.toString());
			privateGrants.add(ownerFullControl);
			return privateGrants;
		}
	};
	
	protected enum AuthenticatedReadGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> authenticatedRead = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			Grantee authenticatedUsers = new Grantee();
			authenticatedUsers.setGroup(new Group(ObjectStorageProperties.S3_GROUP.AUTHENTICATED_USERS_GROUP.toString()));
			Grant authUsersGrant = new Grant();
			authUsersGrant.setPermission(ObjectStorageProperties.Permission.READ.toString());			
			authUsersGrant.setGrantee(authenticatedUsers);
			authenticatedRead.add(authUsersGrant);
			return authenticatedRead;
		}
	};
	
	protected enum PublicReadGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> publicRead = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			Grantee allUsers = new Grantee();
			allUsers.setGroup(new Group(ObjectStorageProperties.S3_GROUP.ALL_USERS_GROUP.toString()));
			Grant allUsersGrant = new Grant();
			allUsersGrant.setPermission(ObjectStorageProperties.Permission.READ.toString());			
			allUsersGrant.setGrantee(allUsers);
			publicRead.add(allUsersGrant);
			return publicRead;
		}
	};
	
	protected enum PublicReadWriteGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> publicReadWrite = PublicReadGrantBuilder.INSTANCE.apply(ownerIds);
			Grantee allUsers = new Grantee();
			allUsers.setGroup(new Group(ObjectStorageProperties.S3_GROUP.ALL_USERS_GROUP.toString()));
			Grant allUsersGrant = new Grant();
			allUsersGrant.setPermission(ObjectStorageProperties.Permission.WRITE.toString());			
			allUsersGrant.setGrantee(allUsers);
			publicReadWrite.add(allUsersGrant);
			return publicReadWrite;
		}
	};
	
	protected enum AwsExecReadGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> awsExecRead = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			Grantee execReadGroup = new Grantee();
			execReadGroup.setGroup(new Group(ObjectStorageProperties.S3_GROUP.AWS_EXEC_READ.toString()));
			Grant execReadGrant = new Grant();
			execReadGrant.setPermission(ObjectStorageProperties.Permission.READ.toString());			
			execReadGrant.setGrantee(execReadGroup);
			awsExecRead.add(execReadGrant);
			return awsExecRead;
		}
	};
	
	protected enum BucketOwnerFullControlGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> bucketOwnerFullControl = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			String canonicalId = ownerIds.getBucketOwnerCanonicalId();
			String displayName = "";
			try {
				displayName = Accounts.lookupAccountByCanonicalId(canonicalId).getName();
			} catch(AuthException e) {
				displayName = "";
			}
			
			Grantee bucketOwner = new Grantee();
			bucketOwner.setCanonicalUser(new CanonicalUser(canonicalId, displayName));
			Grant bucketOwnerGrant = new Grant();
			bucketOwnerGrant.setPermission(ObjectStorageProperties.Permission.FULL_CONTROL.toString());			
			bucketOwnerGrant.setGrantee(bucketOwner);
			bucketOwnerFullControl.add(bucketOwnerGrant);
			return bucketOwnerFullControl;
		}
	};
	
	protected enum BucketOwnerReadGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> bucketOwnerRead = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			String canonicalId = ownerIds.getBucketOwnerCanonicalId();
			String displayName = "";
			try {
				displayName = Accounts.lookupAccountByCanonicalId(canonicalId).getName();
			} catch(AuthException e) {
				displayName = "";
			}
			
			Grantee bucketOwner = new Grantee();
			bucketOwner.setCanonicalUser(new CanonicalUser(canonicalId, displayName));
			Grant bucketOwnerGrant = new Grant();
			bucketOwnerGrant.setPermission(ObjectStorageProperties.Permission.READ.toString());			
			bucketOwnerGrant.setGrantee(bucketOwner);
			bucketOwnerRead.add(bucketOwnerGrant);
			return bucketOwnerRead;
		}
	};
	
	protected enum LogDeliveryWriteGrantBuilder implements Function<OwnerIdPair, List<Grant>> {
		INSTANCE;
		@Override
		public List<Grant> apply(OwnerIdPair ownerIds) {
			List<Grant> logDeliveryWrite = PrivateOnlyGrantBuilder.INSTANCE.apply(ownerIds);
			Grantee allUsers = new Grantee();
			allUsers.setGroup(new Group(ObjectStorageProperties.S3_GROUP.LOGGING_GROUP.toString()));
			Grant allUsersGrant = new Grant();
			allUsersGrant.setPermission(ObjectStorageProperties.Permission.WRITE.toString());	
			allUsersGrant.setGrantee(allUsers);
			logDeliveryWrite.add(allUsersGrant);
			return logDeliveryWrite;
		}
	};

	public static ObjectStorageProperties.S3_GROUP getGroupFromUri(String uri) throws NoSuchElementException {
		ObjectStorageProperties.S3_GROUP foundGroup = groupUriMap.get(uri);
		if(foundGroup == null) {
			throw new NoSuchElementException(uri);
		}
		return foundGroup;
	}
	
	/**
	 * Processes a list by finding all canned-acls and expanding those.
	 * The returned list is a new list that includes all non-canned ACL entries
	 * of the input as well as the expanded grants mapped to canned-acls	
	 * @param msgAcl
	 * @return
	 */
	public static AccessControlList expandCannedAcl(@Nonnull AccessControlList msgAcl, @Nullable final String bucketOwnerCanonicalId, @Nullable final String objectOwnerCanonicalId) throws EucalyptusCloudException {
		if(msgAcl == null) {
			throw new IllegalArgumentException("Null list received");
		}
		
		AccessControlList outputList = new AccessControlList();
		if(outputList.getGrants() == null) {
			//Should be handled by constructor of ACL, but just to be sure
			outputList.setGrants(new ArrayList<Grant>());
		}
		final OwnerIdPair owners = new OwnerIdPair(bucketOwnerCanonicalId, objectOwnerCanonicalId);
		String entryValue = null;
		for(Grant msgGrant : msgAcl.getGrants()) {
			entryValue = msgGrant.getPermission(); //The OSG binding populates the canned-acl in the permission field
			try {
				if(cannedAclMap.containsKey(entryValue)) {
					outputList.getGrants().addAll(cannedAclMap.get(entryValue).apply(owners));
				} else {
					//add to output.
					outputList.getGrants().add(msgGrant);
				}
			} catch(Exception e) {
				//Failed. Stop now
				throw new EucalyptusCloudException("Failed generating the full ACL from canned ACL",e);				
			}
		}
		return outputList;
	}
}
