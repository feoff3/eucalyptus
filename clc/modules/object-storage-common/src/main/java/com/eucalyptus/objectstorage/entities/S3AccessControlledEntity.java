package com.eucalyptus.objectstorage.entities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Lob;
import javax.persistence.MappedSuperclass;
import javax.persistence.PostLoad;
import javax.persistence.PrePersist;
import javax.persistence.Transient;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.hibernate.annotations.Type;

import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.AuthException;
import com.eucalyptus.entities.AbstractPersistent;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.storage.msgs.s3.AccessControlList;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.Grant;
import com.eucalyptus.storage.msgs.s3.Grantee;
import com.eucalyptus.storage.msgs.s3.Group;
import com.google.common.base.Function;
import com.google.common.base.Strings;

/**
 * Common handler for authorization for S3 resources that have access controls via ACLs
 * 
 * @author zhill
 *
 */
@MappedSuperclass
public abstract class S3AccessControlledEntity extends AbstractPersistent {
	@Transient
	private static final Logger LOG = Logger.getLogger(S3AccessControlledEntity.class);
	
	// Hold the real owner ID. This is the canonical user Id.
	@Column( name = "owner_canonical_id" )
	protected String ownerCanonicalId;
	
	//Needed for enforcing IAM resource quotas (an extension of IAM for Euca)
	@Column( name = "owner_iam_user_id" )
	protected String ownerIamUserId;

	@Column( name = "acl")
	@Type(type="org.hibernate.type.StringClobType")
	@Lob
	private String acl; //A JSON encoded string that is the acl list.
	
	/**
	 * Map for running actual checks against. Saved to optimize multiple accesses
	 */
	@Transient
	private Map<String, Integer> decodedAcl = null;
	
	//Lob types don't like comparisons with null, so ensure that doesn't happen.
	@PostLoad
	@PrePersist
	public void nullChecks() {
		if(acl == null) {
			acl = "{}"; //empty json
		}
	}
	
	/**
	 * Returns the full name of the resource. e.g. bucket/object for objects or bucket for bucket
	 * @return
	 */
	protected abstract String getResourceFullName();
	
	public String getOwnerCanonicalId() {
		return ownerCanonicalId;
	}

	public void setOwnerCanonicalId(String ownerCanonicalId) {
		this.ownerCanonicalId = ownerCanonicalId;
	}

	public String getOwnerIamUserId() {
		return ownerIamUserId;
	}

	public void setOwnerIamUserId(String ownerIamUserId) {
		this.ownerIamUserId = ownerIamUserId;
	}

	/**
	 * For internal and JPA use only. This returns a json string
	 * @return
	 */
	protected String getAcl() {
		return this.acl;
	}
	
	public synchronized void setAcl(String aclString) {
		this.acl = aclString;
		setDecodedAcl(null);
	}
	
	/**
	 * Set from the messaging type. The owner must be properly set in
	 * the message msgAcl.
	 * @param msgAcl
	 */
	public void setAcl(final AccessControlPolicy msgAcl) throws Exception {
		AccessControlPolicy policy = msgAcl;
		//Add the owner info if not already set
		if(policy.getOwner() == null && this.getOwnerCanonicalId() != null) {
			policy.setOwner(new CanonicalUser(this.getOwnerCanonicalId(),""));
		} else {
			//Already present or can't be set
		}
		
		Map<String, Integer> resultMap = AccessControlPolicyToInternal.INSTANCE.apply(policy);
		ObjectMapper mapper = new ObjectMapper();
		
		synchronized(this) {
			try {
				//Serialize into json
				this.acl = mapper.writeValueAsString(resultMap);
			} catch (IOException e) {
				LOG.error("Error mapping: " + msgAcl.toString() + " to json via mapper");				
				return;
			}
			this.decodedAcl = resultMap;
		}
	}

	/**
	 * Utility method for getting the string version of an access control list
	 * @param acl
	 * @return
	 */
	public static String decodeAclToString(AccessControlList acl) {
		Map<String, Integer> resultMap = AccessControlListToInternal.INSTANCE.apply(acl);
		ObjectMapper mapper = new ObjectMapper();
		
		try {
			//Serialize into json
			return mapper.writeValueAsString(resultMap);
		} catch (IOException e) {
			LOG.error("Error mapping: " + acl.toString() + " to json via mapper");				
			return null;
		}
		
	}
	
	/**
	 * Returns the message-typed version of the acl policy.
	 * Not necessary for evaluation, just presentation to the user
	 * @return
	 * @throws Exception
	 */
	public AccessControlPolicy getAccessControlPolicy() throws Exception {
		return InternalToAccessControlPolicy.INSTANCE.apply(getDecodedAcl());
	}
	
	/**
	 * Authorization check for requested permission by specified user/account via canonicalId
	 * Currently only checks the ACLs.
	 * @param permission
	 * @return
	 */
	public boolean can(ObjectStorageProperties.Permission permission, String canonicalId) {
		try {
			Map<String, Integer> myAcl = getDecodedAcl();
			if(myAcl == null) {
				LOG.error("Got null acl, cannot authorize " + permission.toString());
				return false;
			}
			
			/*
			 * Grants can only allow access, so return true if *any* gives the user access.
			 */
			//Check groups first.
			String groupName;
			for(ObjectStorageProperties.S3_GROUP group : ObjectStorageProperties.S3_GROUP.values()) {
				groupName = group.toString();
				if(myAcl.containsKey(groupName) 
						&& BitmapGrant.allows(permission, myAcl.get(groupName))
						&& ObjectStorageProperties.isUserMember(canonicalId, groupName)) {
					//User is member of group and the group has permission
					return true;
				}
			}
			
			if(myAcl.containsKey(canonicalId) && BitmapGrant.allows(permission,myAcl.get(canonicalId))) {
				//Explicitly granted by canonical Id.
				return true;
			} else {
				//fall through
			}
		} catch(Exception e) {
			LOG.error("Error checking authorization",e);
		}
		return false;
	}
	
	private synchronized void setDecodedAcl(Map<String, Integer> r) {
		this.decodedAcl = r;
	}
	
	private synchronized Map<String, Integer> getDecodedAcl() throws Exception {
		if(decodedAcl == null) {
			try {
				ObjectMapper mapper = new ObjectMapper();
				//Jackson requires this method to handle generics
				this.decodedAcl = mapper.readValue(this.acl, new TypeReference<Map<String, Integer>>(){});
			} catch(Exception e) {
				setDecodedAcl(null);
				LOG.error("Error decoding acl from DB string",e);	
				throw e;
			}
		}	
		return decodedAcl;
	}
	
	/**
	 * Converts internal representation into the messaging representation.
	 * NOTE: does NOT add owner information as the owner is unknown at this level.
	 * The caller must find the owner grant and set it specifically.
	 * @author zhill
	 *
	 */
	public enum InternalToAccessControlPolicy implements Function<Map<String, Integer>, AccessControlPolicy> {
		INSTANCE;
		
		@Override
		public AccessControlPolicy apply(Map<String, Integer> srcMap) {
			AccessControlPolicy policy = new AccessControlPolicy();
			AccessControlList acList = new AccessControlList();
			ArrayList<Grant> grants = new ArrayList<Grant>();
			for(Map.Entry<String,Integer> entry : srcMap.entrySet()) {
				Grantee grantee = new Grantee();				

				//Check if a group uri
				ObjectStorageProperties.S3_GROUP groupId = null;
				try {
					groupId = ObjectStorageProperties.S3_GROUP.valueOf(entry.getKey());							
				} catch(Exception e) {}				
				if(groupId != null) {
					grantee.setGroup(new Group(groupId.toString()));						
				} else {
					grantee.setCanonicalUser(new CanonicalUser(entry.getKey(),""));
				}
				
				for(Grant g : AccountGrantsFromInternal.INSTANCE.apply(entry.getValue())) {
					g.setGrantee(grantee);
					grants.add(g);
				}
			}
			acList.setGrants(grants);
			policy.setAccessControlList(acList);
			return policy;
		}
	}

	/**
	 * Convert the specified AccessControlPolicy type (a messaging type) into
	 * the persistence type
	 * @param msg
	 */	
	public enum AccessControlPolicyToInternal implements Function<AccessControlPolicy, Map<String, Integer>> {
		INSTANCE;	
		
		/**
		 * Returns a valid map of canonicalid -> grant. Returns null if an error occurred.
		 */
		@Override
		public Map<String, Integer> apply(AccessControlPolicy srcPolicy) {
			Map<String, Integer> aclMap = null;
			aclMap = AccessControlListToInternal.INSTANCE.apply(srcPolicy.getAccessControlList());
			
			//Should be an empty map if nothing in the list. null is an error.
			if(aclMap == null) {
				LOG.warn("Got null from AccessControlList mapping, indicates an error");
				return null;
			} else {
				//Add owner
				String ownerCanonicalId = srcPolicy.getOwner().getID();
				if(ownerCanonicalId == null) {
					//Owner is required.
					return null;
				}
				
				//Check for valid owner
				try {
					Accounts.lookupAccountByCanonicalId(ownerCanonicalId);
				} catch(Exception e) {
					//Invalid owner
					LOG.warn("Got invalid owner in AccessControlPolicy during mapping to DB: " + ownerCanonicalId);
					return null;
				}
								
				//Owner always has full control
				aclMap.remove(ownerCanonicalId); //remove any previous entry, only one, FULL_CONTROL even if policy was redundant
				Integer ownerGrant = BitmapGrant.translateToBitmap(ObjectStorageProperties.Permission.FULL_CONTROL);
				aclMap.put(ownerCanonicalId, ownerGrant);
			}
			return aclMap;
		}
	}
	
	/**
	 * Handles just the access control list itself without the additional owner info
	 * @author zhill
	 *
	 */
	public enum AccessControlListToInternal implements Function<AccessControlList, Map<String, Integer>> {
		INSTANCE;	
		
		/**
		 * Returns a valid map of canonicalid -> grant. Returns null if an error occurred.
		 */
		@Override
		public Map<String, Integer> apply(AccessControlList srcList) {
			HashMap<String, Integer> aclMap = new HashMap<String, Integer>();
			String canonicalId = null;
			for(Grant g: srcList.getGrants()) {
				if(g.getGrantee() == null || Strings.isNullOrEmpty(g.getPermission())) {
					//Invalid message.
					return null;
				}
				
				if(!Strings.isNullOrEmpty(g.getGrantee().getCanonicalUser().getID())) {
					//CanonicalId
					try {
						canonicalId = Accounts.lookupAccountByCanonicalId(g.getGrantee().getCanonicalUser().getID()).getCanonicalId();
					} catch (AuthException e) {
						//Invalid canonical Id.
						return null;
					}					
				} else if(!Strings.isNullOrEmpty(g.getGrantee().getEmailAddress())) {
					//Email
					try {
						canonicalId = Accounts.lookupUserByEmailAddress(g.getGrantee().getEmailAddress()).getAccount().getCanonicalId();
					} catch (AuthException e) {
						//Invalid canonical Id.
						return null;
					}
				} else if(g.getGrantee().getGroup() != null) {
					try {
						ObjectStorageProperties.S3_GROUP group = ObjectStorageProperties.S3_GROUP.valueOf(g.getGrantee().getGroup().getUri());
					} catch(IllegalArgumentException e) {
						//Invalid group name
						LOG.warn("Invalid group name when trying to map ACL grantee: " + g.getGrantee().getGroup().getUri());
						return null;
					}
					
					//Group URI, use as canonicalId for now.
					canonicalId = g.getGrantee().getGroup().getUri();					
				}
				
				if(canonicalId == null) {
					return null;
				} else {
					//Add permission to existing grant if exists, or create a new one
					int oldGrant = (aclMap.containsKey(canonicalId) ? aclMap.get(canonicalId) : 0);
					int newGrant = BitmapGrant.add(ObjectStorageProperties.Permission.valueOf(g.getPermission().toUpperCase()), oldGrant);
					if(newGrant != 0) {
						aclMap.put(canonicalId, newGrant);
					} else {
						//skip no-op grants
					}
				}
			}			
			return aclMap;
		}
	}
	
	/**
	 * Converts from BitmapGrant to 1 or more msg Grants (up to 3).
	 * Does not set the grantee on the grants, just the permission(s).
	 * 
	 * Represents the grant(s) for a single canonicalId/group
	 */
	public enum AccountGrantsFromInternal implements Function<Integer, Grant[]> {
		INSTANCE;
		
		@Override
		public Grant[] apply(Integer srcBitmap) {
			Grant[] grants = new Grant[3];
			
			if(BitmapGrant.allows(ObjectStorageProperties.Permission.FULL_CONTROL, srcBitmap)) {
				grants = new Grant[] { new Grant() };				
				grants[0].setPermission(ObjectStorageProperties.Permission.FULL_CONTROL.toString());
			} else {
				
				int i = 0;
				if(BitmapGrant.allows(ObjectStorageProperties.Permission.READ, srcBitmap)) {
					grants[i++].setPermission(ObjectStorageProperties.Permission.READ.toString());
				}
				
				if(BitmapGrant.allows(ObjectStorageProperties.Permission.WRITE, srcBitmap)) {
					grants[i++].setPermission(ObjectStorageProperties.Permission.WRITE.toString());
				}

				if(BitmapGrant.allows(ObjectStorageProperties.Permission.READ_ACP, srcBitmap)) {
					grants[i++].setPermission(ObjectStorageProperties.Permission.READ_ACP.toString());
				}
				
				if(BitmapGrant.allows(ObjectStorageProperties.Permission.WRITE_ACP, srcBitmap)) {
					grants[i++].setPermission(ObjectStorageProperties.Permission.WRITE_ACP.toString());
				}
			}
			return grants;
		}
	}
	
	/**
	 * Implements the mapping of perms to the bitmap
	 * Mapping: Integer => read,write,readAcp,writeAcp in least significant bits
	 * Example:
	 * read = 8, write = 4, ...
	 * 
	 * @author zhill
	 *
	 */
	public enum BitmapGrant {
		INSTANCE;
		
		private static final int readMask = 8; //4th bit from right
		private static final int writeMask = 4; //3rd bit 
		private static final int readACPMask = 2; //2nd bit
		private static final int writeACPMask = 1; //1st bit
	
		/**
		 * Does this grant allow the requested permission?
		 * @param perm
		 * @return
		 */
		public static boolean allows(ObjectStorageProperties.Permission perm, int mapValue) {			
			switch(perm) {
			case FULL_CONTROL:
				return (mapValue & (readMask | writeMask | readACPMask | writeACPMask)) == (readMask | writeMask | readACPMask | writeACPMask);
			case READ:
				return (mapValue & readMask) == readMask;
			case WRITE:
				return (mapValue & writeMask) == writeMask;
			case READ_ACP:
				return (mapValue & readACPMask) == readACPMask;
			case WRITE_ACP:
				return (mapValue & writeACPMask) == writeACPMask;
			}
			return false;
		}

		/**
		 * Sets to the requested permission ONLY. Not additive, replaces previous value.
		 * @param perm
		 */
		public static int translateToBitmap(ObjectStorageProperties.Permission perm) {
			switch(perm) {
			case FULL_CONTROL:
				return (readMask | writeMask | readACPMask | writeACPMask);
			case READ:
				return readMask;
			case WRITE:
				return writeMask;
			case READ_ACP:
				return readACPMask;
			case WRITE_ACP:
				return writeACPMask;
			}
			return 0;
		}
		
		/**
		 * Add the specified permission, non-destructive. Returns a new bitmap that
		 * adds the requested permission to the oldAclBitmap
		 * @param perm
		 */
		public static int add(ObjectStorageProperties.Permission perm, int oldAclBitmap) {
			switch(perm) {
			case FULL_CONTROL:
				return (int)(oldAclBitmap | readMask | writeMask | readACPMask | writeACPMask);
			case READ:
				return (int)(oldAclBitmap | readMask);
			case WRITE:
				return (int)(oldAclBitmap | writeMask);
			case READ_ACP:
				return (int)(oldAclBitmap | readACPMask);
			case WRITE_ACP:
				return (int)(oldAclBitmap | writeACPMask);			
			}
			return 0;
		}
		
		/**
		 * Returns a log-friendly string of the map in b
		 * @param b
		 * @return
		 */
		public static String toLogString(Integer b) {
			StringBuilder sb = new StringBuilder("{");
			sb.append("read=").append(allows(ObjectStorageProperties.Permission.READ, b));
			sb.append(",write=").append(allows(ObjectStorageProperties.Permission.WRITE, b));
			sb.append(",readacp=").append(allows(ObjectStorageProperties.Permission.READ_ACP, b));
			sb.append(",writeacp=").append(allows(ObjectStorageProperties.Permission.WRITE_ACP, b));
			sb.append("}");
			return sb.toString();
		}		
	}
}
	