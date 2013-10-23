/*************************************************************************
 * Copyright 2009-2012 Eucalyptus Systems, Inc.
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
 *
 * This file may incorporate work covered under the following copyright
 * and permission notice:
 *
 *   Software License Agreement (BSD License)
 *
 *   Copyright (c) 2008, Regents of the University of California
 *   All rights reserved.
 *
 *   Redistribution and use of this software in source and binary forms,
 *   with or without modification, are permitted provided that the
 *   following conditions are met:
 *
 *     Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *     Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 *   FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 *   COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 *   INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 *   BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *   LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 *   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 *   LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 *   ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 *   POSSIBILITY OF SUCH DAMAGE. USERS OF THIS SOFTWARE ACKNOWLEDGE
 *   THE POSSIBLE PRESENCE OF OTHER OPEN SOURCE LICENSED MATERIAL,
 *   COPYRIGHTED MATERIAL OR PATENTED MATERIAL IN THIS SOFTWARE,
 *   AND IF ANY SUCH MATERIAL IS DISCOVERED THE PARTY DISCOVERING
 *   IT MAY INFORM DR. RICH WOLSKI AT THE UNIVERSITY OF CALIFORNIA,
 *   SANTA BARBARA WHO WILL THEN ASCERTAIN THE MOST APPROPRIATE REMEDY,
 *   WHICH IN THE REGENTS' DISCRETION MAY INCLUDE, WITHOUT LIMITATION,
 *   REPLACEMENT OF THE CODE SO IDENTIFIED, LICENSING OF THE CODE SO
 *   IDENTIFIED, OR WITHDRAWAL OF THE CODE CAPABILITY TO THE EXTENT
 *   NEEDED TO COMPLY WITH ANY SUCH LICENSES OR RIGHTS.
 ************************************************************************/

package com.eucalyptus.objectstorage;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.log4j.Logger;
import org.apache.tools.ant.util.DateUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.AuthException;
import com.eucalyptus.auth.Permissions;
import com.eucalyptus.auth.policy.PolicySpec;
import com.eucalyptus.auth.principal.Account;
import com.eucalyptus.auth.principal.Principals;
import com.eucalyptus.auth.principal.User;
import com.eucalyptus.component.ComponentIds;
import com.eucalyptus.component.annotation.ServiceOperation;
import com.eucalyptus.component.id.Eucalyptus;
import com.eucalyptus.configurable.ConfigurableClass;
import com.eucalyptus.configurable.ConfigurableProperty;
import com.eucalyptus.configurable.PropertyDirectory;
import com.eucalyptus.context.Context;
import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.NoSuchContextException;
import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.entities.TransactionException;
import com.eucalyptus.objectstorage.bittorrent.Tracker;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.entities.ObjectStorageGatewayInfo;
import com.eucalyptus.objectstorage.entities.S3AccessControlledEntity;
import com.eucalyptus.objectstorage.exceptions.s3.AccessDeniedException;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.NoSuchBucketException;
import com.eucalyptus.objectstorage.msgs.AddObjectResponseType;
import com.eucalyptus.objectstorage.msgs.AddObjectType;
import com.eucalyptus.objectstorage.msgs.CopyObjectResponseType;
import com.eucalyptus.objectstorage.msgs.CopyObjectType;
import com.eucalyptus.objectstorage.msgs.CreateBucketResponseType;
import com.eucalyptus.objectstorage.msgs.CreateBucketType;
import com.eucalyptus.objectstorage.msgs.DeleteBucketResponseType;
import com.eucalyptus.objectstorage.msgs.DeleteBucketType;
import com.eucalyptus.objectstorage.msgs.DeleteObjectResponseType;
import com.eucalyptus.objectstorage.msgs.DeleteObjectType;
import com.eucalyptus.objectstorage.msgs.DeleteVersionResponseType;
import com.eucalyptus.objectstorage.msgs.DeleteVersionType;
import com.eucalyptus.objectstorage.msgs.GetBucketAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.GetBucketAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.GetBucketLocationResponseType;
import com.eucalyptus.objectstorage.msgs.GetBucketLocationType;
import com.eucalyptus.objectstorage.msgs.GetBucketLoggingStatusResponseType;
import com.eucalyptus.objectstorage.msgs.GetBucketLoggingStatusType;
import com.eucalyptus.objectstorage.msgs.GetBucketVersioningStatusResponseType;
import com.eucalyptus.objectstorage.msgs.GetBucketVersioningStatusType;
import com.eucalyptus.objectstorage.msgs.GetObjectAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.GetObjectAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.GetObjectExtendedResponseType;
import com.eucalyptus.objectstorage.msgs.GetObjectExtendedType;
import com.eucalyptus.objectstorage.msgs.GetObjectResponseType;
import com.eucalyptus.objectstorage.msgs.GetObjectStorageConfigurationResponseType;
import com.eucalyptus.objectstorage.msgs.GetObjectStorageConfigurationType;
import com.eucalyptus.objectstorage.msgs.GetObjectType;
import com.eucalyptus.objectstorage.msgs.HeadBucketResponseType;
import com.eucalyptus.objectstorage.msgs.HeadBucketType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsResponseType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType;
import com.eucalyptus.objectstorage.msgs.ListBucketResponseType;
import com.eucalyptus.objectstorage.msgs.ListBucketType;
import com.eucalyptus.objectstorage.msgs.ListVersionsResponseType;
import com.eucalyptus.objectstorage.msgs.ListVersionsType;
import com.eucalyptus.objectstorage.msgs.ObjectStorageRequestType;
import com.eucalyptus.objectstorage.msgs.PostObjectResponseType;
import com.eucalyptus.objectstorage.msgs.PostObjectType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineResponseType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineType;
import com.eucalyptus.objectstorage.msgs.PutObjectType;
import com.eucalyptus.objectstorage.msgs.SetBucketAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetBucketAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusResponseType;
import com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusType;
import com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusResponseType;
import com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusType;
import com.eucalyptus.objectstorage.msgs.SetObjectAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetObjectAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationResponseType;
import com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationType;
import com.eucalyptus.objectstorage.policy.AdminOverrideAllowed;
import com.eucalyptus.objectstorage.policy.RequiresACLPermission;
import com.eucalyptus.objectstorage.policy.RequiresPermission;
import com.eucalyptus.objectstorage.policy.ResourceType;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.storage.msgs.s3.BucketListEntry;
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.system.Ats;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.util.Lookups;
import com.google.common.base.Function;
import com.google.common.base.Strings;

import edu.ucsb.eucalyptus.msgs.BaseDataChunk;
import edu.ucsb.eucalyptus.msgs.ComponentProperty;
import edu.ucsb.eucalyptus.util.SystemUtil;

/**
 * Operation handler for the ObjectStorageGatewayImpl. Main point of entry
 * This class handles user and system requests.
 *
 */
public class ObjectStorageGatewayImpl implements ObjectStorageGateway {
	private static Logger LOG = Logger.getLogger( ObjectStorageGatewayImpl.class );

	private static ObjectStorageProviderClient ospClient = null;
	protected static ConcurrentHashMap<String, ChannelBuffer> streamDataMap = new ConcurrentHashMap<String, ChannelBuffer>();

	public ObjectStorageProviderClient getClient() {
		return ospClient;
	}

	public static void checkPreconditions() throws EucalyptusCloudException, ExecutionException {}

	/**
	 * Configure 
	 */
	public static void configure() {		
		synchronized(ObjectStorageGatewayImpl.class) {
			if(ospClient == null) {		
				ObjectStorageGatewayInfo osgInfo = ObjectStorageGatewayInfo.getObjectStorageGatewayInfo();
				try {
					ospClient = ObjectStorageProviders.getInstance();
				} catch (Exception ex) {
					LOG.error (ex);
				}
			}
		}

		String limits = System.getProperty(ObjectStorageProperties.USAGE_LIMITS_PROPERTY);
		if(limits != null) {
			ObjectStorageProperties.shouldEnforceUsageLimits = Boolean.parseBoolean(limits);
		}
		try {
			ospClient.check();
		} catch(EucalyptusCloudException ex) {
			LOG.error("Error initializing walrus", ex);
			SystemUtil.shutdownWithError(ex.getMessage());
		}

		//Disable torrents
		//Tracker.initialize();
		if(System.getProperty("euca.virtualhosting.disable") != null) {
			ObjectStorageProperties.enableVirtualHosting = false;
		}
		try {
			if (ospClient != null) {
				ospClient.start();
			}
		} catch(EucalyptusCloudException ex) {
			LOG.error("Error starting storage backend: " + ex);
		}		
	}

	public ObjectStorageGatewayImpl() {}

	public static void enable() throws EucalyptusCloudException {
		ospClient.enable();
	}

	public static void disable() throws EucalyptusCloudException {		
		ospClient.disable();

		//flush the data stream buffer, disconnect clients.
		streamDataMap.clear();
	}

	public static void check() throws EucalyptusCloudException {
		ospClient.check();
	}

	public static void stop() throws EucalyptusCloudException {
		ospClient.stop();
		Tracker.die();
		ObjectStorageProperties.shouldEnforceUsageLimits = true;
		ObjectStorageProperties.enableVirtualHosting = true;

		//Be sure it's empty
		streamDataMap.clear();
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#UpdateObjectStorageConfiguration(com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationType)
	 */
	@Override
	public UpdateObjectStorageConfigurationResponseType UpdateObjectStorageConfiguration(UpdateObjectStorageConfigurationType request) throws EucalyptusCloudException {
		UpdateObjectStorageConfigurationResponseType reply = (UpdateObjectStorageConfigurationResponseType) request.getReply();
		if(ComponentIds.lookup(Eucalyptus.class).name( ).equals(request.getEffectiveUserId()))
			throw new AccessDeniedException("Only admin can change walrus properties.");
		if(request.getProperties() != null) {
			for(ComponentProperty prop : request.getProperties()) {
				LOG.info("ObjectStorage property: " + prop.getDisplayName() + " Qname: " + prop.getQualifiedName() + " Value: " + prop.getValue());
				try {
					ConfigurableProperty entry = PropertyDirectory.getPropertyEntry(prop.getQualifiedName());
					//type parser will correctly covert the value
					entry.setValue(prop.getValue());
				} catch (IllegalAccessException e) {
					LOG.error(e, e);
				}
			}
		}
		String name = request.getName();
		ospClient.check();
		return reply;
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetObjectStorageConfiguration(com.eucalyptus.objectstorage.msgs.GetObjectStorageConfigurationType)
	 */
	@Override
	public GetObjectStorageConfigurationResponseType GetObjectStorageConfiguration(GetObjectStorageConfigurationType request) throws EucalyptusCloudException {
		GetObjectStorageConfigurationResponseType reply = (GetObjectStorageConfigurationResponseType) request.getReply();
		ConfigurableClass configurableClass = ObjectStorageGatewayInfo.class.getAnnotation(ConfigurableClass.class);
		if(configurableClass != null) {
			String prefix = configurableClass.root();
			reply.setProperties((ArrayList<ComponentProperty>) PropertyDirectory.getComponentPropertySet(prefix));
		}
		return reply;
	}

	@ServiceOperation
	public enum HandleFirstChunk implements Function<PutObjectType, Object> {
		INSTANCE;

		@Override
		public Object apply(PutObjectType request) {
			LOG.debug("Processing PutObject request (direct dispatch) " + request.toString() + " id: " + request.getCorrelationId());
			ChannelBuffer b =  ChannelBuffers.dynamicBuffer();
			if(!streamDataMap.containsKey(request.getCorrelationId())) {
				byte[] firstChunk = request.getData();
				b.writeBytes(firstChunk);
				streamDataMap.put(request.getCorrelationId(), b);
			} else {
				//This should not happen. CorrelationIds should be unique for each request
				LOG.error("CorrelationId lookup in data map found duplicate. Unexpected error");
				return null;
			}
			try {
				Object o = ospClient.putObject(request, new ChannelBufferStreamingInputStream(b));
				return o;
			} catch (EucalyptusCloudException ex) {
				LOG.error(ex);
				return null;
			}
		}
	}

	@ServiceOperation
	public enum HandleChunk implements Function<BaseDataChunk, Object> {
		INSTANCE;
		private static final int retryCount = 15;
		@Override
		public Object apply(BaseDataChunk chunk) {
			/*
			 * This works because chunks are delivered in-order through netty.
			 */
			String id = chunk.getCorrelationId();
			LOG.debug("Processing data chunk with id: " + id + " Last? " + chunk.isLast());
			try {
				ChannelBuffer writeBuffer = streamDataMap.get(id);
				//TODO: HACK! This should be handoff through a monitor.
				//This is required because there is a race between the first chunk
				//and the first data-only chunk.
				//Hacking around it to make progress on other ops, should revisit -ns
				for (int i=0; i < retryCount; ++i) {
					if (writeBuffer == null) {
						LOG.info("Stream Data Map is empty, retrying: " + (i + 1) + " of " + retryCount);
						Thread.sleep(100);
						writeBuffer = streamDataMap.get(id);
					}
				}
				writeBuffer.writeBytes(chunk.getContent());
			} catch (Exception e) {
				LOG.error(e, e);
			}
			return null;
		}	
	}


	/**
	 * Does the current request have an authenticated user? Or is it anonymous?
	 * @return
	 */
	protected static boolean isUserAnonymous() {
		Context ctx = Contexts.lookup();
		return (ctx.getAccount() == null || ctx.getAccount().equals(Principals.nobodyAccount()));
	}

	/**
	 * Does the current context have admin priviledges
	 * @return
	 */
	protected static boolean isAdminUser() {
		return Contexts.lookup().hasAdministrativePrivileges();
	}

	/**
	 * Evaluates the authorization for the operation requested, evaluates IAM, ACL, and bucket policy (bucket policy not yet supported).
	 * @param request
	 * @param optionalResourceId optional (can be null) explicit resourceId to check. If null, the request is used to get the resource.
	 * @param optionalOwnerId optional (can be null) owner Id for the resource being evaluated.
	 * @param optionalResourceAcl option acl for the requested resource
	 * @param resourceAllocationSize the size for the quota check(s) if applicable
	 * @return
	 */
	protected static <T extends ObjectStorageRequestType> boolean operationAllowed(@Nonnull T request, @Nullable final S3AccessControlledEntity bucketResourceEntity, @Nullable final S3AccessControlledEntity objectResourceEntity, long resourceAllocationSize) throws IllegalArgumentException {
		/*
		 * Process the operation's authz requirements based on the request type annotations
		 */
		Ats requestAuthzProperties = Ats.from(request);
		ObjectStorageProperties.Permission[] requiredBucketACLPermissions = null;
		ObjectStorageProperties.Permission[] requiredObjectACLPermissions = null;
		Boolean allowOwnerOnly = null;
		RequiresACLPermission requiredACLs = requestAuthzProperties.get(RequiresACLPermission.class);
		if(requiredACLs != null) {
			requiredBucketACLPermissions = requiredACLs.bucket();
			requiredObjectACLPermissions = requiredACLs.object();
			allowOwnerOnly = requiredACLs.ownerOnly();
		} else {
			//No ACL annotation is ok, maybe a admin only op
		}
		
		String[] requiredActions = null;
		RequiresPermission perms = requestAuthzProperties.get(RequiresPermission.class);
		if(perms != null) {
			requiredActions = perms.value(); 
		}

		Boolean allowAdmin = (requestAuthzProperties.get(AdminOverrideAllowed.class) != null);
		Boolean allowOnlyAdmin = (requestAuthzProperties.get(AdminOverrideAllowed.class) != null) && requestAuthzProperties.get(AdminOverrideAllowed.class).adminOnly();
		
		//Must have at least one of: admin-only, owner-only, ACL, or IAM.
		if(requiredBucketACLPermissions == null && 
				requiredObjectACLPermissions == null &&
				requiredActions == null &&
				!allowAdmin) {
			//Insufficient permission set on the message type.
			throw new IllegalArgumentException("Insufficient permission annotations on type: " + request.getClass().getName() + " cannot evaluate authorization");
		}
		
		String resourceType = null;
		if(requestAuthzProperties.get(ResourceType.class) != null) {
			resourceType = requestAuthzProperties.get(ResourceType.class).value();
		}
				
		if(allowAdmin && isAdminUser()) {
			//Admin override
			return true;
		}
		
		Account resourceOwnerAccount = null;
		User requestUser = null;
		Account requestAccount = null;
		try {			
			try {
				requestAccount = Contexts.lookup(request.getCorrelationId()).getAccount();
			} catch(NoSuchContextException e) {
				//This is not an expected path, but if no context found use the request credentials itself
				if(!Strings.isNullOrEmpty(request.getEffectiveUserId())) {
					requestAccount = Accounts.lookupAccessKeyById(request.getEffectiveUserId()).getUser().getAccount();
				} else if(!Strings.isNullOrEmpty(request.getAccessKeyID())) {
					requestAccount = Accounts.lookupAccessKeyById(request.getAccessKeyID()).getUser().getAccount();
				}
			}
		} catch (AuthException e) {
			LOG.error("Failed to get user for request, cannot verify authorization: " + e.getMessage(), e);				
			return false;
		}
		
		if(resourceType == null) {
			throw new IllegalArgumentException("No resource type found in request class annotations, cannot process.");
		} else {
			try {
				//Ensure we have the proper resource entities present and get owner info						
				if(PolicySpec.S3_RESOURCE_BUCKET.equals(resourceType)) {
					//Get the bucket owner.
					if(bucketResourceEntity == null) {
						LOG.error("Could not check access for operation due to no bucket resource entity found");
						return false;
					}
					resourceOwnerAccount = Accounts.lookupAccountByCanonicalId(bucketResourceEntity.getOwnerCanonicalId());
				} else if(PolicySpec.S3_RESOURCE_OBJECT.equals(resourceType)) {
					if(objectResourceEntity == null) {
						LOG.error("Could not check access for operation due to no object resource entity found");
						return false;
					}
					resourceOwnerAccount = Accounts.lookupAccountByCanonicalId(objectResourceEntity.getOwnerCanonicalId());
				}
			} catch(AuthException e) {
				LOG.error("Exception caught looking up resource owner. Disallowing operation.",e);
				return false;
			}
		}
		
		//Get the resourceId based on IAM resource type
		String resourceId = null;
		if(resourceId == null ) {
			if(resourceType.equals(PolicySpec.S3_RESOURCE_BUCKET)) {
				resourceId = request.getBucket();
			} else if(resourceType.equals(PolicySpec.S3_RESOURCE_OBJECT)) {
				resourceId = request.getKey();
			}
		}
		
		if(requiredBucketACLPermissions == null && requiredObjectACLPermissions == null) {
			throw new IllegalArgumentException("No requires-permission actions found in request class annotations, cannot process.");
		}

		/* ACL Checks */
		//Is the user's account allowed?
		Boolean aclAllow = false;
		
		if(requiredBucketACLPermissions != null && requiredBucketACLPermissions.length > 0) {
			//Check bucket ACLs
			
			if(bucketResourceEntity == null) {
				//There are bucket ACL requirements but no bucket entity to check. fail.
				//Don't bother with other checks, this is an invalid state
				throw new IllegalArgumentException("Null bucket resource, cannot evaluate bucket ACL");
			}
			
			//Evaluate the bucket ACL, any matching grant gives permission
			for(ObjectStorageProperties.Permission permission : requiredBucketACLPermissions) {
				aclAllow = aclAllow || bucketResourceEntity.can(permission, requestAccount.getCanonicalId());
			}
		}
		
		//Check object ACLs, if any
		if(requiredObjectACLPermissions != null && requiredObjectACLPermissions.length > 0) {
			if(objectResourceEntity == null) {
				//There are object ACL requirements but no object entity to check. fail.
				//Don't bother with other checks, this is an invalid state				
				throw new IllegalArgumentException("Null bucket resource, cannot evaluate bucket ACL");
			}
			for(ObjectStorageProperties.Permission permission : requiredObjectACLPermissions) {
				aclAllow = aclAllow || objectResourceEntity.can(permission, requestAccount.getCanonicalId());
			}
		}

		/* Resource owner only? if so, override any previous acl decisions
		 * It is not expected that owneronly is set as well as other ACL permissions,
		 * Regular owner permissions (READ, WRITE, READ_ACP, WRITE_ACP) are handled by the regular acl checks.
		 * OwnerOnly should be only used for operations not covered by the other Permissions (e.g. logging, or versioning)
		 */
		aclAllow = (allowOwnerOnly ? resourceOwnerAccount.getAccountNumber().equals(requestAccount.getAccountNumber()) : aclAllow);
		
		/* IAM checks for user */		
		Boolean iamAllow = true;
		//Evaluate each iam action required, all must be allowed
		for(String action : requiredActions ) {
			/*Permissions.isAuthorized(vendor, resourceType, resourceName, resourceAccount, action, requestUser);
			Permissions.canAllocate(
					PolicySpec.VENDOR_S3,
					PolicySpec.S3_RESOURCE_BUCKET, "",
					PolicySpec.S3_CREATEBUCKET, ctx.getUser(), 1L)
					*/
			iamAllow = Permissions.isAuthorized(PolicySpec.VENDOR_S3,
					resourceType, resourceId,
					resourceOwnerAccount , action,
					requestUser) && Permissions.canAllocate(
					PolicySpec.VENDOR_S3,
					resourceType, resourceId,
					action, requestUser, resourceAllocationSize);
			//iamAllow = iamAllow && !Lookups.checkPrivilege(action, PolicySpec.VENDOR_S3, resourceType, resourceId, resourceOwnerAccountId);
		}
		
		return aclAllow && iamAllow;
	}
	
	/**
	 * A terse request logging function to log request entry at INFO level.
	 * @param request
	 */
	private <I extends ObjectStorageRequestType>void logRequest(I request) {
		StringBuilder canonicalLogEntry = new StringBuilder("osg handling request:" );
		try {			
			String accnt = null;
			String src = null;
			try {
				Context ctx = Contexts.lookup(request.getCorrelationId());
				accnt = ctx.getAccount().getAccountNumber();
				src = ctx.getRemoteAddress().getHostAddress();
			} catch(Exception e) {
				LOG.warn("Failed context lookup by correlation Id: " + request.getCorrelationId());
			} finally {
				if(Strings.isNullOrEmpty(accnt)) {
					accnt = "unknown";
				}
				if(Strings.isNullOrEmpty(src)) {
					src = "unknown";
				}
			}

			canonicalLogEntry.append(" Operation: " + request.getClass().getSimpleName());
			canonicalLogEntry.append(" Account: " + accnt);
			canonicalLogEntry.append(" Src Ip: " + src);		
			canonicalLogEntry.append(" Bucket: " + request.getBucket());
			canonicalLogEntry.append(" Object: " + request.getKey());
			if(request instanceof GetObjectType) {
				canonicalLogEntry.append(" VersionId: " + ((GetObjectType)request).getVersionId());
			} else if(request instanceof PutObjectType) {
				canonicalLogEntry.append(" ContentMD5: " + ((PutObjectType)request).getContentMD5());
			}		
			LOG.info(canonicalLogEntry.toString());
		} catch(Exception e) {
			LOG.warn("Problem formatting request log entry. Incomplete entry: " + canonicalLogEntry == null ? "null" : canonicalLogEntry.toString(), e);
		}		
	}
		
	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#HeadBucket(com.eucalyptus.objectstorage.msgs.HeadBucketType)
	 */
	@Override
	public HeadBucketResponseType HeadBucket(HeadBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		Bucket bucket = Buckets.INSTANCE.get(request.getBucket(), null);
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());				
		}
		
		if(operationAllowed(request, bucket, null, 0)) {
			return ospClient.headBucket(request);
		} else {
			throw new AccessDeniedException(request.getBucket());			
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#CreateBucket(com.eucalyptus.objectstorage.msgs.CreateBucketType)
	 */
	@Override
	public CreateBucketResponseType CreateBucket(CreateBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		Bucket bucket = Buckets.INSTANCE.get(request.getBucket(), null);
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());			
		}
		
		if(operationAllowed(request, bucket, null, 1)) {
			return ospClient.createBucket(request);
		} else {
			throw new AccessDeniedException(request.getBucket());			
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#DeleteBucket(com.eucalyptus.objectstorage.msgs.DeleteBucketType)
	 */
	@Override
	public DeleteBucketResponseType DeleteBucket(DeleteBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = Buckets.INSTANCE.get(request.getBucket(), null);
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());			
		}
		
		if(operationAllowed(request, bucket, null, 0)) {
			return ospClient.deleteBucket(request);
		} else {
			throw new AccessDeniedException(request.getBucket());			
		}
		
	}
	
	protected ListAllMyBucketsList generateBucketListing(List<Bucket> buckets) {
		ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
		bucketList.setBuckets(new ArrayList<BucketListEntry>());
		for(Bucket b : buckets ) {
			bucketList.getBuckets().add(new BucketListEntry(b.getBucketName(), 
					DateUtils.format(b.getCreationDate().getTime(),DateUtils.ALT_ISO8601_DATE_PATTERN)));
		}
		return bucketList;
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#ListAllMyBuckets(com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType)
	 */
	@Override
	public ListAllMyBucketsResponseType ListAllMyBuckets(ListAllMyBucketsType request) throws EucalyptusCloudException {
		logRequest(request);
		
		if(operationAllowed(request, null, null, 0)) {
			ListAllMyBucketsResponseType response = (ListAllMyBucketsResponseType) request.getReply();
			/*
			 * This is a strictly metadata operation, no backend is hit. The sync of metadata in OSG to backend is done elsewhere asynchronously.
			 */
			String canonicalId = null;
			try {
				canonicalId = Contexts.lookup(request.getCorrelationId()).getAccount().getCanonicalId();
			} catch (NoSuchContextException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				List<Bucket> listing = Buckets.INSTANCE.list(canonicalId, false, null);
				response.setBucketList(generateBucketListing(listing));			
				return response;
			} catch(TransactionException e) {
				throw new InternalErrorException();
			}
		} else {
			throw new AccessDeniedException(request.getBucket());
		}		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.GetBucketAccessControlPolicyType)
	 */
	@Override
	public GetBucketAccessControlPolicyResponseType GetBucketAccessControlPolicy(GetBucketAccessControlPolicyType request) throws EucalyptusCloudException
	{
		logRequest(request);
		return ospClient.getBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#PostObject(com.eucalyptus.objectstorage.msgs.PostObjectType)
	 */
	@Override
	public PostObjectResponseType PostObject (PostObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.postObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#PutObjectInline(com.eucalyptus.objectstorage.msgs.PutObjectInlineType)
	 */
	@Override
	public PutObjectInlineResponseType PutObjectInline (PutObjectInlineType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.putObjectInline(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#AddObject(com.eucalyptus.objectstorage.msgs.AddObjectType)
	 */
	@Override
	public AddObjectResponseType AddObject (AddObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.addObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#DeleteObject(com.eucalyptus.objectstorage.msgs.DeleteObjectType)
	 */
	@Override
	public DeleteObjectResponseType DeleteObject (DeleteObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.deleteObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#ListBucket(com.eucalyptus.objectstorage.msgs.ListBucketType)
	 */
	@Override
	public ListBucketResponseType ListBucket(ListBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.listBucket(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.GetObjectAccessControlPolicyType)
	 */
	@Override
	public GetObjectAccessControlPolicyResponseType GetObjectAccessControlPolicy(GetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetBucketAccessControlPolicyType)
	 */
	@Override
	public SetBucketAccessControlPolicyResponseType SetBucketAccessControlPolicy(SetBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetObjectAccessControlPolicyType)
	 */
	@Override
	public SetObjectAccessControlPolicyResponseType SetObjectAccessControlPolicy(SetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetRESTBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyType)
	 */
	@Override
	public SetRESTBucketAccessControlPolicyResponseType SetRESTBucketAccessControlPolicy(SetRESTBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setRESTBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetRESTObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyType)
	 */
	@Override
	public SetRESTObjectAccessControlPolicyResponseType SetRESTObjectAccessControlPolicy(SetRESTObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setRESTObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetObject(com.eucalyptus.objectstorage.msgs.GetObjectType)
	 */
	@Override
	public GetObjectResponseType GetObject(GetObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		ospClient.getObject(request);
		//ObjectGetter getter = new ObjectGetter(request);
		//Threads.lookup(ObjectStorage.class, ObjectStorageGatewayImpl.ObjectGetter.class).limitTo(1).submit(getter);
		return null;
	}

	private class ObjectGetter implements Runnable {
		GetObjectType request;
		public ObjectGetter(GetObjectType request) {
			this.request = request;
		}
		@Override
		public void run() {
			try {
				ospClient.getObject(request);
			} catch (Exception ex) {
				LOG.error(ex, ex);
			}
		}

	}
	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetObjectExtended(com.eucalyptus.objectstorage.msgs.GetObjectExtendedType)
	 */
	@Override
	public GetObjectExtendedResponseType GetObjectExtended(GetObjectExtendedType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getObjectExtended(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetBucketLocation(com.eucalyptus.objectstorage.msgs.GetBucketLocationType)
	 */
	@Override
	public GetBucketLocationResponseType GetBucketLocation(GetBucketLocationType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketLocation(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#CopyObject(com.eucalyptus.objectstorage.msgs.CopyObjectType)
	 */
	@Override
	public CopyObjectResponseType CopyObject(CopyObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.copyObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.GetBucketLoggingStatusType)
	 */
	@Override
	public GetBucketLoggingStatusResponseType GetBucketLoggingStatus(GetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketLoggingStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusType)
	 */
	@Override
	public SetBucketLoggingStatusResponseType SetBucketLoggingStatus(SetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketLoggingStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#GetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.GetBucketVersioningStatusType)
	 */
	@Override
	public GetBucketVersioningStatusResponseType GetBucketVersioningStatus(GetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketVersioningStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#SetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusType)
	 */
	@Override
	public SetBucketVersioningStatusResponseType SetBucketVersioningStatus(SetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketVersioningStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#ListVersions(com.eucalyptus.objectstorage.msgs.ListVersionsType)
	 */
	@Override
	public ListVersionsResponseType ListVersions(ListVersionsType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.listVersions(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageGateway#DeleteVersion(com.eucalyptus.objectstorage.msgs.DeleteVersionType)
	 */
	@Override
	public DeleteVersionResponseType DeleteVersion(DeleteVersionType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.deleteVersion(request);
	}

	public static InetAddress getBucketIp(String bucket) throws EucalyptusCloudException {
		EntityWrapper<Bucket> db = EntityWrapper.get(Bucket.class);
		try {
			Bucket searchBucket = new Bucket(bucket);
			db.getUniqueEscape(searchBucket);
			return ObjectStorageProperties.getWalrusAddress();
		} catch (EucalyptusCloudException ex) {
			throw ex;
		} finally {
			db.rollback();
		}
	}

}
