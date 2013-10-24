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
import com.eucalyptus.objectstorage.auth.OSGAuthorizationHandler;
import com.eucalyptus.objectstorage.bittorrent.Tracker;
import com.eucalyptus.objectstorage.entities.Bucket;
import com.eucalyptus.objectstorage.entities.ObjectStorageGatewayInfo;
import com.eucalyptus.objectstorage.entities.S3AccessControlledEntity;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketNameException;
import com.eucalyptus.objectstorage.exceptions.s3.TooManyBucketsException;
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
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.system.Ats;
import com.eucalyptus.util.EucalyptusCloudException;
import com.google.common.base.Function;
import com.google.common.base.Strings;

import edu.ucsb.eucalyptus.msgs.BaseDataChunk;
import edu.ucsb.eucalyptus.msgs.ComponentProperty;
import edu.ucsb.eucalyptus.util.SystemUtil;

/**
 * Operation handler for the ObjectStorageGateway. Main point of entry
 * This class handles user and system requests.
 *
 */
public class ObjectStorageGateway implements ObjectStorageService {
	private static Logger LOG = Logger.getLogger( ObjectStorageGateway.class );

	private static ObjectStorageProviderClient ospClient = null;
	protected static ConcurrentHashMap<String, ChannelBuffer> streamDataMap = new ConcurrentHashMap<String, ChannelBuffer>();
	protected static final String USR_EMAIL_KEY = "email";//lookup for account admins email
	
	public ObjectStorageGateway() {}
	
	public static void checkPreconditions() throws EucalyptusCloudException, ExecutionException {}

	/**
	 * Configure 
	 */
	public static void configure() {		
		synchronized(ObjectStorageGateway.class) {
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
				//TODO: zhill - this seems wrong in check(), should be in enable() ?
				ospClient.start();
			}
		} catch(EucalyptusCloudException ex) {
			LOG.error("Error starting storage backend: " + ex);
		}		
	}

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
		synchronized(ObjectStorageGateway.class) {
			ospClient = null;
		}
		Tracker.die();
		ObjectStorageProperties.shouldEnforceUsageLimits = true;
		ObjectStorageProperties.enableVirtualHosting = true;

		//Be sure it's empty
		streamDataMap.clear();
	}
	
	/**
	 * Check that the bucket is a valid DNS name (or optionally can look like an IP)
	 */
	private boolean checkBucketName(String bucketName) {
		if(!bucketName.matches("^[A-Za-z0-9][A-Za-z0-9._-]+"))
			return false;
		if(bucketName.length() < 3 || bucketName.length() > 255)
			return false;
		String[] addrParts = bucketName.split("\\.");
		boolean ipFormat = true;
		if(addrParts.length == 4) {
			for(String addrPart : addrParts) {
				try {
					Integer.parseInt(addrPart);
				} catch(NumberFormatException ex) {
					ipFormat = false;
					break;
				}
			}
		} else {
			ipFormat = false;
		}		
		if(ipFormat)
			return false;
		return true;
	}
	
	protected String getAccountEmailAddress(String canonicalId) {
		try {
			return Accounts.lookupAccountByCanonicalId(canonicalId).lookupAdmin().getInfo(USR_EMAIL_KEY);
		} catch(Exception e) {
			return "";
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#UpdateObjectStorageConfiguration(com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationType)
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
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObjectStorageConfiguration(com.eucalyptus.objectstorage.msgs.GetObjectStorageConfigurationType)
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
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#HeadBucket(com.eucalyptus.objectstorage.msgs.HeadBucketType)
	 */
	@Override
	public HeadBucketResponseType HeadBucket(HeadBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		try {	
			Bucket bucket = BucketManagerFactory.getInstance().get(request.getBucket(), Contexts.lookup().hasAdministrativePrivileges(), null);

			if(bucket == null) {
				throw new NoSuchBucketException(request.getBucket());				
			}
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {				
				return ospClient.headBucket(request);
			} else {
				throw new AccessDeniedException(request.getBucket());			
			}
		} catch(TransactionException e) {
			LOG.error("Internal error finding bucket " + request.getBucket(), e);
			throw new InternalErrorException(request.getBucket());
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#CreateBucket(com.eucalyptus.objectstorage.msgs.CreateBucketType)
	 */
	@Override
	public CreateBucketResponseType CreateBucket(final CreateBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		String userId = null;
		String canonicalId = null;
		long bucketCount = 0;
		try {
			canonicalId = Contexts.lookup(request.getCorrelationId()).getAccount().getCanonicalId();
			userId = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID()).getUserId();
			bucketCount = BucketManagerFactory.getInstance().countByUser(userId, false, null);
		} catch( AuthException e) {
			LOG.error("Failed userID lookup for accesskeyID " + request.getAccessKeyID());
			throw new AccessDeniedException(request.getBucket());
		} catch(ExecutionException e) {
			LOG.error("Failed getting bucket count for user " + userId);
			//Don't fail the operation, the count may not be important
			bucketCount = 0;
		} catch (NoSuchContextException e) {
			LOG.error("Error finding context to lookup canonical Id of user", e);
			throw new InternalErrorException(request.getBucket());
		}
		
		//Fake entity for auth check
		final S3AccessControlledEntity fakeBucketEntity = new S3AccessControlledEntity() {			
			@Override
			protected String getResourceFullName() {
				return request.getBucket();
			}			
		};
		
		if(OSGAuthorizationHandler.getInstance().operationAllowed(request, fakeBucketEntity, null, bucketCount + 1)) {
			try {
				//Check the validity of the bucket name.				
				if (!checkBucketName(request.getBucket())) {
					throw new InvalidBucketNameException(request.getBucket());
				}

				/* 
				 * This is a secondary check, independent to the iam quota check, based on the configured max bucket count property.
				 * The count does not include "hidden" buckets for snapshots etc since the user has no direct control of those via the s3 endpoint 
				 */
				if (ObjectStorageProperties.shouldEnforceUsageLimits
						&& !Contexts.lookup().hasAdministrativePrivileges() &&					
						BucketManagerFactory.getInstance().countByAccount(canonicalId, true, null) >= ObjectStorageGatewayInfo.getObjectStorageGatewayInfo().getStorageMaxBucketsPerAccount()) {
					throw new TooManyBucketsException(request.getBucket());					
				}
				
				return BucketManagerFactory.getInstance().create(request.getBucket(),
						canonicalId,
						userId,
						S3AccessControlledEntity.decodeAclToString(request.getAccessControlList()), 
						request.getLocationConstraint(),
						new ReversableOperation<CreateBucketResponseType, Boolean>() {
					public CreateBucketResponseType call() throws Exception {
						return ospClient.createBucket(request);
					}
					
					public Boolean rollback(CreateBucketResponseType arg) throws Exception {
						DeleteBucketType deleteRequest = new DeleteBucketType();
						deleteRequest.setBucket(arg.getBucket());					
						try {
							DeleteBucketResponseType response = ospClient.deleteBucket(deleteRequest);
							return response.get_return();
						} catch(Exception e) {
							LOG.error("Rollback (deletebucket) for createbucket " + arg.getBucket() + " failed",e);
							return false;
						}
					}
				});
			} catch(TransactionException e) {
				LOG.error("Error creating bucket metadata. Failing create for bucket " + request.getBucket(), e);
				throw new InternalErrorException(request.getBucket());
			} catch(Exception e) {
				LOG.error("Unknown exception caused failure of CreateBucket for bucket " + request.getBucket(), e);
				throw new InternalErrorException(request.getBucket());
			}
		} else {
			throw new AccessDeniedException(request.getBucket());			
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#DeleteBucket(com.eucalyptus.objectstorage.msgs.DeleteBucketType)
	 */
	@Override
	public DeleteBucketResponseType DeleteBucket(DeleteBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		try {
			Bucket bucket = BucketManagerFactory.getInstance().get(request.getBucket(), false, null);
			if(bucket == null) {
				throw new NoSuchBucketException(request.getBucket());
			}		
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				return ospClient.deleteBucket(request);
			} else {
				throw new AccessDeniedException(request.getBucket());			
			}
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		}
	}
	
	protected static ListAllMyBucketsList generateBucketListing(List<Bucket> buckets) {
		ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
		bucketList.setBuckets(new ArrayList<BucketListEntry>());
		for(Bucket b : buckets ) {
			bucketList.getBuckets().add(new BucketListEntry(b.getBucketName(), 
					DateUtils.format(b.getCreationDate().getTime(),DateUtils.ALT_ISO8601_DATE_PATTERN)));
		}
		return bucketList;
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListAllMyBuckets(com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType)
	 */
	@Override
	public ListAllMyBucketsResponseType ListAllMyBuckets(ListAllMyBucketsType request) throws EucalyptusCloudException {
		logRequest(request);
		
		if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, null, 0)) {
			ListAllMyBucketsResponseType response = (ListAllMyBucketsResponseType) request.getReply();
			/*
			 * This is a strictly metadata operation, no backend is hit. The sync of metadata in OSG to backend is done elsewhere asynchronously.
			 */
			String canonicalId = null;
			try {
				canonicalId = Contexts.lookup(request.getCorrelationId()).getAccount().getCanonicalId();
			} catch (NoSuchContextException e) {
				try {
					canonicalId = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID()).getAccount().getCanonicalId();
				} catch(AuthException ex) {
					LOG.error("Could not retrieve canonicalId for user with accessKey: " + request.getAccessKeyID());
					throw new InternalErrorException();
				}
			}
			try {
				List<Bucket> listing = BucketManagerFactory.getInstance().list(canonicalId, false, null);
				response.setBucketList(generateBucketListing(listing));
				response.setOwner(new CanonicalUser(canonicalId, getAccountEmailAddress(canonicalId)));				
				return response;
			} catch(TransactionException e) {
				throw new InternalErrorException();
			}
		} else {
			throw new AccessDeniedException(request.getBucket());
		}		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.GetBucketAccessControlPolicyType)
	 */
	@Override
	public GetBucketAccessControlPolicyResponseType GetBucketAccessControlPolicy(GetBucketAccessControlPolicyType request) throws EucalyptusCloudException
	{
		logRequest(request);
		return ospClient.getBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#PostObject(com.eucalyptus.objectstorage.msgs.PostObjectType)
	 */
	@Override
	public PostObjectResponseType PostObject (PostObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.postObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#PutObjectInline(com.eucalyptus.objectstorage.msgs.PutObjectInlineType)
	 */
	@Override
	public PutObjectInlineResponseType PutObjectInline (PutObjectInlineType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.putObjectInline(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#AddObject(com.eucalyptus.objectstorage.msgs.AddObjectType)
	 */
	@Override
	public AddObjectResponseType AddObject (AddObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.addObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#DeleteObject(com.eucalyptus.objectstorage.msgs.DeleteObjectType)
	 */
	@Override
	public DeleteObjectResponseType DeleteObject (DeleteObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.deleteObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListBucket(com.eucalyptus.objectstorage.msgs.ListBucketType)
	 */
	@Override
	public ListBucketResponseType ListBucket(ListBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.listBucket(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.GetObjectAccessControlPolicyType)
	 */
	@Override
	public GetObjectAccessControlPolicyResponseType GetObjectAccessControlPolicy(GetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetBucketAccessControlPolicyType)
	 */
	@Override
	public SetBucketAccessControlPolicyResponseType SetBucketAccessControlPolicy(SetBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetObjectAccessControlPolicyType)
	 */
	@Override
	public SetObjectAccessControlPolicyResponseType SetObjectAccessControlPolicy(SetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetRESTBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyType)
	 */
	@Override
	public SetRESTBucketAccessControlPolicyResponseType SetRESTBucketAccessControlPolicy(SetRESTBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setRESTBucketAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetRESTObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyType)
	 */
	@Override
	public SetRESTObjectAccessControlPolicyResponseType SetRESTObjectAccessControlPolicy(SetRESTObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setRESTObjectAccessControlPolicy(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObject(com.eucalyptus.objectstorage.msgs.GetObjectType)
	 */
	@Override
	public GetObjectResponseType GetObject(GetObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		ospClient.getObject(request);
		//ObjectGetter getter = new ObjectGetter(request);
		//Threads.lookup(ObjectStorage.class, ObjectStorageGateway.ObjectGetter.class).limitTo(1).submit(getter);
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
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObjectExtended(com.eucalyptus.objectstorage.msgs.GetObjectExtendedType)
	 */
	@Override
	public GetObjectExtendedResponseType GetObjectExtended(GetObjectExtendedType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getObjectExtended(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketLocation(com.eucalyptus.objectstorage.msgs.GetBucketLocationType)
	 */
	@Override
	public GetBucketLocationResponseType GetBucketLocation(GetBucketLocationType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketLocation(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#CopyObject(com.eucalyptus.objectstorage.msgs.CopyObjectType)
	 */
	@Override
	public CopyObjectResponseType CopyObject(CopyObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.copyObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.GetBucketLoggingStatusType)
	 */
	@Override
	public GetBucketLoggingStatusResponseType GetBucketLoggingStatus(GetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketLoggingStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusType)
	 */
	@Override
	public SetBucketLoggingStatusResponseType SetBucketLoggingStatus(SetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketLoggingStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.GetBucketVersioningStatusType)
	 */
	@Override
	public GetBucketVersioningStatusResponseType GetBucketVersioningStatus(GetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.getBucketVersioningStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusType)
	 */
	@Override
	public SetBucketVersioningStatusResponseType SetBucketVersioningStatus(SetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.setBucketVersioningStatus(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListVersions(com.eucalyptus.objectstorage.msgs.ListVersionsType)
	 */
	@Override
	public ListVersionsResponseType ListVersions(ListVersionsType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.listVersions(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#DeleteVersion(com.eucalyptus.objectstorage.msgs.DeleteVersionType)
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
