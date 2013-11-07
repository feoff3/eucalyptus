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
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.AuthException;
import com.eucalyptus.auth.principal.Account;
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
import com.eucalyptus.objectstorage.entities.ObjectEntity;
import com.eucalyptus.objectstorage.entities.ObjectStorageGatewayInfo;
import com.eucalyptus.objectstorage.entities.S3AccessControlledEntity;
import com.eucalyptus.objectstorage.exceptions.s3.BucketNotEmptyException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidArgumentException;
import com.eucalyptus.objectstorage.exceptions.s3.InvalidBucketNameException;
import com.eucalyptus.objectstorage.exceptions.s3.MalformedACLErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.MissingContentLengthException;
import com.eucalyptus.objectstorage.exceptions.s3.NoSuchKeyException;
import com.eucalyptus.objectstorage.exceptions.s3.NoSuchVersionException;
import com.eucalyptus.objectstorage.exceptions.s3.NotImplementedException;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.eucalyptus.objectstorage.exceptions.s3.TooManyBucketsException;
import com.eucalyptus.objectstorage.exceptions.s3.AccessDeniedException;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.NoSuchBucketException;
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
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
import com.eucalyptus.objectstorage.msgs.PutObjectType;
import com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusResponseType;
import com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusType;
import com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusResponseType;
import com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusType;
import com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyResponseType;
import com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyType;
import com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationResponseType;
import com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationType;
import com.eucalyptus.objectstorage.util.AclUtils;
import com.eucalyptus.objectstorage.util.OSGUtil;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties.VersioningStatus;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.eucalyptus.storage.msgs.s3.BucketListEntry;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.Grant;
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.storage.msgs.s3.ListEntry;
import com.eucalyptus.storage.msgs.s3.LoggingEnabled;
import com.eucalyptus.storage.msgs.s3.PrefixEntry;
import com.eucalyptus.storage.msgs.s3.TargetGrants;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.util.Exceptions;
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
	
	public static void checkPreconditions() throws EucalyptusCloudException, ExecutionException {
		LOG.debug("Checking ObjectStorageGateway preconditions");
		LOG.debug("ObjectStorageGateway Precondition check complete");
	}

	/**
	 * Configure 
	 */
	public static void configure() {		
		synchronized(ObjectStorageGateway.class) {
			if(ospClient == null) {		
				//TODO: zhill - wtf? Is this just priming the config? why is it unused.
				//lol yes. Lame, must be fixed.
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
		LOG.debug("Enabling ObjectStorageGateway");
		ospClient.enable();
		LOG.debug("Enabling ObjectStorageGateway complete");
	}

	public static void disable() throws EucalyptusCloudException {
		LOG.debug("Disabling ObjectStorageGateway");
		ospClient.disable();

		//flush the data stream buffer, disconnect clients.
		streamDataMap.clear();
		LOG.debug("Disabling ObjectStorageGateway complete");
	}

	public static void check() throws EucalyptusCloudException {
		LOG.trace("Checking ObjectStorageGateway");
		ospClient.check();
		LOG.trace("Checking ObjectStorageGateway complete");
	}

	public static void stop() throws EucalyptusCloudException {
		LOG.debug("Checking ObjectStorageGateway preconditions");
		ospClient.stop();
		synchronized(ObjectStorageGateway.class) {
			ospClient = null;
		}
		Tracker.die();
		ObjectStorageProperties.shouldEnforceUsageLimits = true;
		ObjectStorageProperties.enableVirtualHosting = true;

		//Be sure it's empty
		streamDataMap.clear();
		LOG.debug("Checking ObjectStorageGateway preconditions");
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
	
	public static CanonicalUser buildCanonicalUser(Account accnt) {
		return new CanonicalUser(accnt.getCanonicalId(), accnt.getName());
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#UpdateObjectStorageConfiguration(com.eucalyptus.objectstorage.msgs.UpdateObjectStorageConfigurationType)
	 */
	@Override
	public UpdateObjectStorageConfigurationResponseType updateObjectStorageConfiguration(UpdateObjectStorageConfigurationType request) throws EucalyptusCloudException {
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
	public GetObjectStorageConfigurationResponseType getObjectStorageConfiguration(GetObjectStorageConfigurationType request) throws EucalyptusCloudException {
		GetObjectStorageConfigurationResponseType reply = (GetObjectStorageConfigurationResponseType) request.getReply();
		ConfigurableClass configurableClass = ObjectStorageGatewayInfo.class.getAnnotation(ConfigurableClass.class);
		if(configurableClass != null) {
			String prefix = configurableClass.root();
			reply.setProperties((ArrayList<ComponentProperty>) PropertyDirectory.getComponentPropertySet(prefix));
		}
		return reply;
	}

	/* PUT object */
	/*
	 * PUT Object is a core and potentially complex bit of code. Need to reason about it properly. The reason to keep as consistent as possible is
	 * for the ACL checks. Since ACLs can be specified during a PUT, must ensure that the ACL evaluated is for the content actually returned.
	 * 
	 * Assumptions: 
	 * 1. The backend may or may-not support versioning. Eucalyptus's support for versioning is
	 * independent of that of the backend.
	 * 
	 * 2. There will necessarily be a consistency mis-match between the 'latest' version of an object
	 * that the backend has and the view of the objects as seen in the OSG database. This is due to
	 * the ability successfully complete a put in the backend without appropriate rollback even if a
	 * transaction commit fails. Until there is support for DELETE by content MD5 this can occur.
	 * 
	 * 3. We cannot hold a db transaction open for a long time, and certainly not lock many rows just
	 * for a single object operation.
	 * 
	 * Design:
	 * Each object creation, regardless of versioning status, will result in a new DB record being created.
	 * Each object is a unique object put, achieved by appending the correlation-id to the object key. This
	 * ensures uniqueness in the backend and removes content consistency issues.
	 * 
	 * This ensures the proper ACL is attached to the proper version with content and size accordingly. The creation
	 * and update timestamps are intentionally left null to indicate 'in-progress' status of the upload.
	 * 
	 * Send the data to the backend. On successful response, we re-load the entity for that exact operation by remembering
	 * the entity unique ID and update the versionId (if provided), content MD5 and timestamps. Upon commit we know that
	 * the records match exactly and we cannot have overwritten a previous version of metadata.
	 * 
	 * The caller is responsible for interpreting and cleaning multiple records per object. If the bucket is not versioning-enabled
	 * it should provide a way to clean old versions asynchronously. Lookups will return the latest completed upload of an object.
	 * 
	 * Failure cases:
	 * 1. Backend fails to put data: on error response from backend osg will delete the new record and return error to user. This leaves
	 * state of last version unaffected
	 * 
	 * 2. OSG node fails after receiving successful return from backend, after having updated the db. No action, db is consistent.
	 * 
	 * 3. OSG node fails after receiving successful return from backend, BEFORE updating the db for the new entry. Now, orphaned object record
	 * in 'pending' state. MUST MERGE states.
	 * 
	 * 4. OSG node fails after sending request but before getting result. Other OSG nodes (or same one after restart) won't know if PUT succeeded, but
	 * they can see the 'pending' record in the db. MUST MERGE states as before, but detect if backend object was updated first.
	 * 
	 * 
	 * Impact on versioning support:
	 *  1. If versioning not supported on back-end provider: generated uuid for object suffix guarantees object key uniqueness. Can implement versioning
	 * ourselves this way since versions are uniquely identifiable. This can be mapped to a versionId at the OSG if versioning is enabled on the bucket.
	 * 
	 *  2. If versioning supported on back-end provider: still need to generate a uuid for placement in the DB since versionIds are generated by the backend
	 *  and thus cannot be pre-populated in the DB to handle OSG failure.
	 *  
	 */	
	@ServiceOperation (async = false)
	public enum HandleFirstChunk implements Function<PutObjectType, Object> {
		INSTANCE;
			
		@Override
		public Object apply(final PutObjectType request) {
			logRequest(request);
			final ChannelBuffer b =  ChannelBuffers.dynamicBuffer();
			if(!streamDataMap.containsKey(request.getCorrelationId())) {
				byte[] firstChunk = request.getData();
				b.writeBytes(firstChunk);
				streamDataMap.put(request.getCorrelationId(), b);
			} else {
				//This should not happen. CorrelationIds should be unique for each request
				LOG.error("CorrelationId lookup in data map found duplicate. Unexpected error");
				return null;
			}
			
			Bucket bucket = null;
			try {
				User requestUser = Contexts.lookup().getUser();
				try {
					//Get the bucket metadata
					bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
				} catch (TransactionException e) {
					LOG.error(e);
				} catch(NoSuchElementException e) {
					throw new NoSuchBucketException(request.getBucket());
				}

				long newBucketSize = bucket.getBucketSize() == null ? 0 : bucket.getBucketSize();

				//TODO: this should be done in binding.
				if(Strings.isNullOrEmpty(request.getContentLength())) {
					//Not known. Content-Length is required by S3-spec.
					throw new MissingContentLengthException(request.getBucket() + "/" + request.getKey());
				}
				
				long objectSize = -1;
				try {					
					objectSize = Long.parseLong(request.getContentLength());					
					newBucketSize = bucket.getBucketSize() + objectSize;
				} catch(Exception e) {
					LOG.error("Could not parse content length into a long: " + request.getContentLength(), e);
					throw new MissingContentLengthException(request.getBucket() + "/" + request.getKey());
				}

				ObjectEntity objectEntity = new ObjectEntity(request.getBucket(), request.getKey(), null);
				
				//Generate a versionId if necessary based on versioning status of bucket
				String versionId = BucketManagers.getInstance().getVersionId(bucket);

				objectEntity.initializeForCreate(request.getBucket(), 
						request.getKey(), 
						versionId, 
						request.getCorrelationId(),
						objectSize,
						requestUser);
								
				if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, objectEntity, newBucketSize)) {
					//Construct and set the ACP properly, post Auth check so no self-auth can occur even accidentally
					AccessControlPolicy acp = new AccessControlPolicy();
					acp.setOwner(buildCanonicalUser(requestUser.getAccount()));
					acp.setAccessControlList(AclUtils.expandCannedAcl(request.getAccessControlList(), bucket.getOwnerCanonicalId(), requestUser.getAccount().getCanonicalId()));
					objectEntity.setAcl(acp);
					
					final String fullObjectKey = objectEntity.getObjectUuid();
					request.setKey(fullObjectKey); //Ensure the backend uses the new full object name
					
					return ObjectManagers.getInstance().create(request.getBucket(),
							objectEntity,
							new CallableWithRollback<PutObjectResponseType,Boolean>() {

								@Override
								public PutObjectResponseType call() throws S3Exception, Exception {
									return ospClient.putObject(request, new ChannelBufferStreamingInputStream(b));
								}

								@Override
								public Boolean rollback(PutObjectResponseType arg) throws S3Exception,
										Exception {
									DeleteObjectType deleteRequest = new DeleteObjectType();
									deleteRequest.setBucket(request.getBucket());
									deleteRequest.setKey(fullObjectKey);
									DeleteObjectResponseType resp = ospClient.deleteObject(deleteRequest);
									if(resp != null) {
										return true;
									} else {
										return false;
									}
								}						
							}
							);
					
				} else {
					throw new AccessDeniedException(request.getBucket());			
				}
				
			} catch(Exception ex) {
				//Convert since Function() can't throw exceptions
				//Should probably convert this to error response
				throw Exceptions.toUndeclared(ex);
			}
		}
	}

	@ServiceOperation (async = false)
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
	protected static <I extends ObjectStorageRequestType> void logRequest(I request) {
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
	public HeadBucketResponseType headBucket(HeadBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		try {	
			Bucket bucket = BucketManagers.getInstance().get(request.getBucket(), Contexts.lookup().hasAdministrativePrivileges(), null);

			if(bucket == null) {
				throw new NoSuchBucketException(request.getBucket());				
			}
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				HeadBucketResponseType reply = (HeadBucketResponseType) request.getReply();
				reply.setBucket(bucket.getBucketName());
				reply.setStatus(HttpResponseStatus.OK);
				reply.setStatusMessage("OK");
				reply.setTimestamp(new Date());
				return reply;
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
	public CreateBucketResponseType createBucket(final CreateBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		long bucketCount = 0;
		User requestUser = null;
		try {
			requestUser = Contexts.lookup(request.getCorrelationId()).getUser();
			bucketCount = BucketManagers.getInstance().countByUser(requestUser.getUserId(), false, null);
		} catch(ExecutionException e) {
			LOG.error("Failed getting bucket count for user " + requestUser.getUserId());
			//Don't fail the operation, the count may not be important
			bucketCount = 0;
		} catch (NoSuchContextException e) {
			LOG.error("Error finding context to lookup canonical Id of user", e);
			throw new InternalErrorException(request.getBucket());
		}
		
		//Fake entity for auth check
		final S3AccessControlledEntity fakeBucketEntity = new S3AccessControlledEntity() {			
			@Override
			public String getResourceFullName() {
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
						BucketManagers.getInstance().countByAccount(requestUser.getAccount().getCanonicalId(), true, null) >= ObjectStorageGatewayInfo.getObjectStorageGatewayInfo().getStorageMaxBucketsPerAccount()) {
					throw new TooManyBucketsException(request.getBucket());					
				}

				final AccessControlPolicy acPolicy = new AccessControlPolicy();				
				acPolicy.setOwner(buildCanonicalUser(requestUser.getAccount()));
				acPolicy.setAccessControlList(
						AclUtils.expandCannedAcl(request.getAccessControlList(), requestUser.getAccount().getCanonicalId(), null));
				
				String aclString = S3AccessControlledEntity.marshallACPToString(acPolicy);
				if(aclString == null) {
					LOG.error("Unexpectedly got null for acl string. Cannot complete bucket creation with null acl");
					throw new InternalErrorException(request.getBucket());
				}
						
				return BucketManagers.getInstance().create(request.getBucket(),
						requestUser,
						aclString,
						request.getLocationConstraint(),
						new CallableWithRollback<CreateBucketResponseType, Boolean>() {
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
			} catch(S3Exception e) {
				LOG.error("Error creating bucket " + request.getBucket(), e);
				throw e;
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
	public DeleteBucketResponseType deleteBucket(final DeleteBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			//Bucket does not exist, so return success. This is per s3-spec.
			DeleteBucketResponseType reply = (DeleteBucketResponseType) request.getReply();
			reply.setStatus(HttpResponseStatus.NO_CONTENT);
			reply.setStatusMessage("No Content");
			return reply;
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				long objectCount = 0;
				try {
					objectCount = ObjectManagers.getInstance().count(bucket.getBucketName());
				} catch(Exception e) {
					//Bail if we can't confirm bucket is empty.
					LOG.error("Error fetching object count for bucket " + bucket.getBucketName());
					throw new InternalErrorException(bucket.getBucketName());
				}
				
				if(objectCount > 0) {
					throw new BucketNotEmptyException(bucket.getBucketName());
				} else {
					try {
						return BucketManagers.getInstance().delete(bucket, new CallableWithRollback<DeleteBucketResponseType, Boolean>() {
							
							@Override
							public DeleteBucketResponseType call() throws Exception {
								return ospClient.deleteBucket(request);
							}
							
							@Override
							public Boolean rollback(DeleteBucketResponseType arg)
									throws Exception {
								//No rollback for bucket deletion
								return true;
							}					
						});
					} catch(TransactionException e) {
						LOG.error("Transaction error deleting bucket " + request.getBucket(),e);
						throw new InternalErrorException(request.getBucket());
					}
				}				
			} else {
				throw new AccessDeniedException(request.getBucket());			
			}
		}
	}
	
	protected static ListAllMyBucketsList generateBucketListing(List<Bucket> buckets) {
		ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
		bucketList.setBuckets(new ArrayList<BucketListEntry>());
		for(Bucket b : buckets ) {
			bucketList.getBuckets().add(new BucketListEntry(b.getBucketName(),
					OSGUtil.dateToRFC822FormattedString(b.getCreationDate())));
		}
		return bucketList;
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListAllMyBuckets(com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType)
	 */
	@Override
	public ListAllMyBucketsResponseType listAllMyBuckets(ListAllMyBucketsType request) throws EucalyptusCloudException {
		logRequest(request);
		
		if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, null, 0)) {
			ListAllMyBucketsResponseType response = (ListAllMyBucketsResponseType) request.getReply();
			/*
			 * This is a strictly metadata operation, no backend is hit. The sync of metadata in OSG to backend is done elsewhere asynchronously.
			 */
			Account accnt = null;
			try {
				accnt = Contexts.lookup(request.getCorrelationId()).getAccount();
				if(accnt == null) {
					throw new NoSuchContextException();
				}
			} catch (NoSuchContextException e) {
				try {
					accnt = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID()).getAccount();
				} catch(AuthException ex) {
					LOG.error("Could not retrieve canonicalId for user with accessKey: " + request.getAccessKeyID());
					throw new InternalErrorException();
				}
			}
			try {
				List<Bucket> listing = BucketManagers.getInstance().list(accnt.getCanonicalId(), false, null);
				response.setBucketList(generateBucketListing(listing));
				response.setOwner(buildCanonicalUser(accnt));
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
	public GetBucketAccessControlPolicyResponseType getBucketAccessControlPolicy(GetBucketAccessControlPolicyType request) throws EucalyptusCloudException
	{
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting metadata for object " + request.getBucket() + " " + request.getKey());
			throw new InternalErrorException(request.getBucket() + "/?acl");
		} catch(NoSuchElementException e) {
			//bucket not found
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				//Get the listing from the back-end and copy results in.
				GetBucketAccessControlPolicyResponseType reply = (GetBucketAccessControlPolicyResponseType)request.getReply();
				reply.setBucket(request.getBucket());
				try {
					reply.setAccessControlPolicy(bucket.getAccessControlPolicy());
				} catch(Exception e) {
					throw new InternalErrorException(request.getBucket() + "/?acl");
				}
				return reply;
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#PostObject(com.eucalyptus.objectstorage.msgs.PostObjectType)
	 */
	@Override
	public PostObjectResponseType postObject (PostObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		return ospClient.postObject(request);
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#DeleteObject(com.eucalyptus.objectstorage.msgs.DeleteObjectType)
	 */
	@Override
	public DeleteObjectResponseType deleteObject (final DeleteObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		ObjectEntity objectEntity = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
			objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), null);
		} catch(TransactionException e) {
			LOG.error("Error getting bucket metadata for bucket " + request.getBucket());
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//bucket not found
			bucket = null;
			objectEntity = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {				
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, objectEntity, 0)) {
				//Get the listing from the back-end and copy results in.
				try {
					ObjectManagers.getInstance().delete(objectEntity,
							new CallableWithRollback<DeleteObjectResponseType,Boolean>() {
								public DeleteObjectResponseType call() throws S3Exception, Exception {
									return ospClient.deleteObject(request);
								}
								
								public Boolean rollback(DeleteObjectResponseType arg) throws S3Exception, Exception {
									//Can't roll-back a delete
									return true;
								}
							});
					DeleteObjectResponseType reply = (DeleteObjectResponseType) request.getReply();
					return reply;
				} catch (TransactionException e) {
					LOG.error("Transaction error during delete object: " + request.getBucket() + "/" + request.getKey(), e);
					throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
				}
			} else {
				throw new AccessDeniedException(request.getBucket() + "/" + request.getKey());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListBucket(com.eucalyptus.objectstorage.msgs.ListBucketType)
	 */
	@Override
	public ListBucketResponseType listBucket(ListBucketType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket listBucket = null;
		try {
			listBucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting bucket metadata for bucket " + request.getBucket());
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//bucket not found
			listBucket = null;
		}
		
		if(listBucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {				
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, listBucket, null, 0)) {
				//Get the listing from the back-end and copy results in.				
				//return ospClient.listBucket(request);
				ListBucketResponseType reply = (ListBucketResponseType) request.getReply();				
				int maxKeys = 1000;
				reply.setMaxKeys(maxKeys);
				reply.setName(request.getBucket());				
				reply.setDelimiter(request.getDelimiter());
				reply.setMarker(request.getMarker());
				reply.setPrefix(request.getPrefix());								
				reply.setIsTruncated(false);
				
				try {
					if(!Strings.isNullOrEmpty(request.getMaxKeys())) {
						maxKeys = Integer.parseInt(request.getMaxKeys());
					}
				} catch(NumberFormatException e) {
					LOG.error("Failed to parse maxKeys from request properly: " + request.getMaxKeys(), e);
					throw new InvalidArgumentException("MaxKeys");
				}
				
				PaginatedResult<ObjectEntity> result = null;
				try {
					result = ObjectManagers.getInstance().listPaginated(request.getBucket(), maxKeys, request.getPrefix(), request.getDelimiter(), request.getMarker());									
				} catch(Exception e) {
					LOG.error("Error getting object listing for bucket: " + request.getBucket(), e);
					throw new InternalErrorException(request.getBucket());
				}
				
				if(result != null) {
					reply.setContents(new ArrayList<ListEntry>());
					
					for(ObjectEntity obj : result.getEntityList()){
						reply.getContents().add(obj.toListEntry());
					}
					
					for(String s : result.getCommonPrefixes()) {
						reply.getCommonPrefixes().add(new PrefixEntry(s));
					}
					reply.setIsTruncated(result.isTruncated);
					if(result.getLastEntry() instanceof ObjectEntity) {
						reply.setNextMarker(((ObjectEntity)result.getLastEntry()).getObjectKey());
					} else if(result.getLastEntry() instanceof String) {
						reply.setNextMarker((String)result.getLastEntry());
					} else {
						reply.setNextMarker("");
					}
				} else {
					//Do nothing
//					reply.setContents(new ArrayList<ListEntry>());
				}
											
				return reply;
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}
		
	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.GetObjectAccessControlPolicyType)
	 */
	@Override
	public GetObjectAccessControlPolicyResponseType getObjectAccessControlPolicy(GetObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		ObjectEntity objectEntity = null;
		try {
			objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), request.getVersionId());
		} catch(TransactionException e) {
			LOG.error("Error getting metadata for object " + request.getBucket() + " " + request.getKey());
			throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
		} catch(NoSuchElementException e) {
			//bucket not found
			objectEntity = null;
		}
		
		if(objectEntity == null) {
			throw new NoSuchKeyException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, objectEntity, 0)) {
				//Get the listing from the back-end and copy results in.
				GetObjectAccessControlPolicyResponseType reply = (GetObjectAccessControlPolicyResponseType)request.getReply();
				reply.setBucket(request.getBucket());
				try {
					reply.setAccessControlPolicy(objectEntity.getAccessControlPolicy());
				} catch(Exception e) {
					throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
				}
				return reply;
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetRESTBucketAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTBucketAccessControlPolicyType)
	 */
	@Override
	public SetRESTBucketAccessControlPolicyResponseType setRESTBucketAccessControlPolicy(final SetRESTBucketAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting metadata for object " + request.getBucket() + " " + request.getKey());
			throw new InternalErrorException(request.getBucket() + "/?acl");
		} catch(NoSuchElementException e) {
			//bucket not found
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				final String bucketOwnerCanonicalId = bucket.getOwnerCanonicalId();
				String aclString = null;						
				if(request.getAccessControlPolicy() == null || request.getAccessControlPolicy().getAccessControlList() == null) {
					//Can't set to null
					throw new MalformedACLErrorException(request.getBucket() + "/" + request.getKey() + "?acl");
				} else {
					//Expand the acl first
					request.getAccessControlPolicy().setAccessControlList(AclUtils.expandCannedAcl(request.getAccessControlPolicy().getAccessControlList(), bucketOwnerCanonicalId, null));						
					if(request.getAccessControlPolicy() == null || request.getAccessControlPolicy().getAccessControlList() == null) {
						//Something happened in acl expansion.
						LOG.error("Cannot put ACL that does not exist in request");
						throw new InternalErrorException(request.getBucket() + "?acl");
					} else {
						//Add in the owner entry if not present
						if(request.getAccessControlPolicy().getOwner() == null ) {
							request.getAccessControlPolicy().setOwner(new CanonicalUser(bucketOwnerCanonicalId,""));
						}
					}
						
					//Marshal into a string
					aclString = S3AccessControlledEntity.marshallACPToString(request.getAccessControlPolicy());
					if(Strings.isNullOrEmpty(aclString)) {
						throw new MalformedACLErrorException(request.getBucket() + "?acl");
					}
				}			
				try {
					return BucketManagers.getInstance().setAcp(bucket, aclString,
							new CallableWithRollback<SetRESTBucketAccessControlPolicyResponseType, Boolean>() {
						
						@Override
						public SetRESTBucketAccessControlPolicyResponseType call() throws S3Exception, Exception {
							return ospClient.setRESTBucketAccessControlPolicy(request);
						}
						
						@Override
						public Boolean rollback(
								SetRESTBucketAccessControlPolicyResponseType arg)
										throws S3Exception, Exception {
							//TODO: could preserve the old ACP and restore it here for the backend
							return true;
						}
					});
				} catch(TransactionException e) {
					LOG.error("Transaction error updating bucket ACL for bucket " + request.getBucket(),e);
					throw new InternalErrorException(request.getBucket() + "?acl");
				}
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetRESTObjectAccessControlPolicy(com.eucalyptus.objectstorage.msgs.SetRESTObjectAccessControlPolicyType)
	 */
	@Override
	public SetRESTObjectAccessControlPolicyResponseType setRESTObjectAccessControlPolicy(final SetRESTObjectAccessControlPolicyType request) throws EucalyptusCloudException {
		logRequest(request);
		ObjectEntity objectEntity = null;
		Bucket bucket = null;
		try {
			objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), request.getVersionId());
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting metadata for object " + request.getBucket() + " " + request.getKey());
			throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
		} catch(NoSuchElementException e) {
			//bucket not found
			objectEntity = null;
		}
		
		if(objectEntity == null) {
			throw new NoSuchKeyException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, objectEntity, 0)) {
								
				SetRESTObjectAccessControlPolicyResponseType reply = (SetRESTObjectAccessControlPolicyResponseType)request.getReply();
				final String bucketOwnerId = bucket.getOwnerCanonicalId();
				final String objectOwnerId = objectEntity.getOwnerCanonicalId();
				try {
					String aclString = null;
					if(request.getAccessControlPolicy() == null || request.getAccessControlPolicy().getAccessControlList() == null) {
						//Can't set to null
						throw new MalformedACLErrorException(request.getBucket() + "/" + request.getKey() + "?acl");
					} else {
						//Expand the acl first
						request.getAccessControlPolicy().setAccessControlList(AclUtils.expandCannedAcl(request.getAccessControlPolicy().getAccessControlList(), bucketOwnerId, objectOwnerId));						
						if(request.getAccessControlPolicy() == null || request.getAccessControlPolicy().getAccessControlList() == null) {
							//Something happened in acl expansion.
							LOG.error("Cannot put ACL that does not exist in request");
							throw new InternalErrorException(request.getBucket() + "/" + request.getKey() + "?acl");
						} else {
							//Add in the owner entry if not present
							if(request.getAccessControlPolicy().getOwner() == null ) {
								request.getAccessControlPolicy().setOwner(new CanonicalUser(objectOwnerId,""));
							}
						}
							
						//Marshal into a string
						aclString = S3AccessControlledEntity.marshallACPToString(request.getAccessControlPolicy());
						if(Strings.isNullOrEmpty(aclString)) {
							throw new MalformedACLErrorException(request.getBucket() + "/" + request.getKey() + "?acl");
						}
					}
					
					//Get the listing from the back-end and copy results in.
					return ObjectManagers.getInstance().setAcp(objectEntity, request.getAccessControlPolicy(), 
							new CallableWithRollback<SetRESTObjectAccessControlPolicyResponseType, Boolean>() {

								@Override
								public SetRESTObjectAccessControlPolicyResponseType call()
										throws S3Exception, Exception {
									return ospClient.setRESTObjectAccessControlPolicy(request);
									}

								@Override
								public Boolean rollback(
										SetRESTObjectAccessControlPolicyResponseType arg)
										throws S3Exception, Exception {
									return true;
								}							
					});
				} catch(Exception e) {
					LOG.error("Internal error during PUT object?acl for object " + request.getBucket() + "/" + request.getKey(), e);
					throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
				}
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetObject(com.eucalyptus.objectstorage.msgs.GetObjectType)
	 */
	@Override
	public GetObjectResponseType getObject(GetObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		ObjectEntity objectEntity = null;
		try {
			User requestUser = Contexts.lookup().getUser();			
			try {
				//Handle the pass-through
				objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), request.getVersionId());
			} catch (TransactionException e) {
				LOG.error(e);
			} catch(NoSuchElementException e) {
				throw new NoSuchKeyException(request.getBucket() + "/" + request.getKey() + "?versionId=" + request.getVersionId());
			}
			
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, objectEntity, 0)) {				
				ospClient.getObject(request);
				//ObjectGetter getter = new ObjectGetter(request);
				//Threads.lookup(ObjectStorage.class, ObjectStorageGateway.ObjectGetter.class).limitTo(1).submit(getter);
				return null;
			} else {
				throw new AccessDeniedException(request.getBucket() + "/" + request.getKey() + "?versionId=" + request.getVersionId());
			}
			
		} catch(Exception ex) {
			throw new InternalErrorException(request.getBucket() + "/" + request.getKey() + "?versionId=" + request.getVersionId());
		}
	
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
	public GetObjectExtendedResponseType getObjectExtended(GetObjectExtendedType request) throws EucalyptusCloudException {
		logRequest(request);
		ObjectEntity objectEntity = null;
		try {
			User requestUser = Contexts.lookup().getUser();			
			try {
				//Handle the pass-through
				objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), null);
			} catch (TransactionException e) {
				LOG.error(e);
			} catch(NoSuchElementException e) {
				throw new NoSuchBucketException(request.getBucket());
			}
			
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, null, objectEntity, 0)) {				
				ospClient.getObjectExtended(request);
				//return ospClient.getObjectExtended(request);
				return null;
			} else {
				throw new AccessDeniedException(request.getBucket() + "/" + request.getKey());
			}
			
		} catch(Exception ex) {
			throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
		}
		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketLocation(com.eucalyptus.objectstorage.msgs.GetBucketLocationType)
	 */
	@Override
	public GetBucketLocationResponseType getBucketLocation(GetBucketLocationType request) throws EucalyptusCloudException {
		logRequest(request);		
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				GetBucketLocationResponseType reply = (GetBucketLocationResponseType) request.getReply();
				reply.setLocationConstraint(bucket.getLocation());
				reply.setBucket(request.getBucket());
				return reply;
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#CopyObject(com.eucalyptus.objectstorage.msgs.CopyObjectType)
	 */
	@Override
	public CopyObjectResponseType copyObject(CopyObjectType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		ObjectEntity objectEntity = null;
		try {
			objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			objectEntity = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} if(objectEntity == null) {
			throw new NoSuchKeyException(request.getKey());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, objectEntity, 0)) {
				return ospClient.copyObject(request);
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.GetBucketLoggingStatusType)
	 */
	@Override
	public GetBucketLoggingStatusResponseType getBucketLoggingStatus(GetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				GetBucketLoggingStatusResponseType reply = (GetBucketLoggingStatusResponseType) request.getReply();
				LoggingEnabled loggingConfig = new LoggingEnabled();
				if(bucket.getLoggingEnabled()) {
					Bucket targetBucket = null;
					try {
						targetBucket = BucketManagers.getInstance().get(bucket.getTargetBucket(), false, null);
					} catch(Exception e) {
						LOG.error("Error locating target bucket info for bucket " + request.getBucket() + " on target bucket " + bucket.getTargetBucket(), e);
					}
					
					TargetGrants grants = new TargetGrants();
					try {
						grants.setGrants(targetBucket.getAccessControlPolicy().getAccessControlList().getGrants());
					} catch(Exception e) {
						LOG.error("Error populating target grants for bucket " + request.getBucket() + " for target " + targetBucket.getBucketName(),e);
						grants.setGrants(new ArrayList<Grant>());
					}						
					loggingConfig.setTargetBucket(bucket.getTargetBucket());
					loggingConfig.setTargetPrefix(bucket.getTargetPrefix());
					loggingConfig.setTargetGrants(grants);
					reply.setLoggingEnabled(loggingConfig);
				} else {
					//Logging not enabled
					reply.setLoggingEnabled(null);
				}
				
				return reply;
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}
		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetBucketLoggingStatus(com.eucalyptus.objectstorage.msgs.SetBucketLoggingStatusType)
	 */
	@Override
	public SetBucketLoggingStatusResponseType setBucketLoggingStatus(final SetBucketLoggingStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				//TODO: zhill -- add support for this. Not implemented for the tech preview
				throw new NotImplementedException("Bucket Logging");
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}
		
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#GetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.GetBucketVersioningStatusType)
	 */
	@Override
	public GetBucketVersioningStatusResponseType getBucketVersioningStatus(GetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				//Metadata only, don't hit the backend
				GetBucketVersioningStatusResponseType reply = (GetBucketVersioningStatusResponseType)request.getReply();
				reply.setVersioningStatus(bucket.getVersioning());
				reply.setBucket(request.getBucket());
				return reply;
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#SetBucketVersioningStatus(com.eucalyptus.objectstorage.msgs.SetBucketVersioningStatusType)
	 */
	@Override
	public SetBucketVersioningStatusResponseType setBucketVersioningStatus(final SetBucketVersioningStatusType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket bucket = null;
		try {
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//Ok, bucket not found.
			bucket = null;
		}
		
		if(bucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, null, 0)) {
				try {
					final String oldState = bucket.getVersioning();
					final String bucketName = request.getBucket();
					final VersioningStatus newState = VersioningStatus.valueOf(request.getVersioningStatus());
					
					return BucketManagers.getInstance().setVersioning(bucket, 
							newState, 
							new CallableWithRollback<SetBucketVersioningStatusResponseType, Boolean>() {
						
						@Override
						public SetBucketVersioningStatusResponseType call() throws Exception {
							return ospClient.setBucketVersioningStatus(request);
						}
						
						@Override
						public Boolean rollback(SetBucketVersioningStatusResponseType arg) throws Exception {
							SetBucketVersioningStatusType revertRequest = new SetBucketVersioningStatusType();
							revertRequest.setVersioningStatus(oldState);
							revertRequest.setBucket(bucketName);
							try {
								SetBucketVersioningStatusResponseType response = ospClient.setBucketVersioningStatus(revertRequest);							
								if(response != null && response.get_return()) {
									return true;
								}
							} catch(Exception e) {
								LOG.error("Error invoking bucket versioning state rollback from " + request.getVersioningStatus() + " to " + oldState, e);
								return false;
							}
							
							return false;
						}					
					});
				} catch(TransactionException e) {
					LOG.error("Transaction error deleting bucket " + request.getBucket(),e);
					throw new InternalErrorException(request.getBucket());
				}
			} else {		
				throw new AccessDeniedException(request.getBucket());			
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#ListVersions(com.eucalyptus.objectstorage.msgs.ListVersionsType)
	 */
	@Override
	public ListVersionsResponseType listVersions(ListVersionsType request) throws EucalyptusCloudException {
		logRequest(request);
		Bucket listBucket = null;
		try {
			listBucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting bucket metadata for bucket " + request.getBucket());
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//bucket not found
			listBucket = null;
		}
		
		if(listBucket == null) {
			throw new NoSuchBucketException(request.getBucket());
		} else {				
			if(OSGAuthorizationHandler.getInstance().operationAllowed(request, listBucket, null, 0)) {
				//Get the listing from the back-end and copy results in.
				return ospClient.listVersions(request);
			} else {
				throw new AccessDeniedException(request.getBucket());
			}
		}
	}

	/* (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageService#DeleteVersion(com.eucalyptus.objectstorage.msgs.DeleteVersionType)
	 */
	@Override
	public DeleteVersionResponseType deleteVersion(final DeleteVersionType request) throws EucalyptusCloudException {
		logRequest(request);
		ObjectEntity objectEntity = null;
		Bucket bucket = null;
		try {
			objectEntity = ObjectManagers.getInstance().get(request.getBucket(), request.getKey(), request.getVersionid());
			bucket = BucketManagers.getInstance().get(request.getBucket(), false, null);
		} catch(TransactionException e) {
			LOG.error("Error getting metadata for delete version operation on " + request.getBucket() + "/" + request.getKey() + "?version=" + request.getVersionid());
			throw new InternalErrorException(request.getBucket());
		} catch(NoSuchElementException e) {
			//object version not found
			objectEntity = null;
			throw new NoSuchVersionException(request.getBucket() + "/" + request.getKey() + "?versionId=" + request.getVersionid());
		}
		
		if(OSGAuthorizationHandler.getInstance().operationAllowed(request, bucket, objectEntity, 0)) {
			//Get the listing from the back-end and copy results in.
			try {
				final DeleteVersionType backendRequest = (DeleteVersionType)request.regardingUserRequest(request);
				backendRequest.setBucket(request.getBucket());
				backendRequest.setKey(objectEntity.getObjectUuid());
				backendRequest.setVersionid(request.getVersionid());
				
				ObjectManagers.getInstance().delete(objectEntity, new CallableWithRollback<DeleteVersionResponseType, Boolean>() {
					@Override
					public DeleteVersionResponseType call() throws S3Exception,
					Exception {
						//TODO: need to use a different request to handle the internal key
						return ospClient.deleteVersion(request);
					}
					
					@Override
					public Boolean rollback(DeleteVersionResponseType arg)
							throws S3Exception, Exception {
						// TODO Auto-generated method stub
						return null;
					}					
				});
				
				DeleteVersionResponseType reply = (DeleteVersionResponseType)request.getReply();
				return reply;				
			} catch(TransactionException e) {
				LOG.error("Error deleting",e);
				throw new InternalErrorException(request.getBucket() + "/" + request.getKey());
			}
		} else {
			throw new AccessDeniedException(request.getBucket());
		}
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
