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

package com.eucalyptus.objectstorage.providers.s3;

import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListBucketsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.principal.User;
import com.eucalyptus.context.Context;
import com.eucalyptus.context.Contexts;
import com.eucalyptus.context.NoSuchContextException;
import com.eucalyptus.objectstorage.ObjectStorageGateway;
import com.eucalyptus.objectstorage.ObjectStorageProviderClient;
import com.eucalyptus.objectstorage.ObjectStorageProviders.ObjectStorageProviderClientProperty;
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
import com.eucalyptus.objectstorage.msgs.GetObjectType;
import com.eucalyptus.objectstorage.msgs.HeadBucketResponseType;
import com.eucalyptus.objectstorage.msgs.HeadBucketType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsResponseType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType;
import com.eucalyptus.objectstorage.msgs.ListBucketResponseType;
import com.eucalyptus.objectstorage.msgs.ListBucketType;
import com.eucalyptus.objectstorage.msgs.ListVersionsResponseType;
import com.eucalyptus.objectstorage.msgs.ListVersionsType;
import com.eucalyptus.objectstorage.msgs.PostObjectResponseType;
import com.eucalyptus.objectstorage.msgs.PostObjectType;
import com.eucalyptus.objectstorage.msgs.PutObjectResponseType;
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
import com.eucalyptus.objectstorage.util.OSGUtil;
import com.eucalyptus.storage.common.ChunkedDataStream;
import com.eucalyptus.storage.msgs.s3.AccessControlList;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.eucalyptus.storage.msgs.s3.BucketListEntry;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.Grant;
import com.eucalyptus.storage.msgs.s3.Grantee;
import com.eucalyptus.storage.msgs.s3.Group;
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.storage.msgs.s3.ListEntry;
import com.eucalyptus.storage.msgs.s3.MetaDataEntry;
import com.eucalyptus.storage.msgs.s3.PrefixEntry;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.walrus.util.WalrusProperties;
import com.eucalyptus.objectstorage.exceptions.s3.InternalErrorException;
import com.eucalyptus.objectstorage.exceptions.s3.NotImplementedException;
import com.eucalyptus.objectstorage.exceptions.s3.S3Exception;
import com.google.common.base.Strings;

/**
 * Base class for S3-api based backends. Uses the Amazon Java SDK as the client.
 * Can be extended for additional capabilities.
 * 
 * The S3ProviderClient does IAM evaluation prior to dispatching requests to the backend and
 * will validate all results upon receipt of the response from the backend (e.g. for listing buckets).
 * 
 * Current implementation maps all Euca credentials to a single backend s3 credential as configured in
 * {@link S3ProviderConfiguration}. The implication is that this provider will not enforce ACLs, policies,
 * or any separation between Euca-users.
 * 
 */
@ObjectStorageProviderClientProperty("s3")
public class S3ProviderClient extends ObjectStorageProviderClient {
	private static final Logger LOG = Logger.getLogger(S3ProviderClient.class); 
	private static final int CONNECTION_TIMEOUT_MS = 500;
	private static final Boolean USE_HTTPS = false; //TODO: make configurable
	
	protected boolean usePathStyle = true;
	protected AmazonS3Client s3Client = null; //Single shared client. May need to expand this to a pool
	
	public boolean usePathStyle() {
		return usePathStyle;
	}

	public void setUsePathStyle(boolean usePathStyle) {
		this.usePathStyle = usePathStyle;
	}
	
	/**
	 * Returns a usable S3 Client configured to send requests to the currently configured
	 * endpoint with the currently configured credentials.
	 * @return
	 */
	protected AmazonS3Client getS3Client(User requestUser, String requestAWSAccessKeyId) {
		//TODO: this should be enhanced to share clients/use a pool for efficiency.
		if (s3Client == null) {
			synchronized(this) {
				if(s3Client == null) {
					ClientConfiguration config = new ClientConfiguration();
					if(USE_HTTPS) {
						config.setProtocol(Protocol.HTTPS);
					} else {
						config.setProtocol(Protocol.HTTP);
					}
					config.setConnectionTimeout(CONNECTION_TIMEOUT_MS); //very short timeout
					AWSCredentials credentials = mapCredentials(requestUser, requestAWSAccessKeyId);
					AmazonS3Client client = new AmazonS3Client(credentials, config);
					client.setEndpoint(S3ProviderConfiguration.getS3Endpoint());
					S3ClientOptions ops = new S3ClientOptions();
					ops.setPathStyleAccess(this.usePathStyle);
					client.setS3ClientOptions(ops);
					s3Client = client;
				}
			}
		}
		return s3Client;
	}

	/**
	 * Returns the S3 ACL in euca object form. Does not modify the results,
	 * so owner information will be preserved.
	 * @param s3Acl
	 * @return
	 */
	protected static AccessControlPolicy sdkAclToEucaAcl(com.amazonaws.services.s3.model.AccessControlList s3Acl) {
		if(s3Acl == null) { return null; }		
		AccessControlPolicy acp = new AccessControlPolicy();
		
		acp.setOwner(new CanonicalUser(acp.getOwner().getID(), acp.getOwner().getDisplayName()));
		if(acp.getAccessControlList() == null) {
			acp.setAccessControlList(new AccessControlList());
		}
		Grantee grantee = null;
		for(com.amazonaws.services.s3.model.Grant g : s3Acl.getGrants()) {
			
			grantee = new Grantee();
			if(g.getGrantee() instanceof CanonicalGrantee) {
				grantee.setCanonicalUser(new CanonicalUser(g.getGrantee().getIdentifier(),((CanonicalGrantee)g.getGrantee()).getDisplayName()));
			} else if(g.getGrantee() instanceof GroupGrantee) {
				grantee.setGroup(new Group(g.getGrantee().getIdentifier()));
			} else if(g.getGrantee() instanceof EmailAddressGrantee) {
				grantee.setEmailAddress(g.getGrantee().getIdentifier());
			}
			
			acp.getAccessControlList().getGrants().add(new Grant(grantee, g.getPermission().toString()));
		}
		
		return acp;
	}
	
	/**
	 * Maps the request credentials to another set of credentials. This implementation maps
	 * all Eucalyptus credentials to a single s3/backend credential.
	 * 
	 * @param requestUser The Eucalyptus user that generated the request
	 * @param requestAccessKeyId The access key id used for this request
	 * @return a BasicAWSCredentials object initialized with the credentials to use
	 * @throws NoSuchElementException
	 * @throws IllegalArgumentException
	 */
	protected BasicAWSCredentials mapCredentials(User requestUser, String requestAWSAccessKeyId) throws NoSuchElementException, IllegalArgumentException {
		return new BasicAWSCredentials(S3ProviderConfiguration.getS3AccessKey(), S3ProviderConfiguration.getS3SecretKey());
	}
	
	protected ObjectMetadata getS3ObjectMetadata(PutObjectType request) {
		ObjectMetadata meta = new ObjectMetadata();
		if(request.getMetaData() != null) {
			for(MetaDataEntry m : request.getMetaData()) {				
				meta.addUserMetadata(m.getName(), m.getValue());
			}
		}
		
		if(!Strings.isNullOrEmpty(request.getContentLength())) {
			meta.setContentLength(Long.parseLong(request.getContentLength()));
		}
		
		if(!Strings.isNullOrEmpty(request.getContentMD5())) {		
			meta.setContentMD5(request.getContentMD5());
		}
		
		if(!Strings.isNullOrEmpty(request.getContentType())) {
			meta.setContentType(request.getContentType());
		}

		return meta;
	}
	
	@Override
	public void initialize() throws EucalyptusCloudException {
		LOG.debug("Initializing");		
		LOG.debug("Initialization completed successfully");		
	}

	@Override
	public void check() throws EucalyptusCloudException {
		LOG.debug("Checking");		
		LOG.debug("Check completed successfully");		
	}
	
	@Override
	public void checkPreconditions() throws EucalyptusCloudException {
		LOG.debug("Checking preconditions");		
		LOG.debug("Check preconditions completed successfully");
	}

	@Override
	public void start() throws EucalyptusCloudException {
		LOG.debug("Starting");		
		LOG.debug("Start completed successfully");		
	}

	@Override
	public void stop() throws EucalyptusCloudException {
		LOG.debug("Stopping");
		LOG.debug("Stop completed successfully");		
	}

	@Override
	public void enable() throws EucalyptusCloudException {
		LOG.debug("Enabling");		
		LOG.debug("Enable completed successfully");		
	}

	@Override
	public void disable() throws EucalyptusCloudException {
		LOG.debug("Disabling");		
		LOG.debug("Disable completed successfully");
	}
		
	/*
	 * TODO: add multi-account support on backend and then this can be a pass-thru to backend for bucket listing.
	 * Multiplexing a single eucalyptus account on the backend means we have to track all of the user buckets ourselves
	 * (non-Javadoc)
	 * @see com.eucalyptus.objectstorage.ObjectStorageProviderClient#listAllMyBuckets(com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType)
	 */
	@Override
	public ListAllMyBucketsResponseType listAllMyBuckets(ListAllMyBucketsType request) throws EucalyptusCloudException {		
		ListAllMyBucketsResponseType reply = (ListAllMyBucketsResponseType) request.getReply();
		try {
			//The euca-types
			ListAllMyBucketsList myBucketList = new ListAllMyBucketsList();
			myBucketList.setBuckets(new ArrayList<BucketListEntry>());
			Context ctx = Contexts.lookup(request.getCorrelationId());
			
			//The s3 client types
			AmazonS3Client s3Client = this.getS3Client(ctx.getUser(), request.getAccessKeyID());
			ListBucketsRequest listRequest = new ListBucketsRequest();
			
			//Map s3 client result to euca response message
			List<Bucket> result = s3Client.listBuckets(listRequest);
			for(Bucket b : result) {
				myBucketList.getBuckets().add(new BucketListEntry(b.getName(), OSGUtil.dateToFormattedString(b.getCreationDate())));
			}
			
			reply.setBucketList(myBucketList);
			reply.setOwner(new CanonicalUser(ctx.getAccount().getCanonicalId(), ctx.getUser().getName()));
		} catch (Exception e) {
			LOG.debug(e, e);
			throw new EucalyptusCloudException(e);
		}
		return reply;		
	}

	/**
	 * Handles a HEAD request to the bucket. Just returns 200ok if bucket exists and user has access. Otherwise
	 * returns 404 if not found or 403 if no accesss.
	 * @param request
	 * @return
	 * @throws EucalyptusCloudException
	 */
	@Override
	public HeadBucketResponseType headBucket(HeadBucketType request) throws EucalyptusCloudException {
		throw new NotImplementedException("HeadBucket");
	}

	@Override
	public CreateBucketResponseType createBucket(CreateBucketType request) throws EucalyptusCloudException {
		CreateBucketResponseType reply = (CreateBucketResponseType) request.getReply();
		User requestUser = null;
		try {
			Context ctx = Contexts.lookup(request.getCorrelationId());
			requestUser = ctx.getUser();
		} catch(NoSuchContextException e) {
			LOG.error("No context found for correlationId " + request.getCorrelationId(), e);
			
			try {
				requestUser = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID());				
			} catch(Exception ex) {
				LOG.error("Fallback non-context-based lookup of user and canonical id failed", e);
				throw new EucalyptusCloudException("Cannot create bucket without user identity");
			}
			
		}	
		
		// call the storage manager to save the bucket to disk
		try {
			AmazonS3Client s3Client = getS3Client(requestUser, requestUser.getUserId());
			Bucket responseBucket = s3Client.createBucket(request.getBucket());
			//Save the owner info in response?
		} catch(AmazonServiceException ex) {
			LOG.error("Got service error from backend: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		} catch(AmazonClientException ex) {
			LOG.error("Got client error from internal Amazon Client: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		}
		
		return reply;		
	}

	@Override
	public DeleteBucketResponseType deleteBucket(DeleteBucketType request) throws EucalyptusCloudException {
		DeleteBucketResponseType reply = (DeleteBucketResponseType) request.getReply();
		User requestUser = null;
		try {
			Context ctx = Contexts.lookup(request.getCorrelationId());
			requestUser = ctx.getUser();
		} catch(NoSuchContextException e) {
			LOG.error("No context found for correlationId " + request.getCorrelationId(), e);
			
			try {
				requestUser = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID());
			} catch(Exception ex) {
				LOG.error("Fallback non-context-based lookup of user and canonical id failed", e);
				throw new EucalyptusCloudException("Cannot create bucket without user identity");
			}
			
		}	
		
		// call the storage manager to save the bucket to disk
		try {
			AmazonS3Client s3Client = getS3Client(requestUser, requestUser.getUserId());
			s3Client.deleteBucket(request.getBucket());
		} catch(AmazonServiceException ex) {
			LOG.error("Got service error from backend: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		} catch(AmazonClientException ex) {
			LOG.error("Got client error from internal Amazon Client: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		}
		
		return reply;		
	}

	@Override
	public GetBucketAccessControlPolicyResponseType getBucketAccessControlPolicy(
			GetBucketAccessControlPolicyType request)
					throws EucalyptusCloudException {
		GetBucketAccessControlPolicyResponseType reply = (GetBucketAccessControlPolicyResponseType) request.getReply();
		User requestUser = null;
		try {
			Context ctx = Contexts.lookup(request.getCorrelationId());
			requestUser = ctx.getUser();
		} catch(NoSuchContextException e) {
			LOG.error("No context found for correlationId " + request.getCorrelationId(), e);
			
			try {
				requestUser = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID());				
			} catch(Exception ex) {
				LOG.error("Fallback non-context-based lookup of user and canonical id failed", e);
				throw new EucalyptusCloudException("Cannot create bucket without user identity");
			}			
		}	
		
		// call the storage manager to save the bucket to disk
		try {
			AmazonS3Client s3Client = getS3Client(requestUser, requestUser.getUserId());
			com.amazonaws.services.s3.model.AccessControlList acl = s3Client.getBucketAcl(request.getBucket());
			reply.setAccessControlPolicy(sdkAclToEucaAcl(acl));			
		} catch(AmazonServiceException ex) {
			LOG.error("Got service error from backend: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		} catch(AmazonClientException ex) {
			LOG.error("Got client error from internal Amazon Client: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		}
		
		return reply;
	}
	
	@Override
	public PutObjectResponseType putObject(PutObjectType request, InputStream inputData) throws EucalyptusCloudException {
		try {
			AmazonS3Client s3Client = getS3Client(Contexts.lookup().getUser(), request.getAccessKeyID());
			PutObjectResult result = null;
			try {
				ObjectMetadata metadata = getS3ObjectMetadata(request);
				//Set the acl to private.
				PutObjectRequest putRequest = new PutObjectRequest(request.getBucket(), 
						request.getKey(), 
						inputData, 
						metadata).withCannedAcl(CannedAccessControlList.Private);
				result = s3Client.putObject(putRequest);
			} catch(Exception e) {
				LOG.error("Error putting object to backend",e);
				throw e;
			}

			PutObjectResponseType reply = (PutObjectResponseType)request.getReply();			
			if(result == null) {
				throw new EucalyptusCloudException("Null result. Internal error");
			} else {
				reply.setEtag(result.getETag());
				reply.setVersionId(result.getVersionId());				
			}
			return reply;
		} catch(Exception e) {
			throw new EucalyptusCloudException(e);
		}
	}

	@Override
	public PostObjectResponseType postObject(PostObjectType request)
			throws EucalyptusCloudException {
		throw new NotImplementedException("PostObject");
	}

	@Override
	public DeleteObjectResponseType deleteObject(DeleteObjectType request) throws EucalyptusCloudException {
		try {
			AmazonS3Client s3Client = getS3Client(Contexts.lookup().getUser(), request.getAccessKeyID());
			s3Client.deleteObject(request.getBucket(), request.getKey());
			DeleteObjectResponseType reply = (DeleteObjectResponseType) request.getReply();
			reply.setCode("200");
			reply.setDescription("OK");
			return reply;
		} catch(Exception e) {
			LOG.error("Unable to delete object", e);
			throw new EucalyptusCloudException(e);
		}
	}

	@Override
	public ListBucketResponseType listBucket(ListBucketType request) throws EucalyptusCloudException {
		ListBucketResponseType reply = (ListBucketResponseType) request.getReply();
		try {
			AmazonS3Client s3Client = getS3Client(Contexts.lookup(request.getCorrelationId()).getUser(), Contexts.lookup(request.getCorrelationId()).getUser().getUserId());
			ListObjectsRequest listRequest = new ListObjectsRequest();
			listRequest.setBucketName(request.getBucket());
			listRequest.setDelimiter(Strings.isNullOrEmpty(request.getDelimiter()) ? null : request.getDelimiter());
			listRequest.setMarker(Strings.isNullOrEmpty(request.getMarker()) ? null : request.getMarker());
			listRequest.setMaxKeys((request.getMaxKeys() == null ? null : Integer.parseInt(request.getMaxKeys())));
			listRequest.setPrefix(Strings.isNullOrEmpty(request.getPrefix()) ? null : request.getPrefix());
			
			ObjectListing response = s3Client.listObjects(listRequest);
			/* Non-optional, must have non-null values */
			reply.setName(request.getBucket());
			reply.setMaxKeys(response.getMaxKeys());
			reply.setMarker(response.getMarker() == null ? "" : response.getMarker());
			reply.setPrefix(response.getPrefix() == null ? "" : response.getPrefix());
			reply.setIsTruncated(response.isTruncated());

			/* Optional */
			reply.setNextMarker(response.getNextMarker());			
			reply.setDelimiter(response.getDelimiter());
			
			if(reply.getContents() == null) {
				reply.setContents(new ArrayList<ListEntry>());
			}
			if(reply.getCommonPrefixes() == null) {
				reply.setCommonPrefixes(new ArrayList<PrefixEntry>());
			}
			
			for(S3ObjectSummary obj : response.getObjectSummaries()) {
				//Add entry, note that the canonical user is set based on requesting user, not returned user
				reply.getContents().add(new ListEntry(
						obj.getKey(),
						OSGUtil.dateToFormattedString(obj.getLastModified()),
						obj.getETag(),
						obj.getSize(),
						ObjectStorageGateway.buildCanonicalUser(Contexts.lookup(request.getCorrelationId()).getAccount()),
						obj.getStorageClass()));
			}
			
			for(String commonPrefix : response.getCommonPrefixes()) {
				reply.getCommonPrefixes().add(new PrefixEntry(commonPrefix));
			}
			
			return reply;
		} catch(AmazonServiceException e) {
			throw new S3Exception(e.getErrorCode(), e.getMessage(), HttpResponseStatus.valueOf(e.getStatusCode()));
		} catch(AmazonClientException e) {
			InternalErrorException ex = new InternalErrorException();
			ex.initCause(e);
			ex.setMessage(e.getMessage());
			throw ex;
		} catch(Exception e) {
			LOG.error("Error listing bucket from s3", e);
			throw new EucalyptusCloudException("Unknown error", e);
		}
	}

	@Override
	public GetObjectAccessControlPolicyResponseType getObjectAccessControlPolicy(
			GetObjectAccessControlPolicyType request)
					throws EucalyptusCloudException {
		GetObjectAccessControlPolicyResponseType reply = (GetObjectAccessControlPolicyResponseType) request.getReply();
		User requestUser = null;
		try {
			Context ctx = Contexts.lookup(request.getCorrelationId());
			requestUser = ctx.getUser();
		} catch(NoSuchContextException e) {
			LOG.error("No context found for correlationId " + request.getCorrelationId(), e);
			
			try {
				requestUser = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID());				
			} catch(Exception ex) {
				LOG.error("Fallback non-context-based lookup of user and canonical id failed", e);
				throw new EucalyptusCloudException("Cannot create bucket without user identity");
			}			
		}	
		
		// call the storage manager to save the bucket to disk
		try {
			AmazonS3Client s3Client = getS3Client(requestUser, requestUser.getUserId());
			com.amazonaws.services.s3.model.AccessControlList acl = s3Client.getObjectAcl(request.getBucket(),request.getKey(), request.getVersionId());
			reply.setAccessControlPolicy(sdkAclToEucaAcl(acl));			
		} catch(AmazonServiceException ex) {
			LOG.error("Got service error from backend: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		} catch(AmazonClientException ex) {
			LOG.error("Got client error from internal Amazon Client: " + ex.getMessage(), ex);
			throw new EucalyptusCloudException(ex);
		}
		
		return reply;
	}

	@Override
	public SetBucketAccessControlPolicyResponseType setBucketAccessControlPolicy(
			SetBucketAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public SetRESTBucketAccessControlPolicyResponseType setRESTBucketAccessControlPolicy(
			SetRESTBucketAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public SetObjectAccessControlPolicyResponseType setObjectAccessControlPolicy(
			SetObjectAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public SetRESTObjectAccessControlPolicyResponseType setRESTObjectAccessControlPolicy(
			SetRESTObjectAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}
	
	@Override
	public GetObjectResponseType getObject(final GetObjectType request) throws EucalyptusCloudException {
		AmazonS3Client s3Client = getS3Client(Contexts.lookup().getUser(), request.getAccessKeyID());
		GetObjectRequest getRequest = new GetObjectRequest(request.getBucket(), request.getKey());
		try {
			S3Object response = s3Client.getObject(getRequest);
			S3ObjectInputStream input = response.getObjectContent();
			DefaultHttpResponse httpResponse = createHttpResponse(response.getObjectMetadata());
			Channel channel = request.getChannel();
			channel.write(httpResponse);
			final ChunkedDataStream dataStream = new ChunkedDataStream(new PushbackInputStream(input));
			channel.write(dataStream).addListener(new ChannelFutureListener( ) {
				@Override public void operationComplete( ChannelFuture future ) throws Exception {			
					Contexts.clear(request.getCorrelationId());
					dataStream.close();
				}
			});
			return null;
		} catch(Exception ex) {
			LOG.error(ex, ex);
			return null;
		}
	}

	/**
	 * Common get routine used by simple and extended GETs.
	 * 
	 * @param request
	 * @param getRequest
	 * @return
	 */

	protected DefaultHttpResponse createHttpResponse(ObjectMetadata metadata) {
		DefaultHttpResponse httpResponse = new DefaultHttpResponse(
				HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
		long contentLength = metadata.getContentLength();
		String contentType = metadata.getContentType();
		String etag = metadata.getETag();
		Date lastModified = metadata.getLastModified();
		String contentDisposition = metadata.getContentDisposition();
		httpResponse.addHeader( HttpHeaders.Names.CONTENT_TYPE, contentType != null ? contentType : "binary/octet-stream" );
		if(etag != null)
			httpResponse.addHeader(HttpHeaders.Names.ETAG, etag);
		httpResponse.addHeader(HttpHeaders.Names.LAST_MODIFIED, lastModified);
		if(contentDisposition != null) {
			httpResponse.addHeader("Content-Disposition", contentDisposition);
		}
		httpResponse.addHeader( HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(contentLength));
		String versionId = metadata.getVersionId();
		if(versionId != null) {
			httpResponse.addHeader(WalrusProperties.X_AMZ_VERSION_ID, versionId);
		}
		return httpResponse;
	}

	@Override
	public GetObjectExtendedResponseType getObjectExtended(final GetObjectExtendedType request) throws EucalyptusCloudException {
        //TODO: This is common. Stop repeating.
		User requestUser = null;
		String canonicalId = null;
		try {
			Context ctx = Contexts.lookup(request.getCorrelationId());
			canonicalId = ctx.getAccount().getCanonicalId();
			requestUser = ctx.getUser();
		} catch(NoSuchContextException e) {
			LOG.error("No context found for correlationId " + request.getCorrelationId(), e);
			
			try {
				requestUser = Accounts.lookupUserByAccessKeyId(request.getAccessKeyID());
				canonicalId = requestUser.getAccount().getCanonicalId();
			} catch(Exception ex) {
				LOG.error("Fallback non-context-based lookup of user and canonical id failed", e);
				throw new EucalyptusCloudException("Cannot create bucket without user identity");
			}
			
		}	

		Boolean isHead = request.getGetData() == null ? false : !(request.getGetData());
		Boolean getMetaData = request.getGetMetaData();
		Boolean inlineData = request.getInlineData();
		Long byteRangeStart = request.getByteRangeStart();
		Long byteRangeEnd = request.getByteRangeEnd();
		Date ifModifiedSince = request.getIfModifiedSince();
		Date ifUnmodifiedSince = request.getIfUnmodifiedSince();
		String ifMatch = request.getIfMatch();
		String ifNoneMatch = request.getIfNoneMatch();

		GetObjectRequest getRequest = new GetObjectRequest(request.getBucket(), request.getKey());
		if(byteRangeStart == null) {
			byteRangeStart = 0L;
		}
		if(byteRangeEnd != null) {
			getRequest.setRange(byteRangeStart, byteRangeEnd);
		}
		if(getMetaData != null) {
			//Get object metadata
		}
		if(ifModifiedSince != null) {
			getRequest.setModifiedSinceConstraint(ifModifiedSince);
		}
		if(ifUnmodifiedSince != null) {
			getRequest.setUnmodifiedSinceConstraint(ifUnmodifiedSince);
		}
		if(ifMatch != null) {
			List matchList = new ArrayList();
			matchList.add(ifMatch);
			getRequest.setMatchingETagConstraints(matchList);
		}
		if(ifNoneMatch != null) {
			List nonMatchList = new ArrayList();
			nonMatchList.add(ifNoneMatch);
			getRequest.setNonmatchingETagConstraints(nonMatchList);
		}
		try {
			AmazonS3Client s3Client = this.getS3Client(requestUser, requestUser.getUserId());
			S3Object response = s3Client.getObject(getRequest);
			DefaultHttpResponse httpResponse = createHttpResponse(response.getObjectMetadata());
			//write extra headers
			if(byteRangeEnd != null) {
				httpResponse.addHeader("Content-Range", byteRangeStart + "-" + byteRangeEnd + "/" + response.getObjectMetadata().getContentLength());
			}
			Channel channel = request.getChannel();
			channel.write(httpResponse);
			S3ObjectInputStream input = response.getObjectContent();
			final ChunkedDataStream dataStream = new ChunkedDataStream(new PushbackInputStream(input));
			channel.write(dataStream).addListener(new ChannelFutureListener( ) {
				@Override public void operationComplete( ChannelFuture future ) throws Exception {			
					Contexts.clear(request.getCorrelationId());
					dataStream.close();
				}
			});
			return null;
		} catch(Exception ex) {
			LOG.error(ex, ex);
			return null;
		}
	}

	@Override
	public GetBucketLocationResponseType getBucketLocation(
			GetBucketLocationType request) throws EucalyptusCloudException {		
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public CopyObjectResponseType copyObject(CopyObjectType request)
			throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public SetBucketLoggingStatusResponseType setBucketLoggingStatus(
			SetBucketLoggingStatusType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public GetBucketLoggingStatusResponseType getBucketLoggingStatus(
			GetBucketLoggingStatusType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public GetBucketVersioningStatusResponseType getBucketVersioningStatus(GetBucketVersioningStatusType request)
			throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public SetBucketVersioningStatusResponseType setBucketVersioningStatus(
			SetBucketVersioningStatusType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}


	@Override
	public ListVersionsResponseType listVersions(ListVersionsType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public DeleteVersionResponseType deleteVersion(DeleteVersionType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}


}
