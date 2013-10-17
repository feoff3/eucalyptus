package com.eucalyptus.objectstorage.providers.s3;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.log4j.Logger;
import org.apache.tools.ant.util.DateUtils;
import org.hibernate.Criteria;
import org.hibernate.criterion.Example;
import org.hibernate.criterion.Projections;
import org.hibernate.exception.ConstraintViolationException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.eucalyptus.auth.Accounts;
import com.eucalyptus.auth.Permissions;
import com.eucalyptus.auth.policy.PolicySpec;
import com.eucalyptus.auth.principal.Account;
import com.eucalyptus.auth.principal.User;
import com.eucalyptus.context.Context;
import com.eucalyptus.context.Contexts;
import com.eucalyptus.entities.EntityWrapper;
import com.eucalyptus.objectstorage.ObjectStorageProviderClient;
import com.eucalyptus.objectstorage.ObjectStorageProviders.ObjectStorageProviderClientProperty;
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
import com.eucalyptus.objectstorage.msgs.GetObjectType;
import com.eucalyptus.objectstorage.msgs.HeadBucketResponseType;
import com.eucalyptus.objectstorage.msgs.HeadBucketType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsResponseType;
import com.eucalyptus.objectstorage.msgs.ListAllMyBucketsType;
import com.eucalyptus.objectstorage.msgs.ListBucketResponseType;
import com.eucalyptus.objectstorage.msgs.ListBucketType;
import com.eucalyptus.objectstorage.msgs.ListVersionsResponseType;
import com.eucalyptus.objectstorage.msgs.ListVersionsType;
import com.eucalyptus.objectstorage.msgs.ObjectStorageDataRequestType;
import com.eucalyptus.objectstorage.msgs.ObjectStorageRequestType;
import com.eucalyptus.objectstorage.msgs.PostObjectResponseType;
import com.eucalyptus.objectstorage.msgs.PostObjectType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineResponseType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineType;
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
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.storage.msgs.s3.AccessControlList;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.eucalyptus.storage.msgs.s3.BucketListEntry;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.storage.msgs.s3.MetaDataEntry;
import com.eucalyptus.util.EucalyptusCloudException;
import com.eucalyptus.util.Exceptions;
import com.eucalyptus.util.Lookups;
import com.eucalyptus.objectstorage.entities.BucketInfo;
import com.eucalyptus.objectstorage.entities.GrantInfo;
import com.eucalyptus.objectstorage.entities.ObjectInfo;
import com.eucalyptus.objectstorage.entities.ObjectStorageGatewayInfo;
import com.eucalyptus.objectstorage.exceptions.AccessDeniedException;
import com.eucalyptus.objectstorage.exceptions.BucketAlreadyExistsException;
import com.eucalyptus.objectstorage.exceptions.BucketAlreadyOwnedByYouException;
import com.eucalyptus.objectstorage.exceptions.InvalidBucketNameException;
import com.eucalyptus.objectstorage.exceptions.NotImplementedException;
import com.eucalyptus.objectstorage.exceptions.ObjectStorageException;
import com.eucalyptus.objectstorage.exceptions.TooManyBucketsException;
import com.google.common.base.Strings;

/**
 * Base class for S3-api based backends. Uses the Amazon Java SDK as the client.
 * Can be extended for additional capabilities.
 * 
 * The S3ProviderClient does IAM evaluation prior to dispatching requests to the backend and
 * will validate all results upon receipt of the response from the backend (e.g. for listing buckets).
 * 
 *
 */
@ObjectStorageProviderClientProperty("s3")
public class S3ProviderClient extends ObjectStorageProviderClient {
	private static final Logger LOG = Logger.getLogger(S3ProviderClient.class); 
	
	protected boolean usePathStyle = true;
	
	
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
		ClientConfiguration config = new ClientConfiguration();
		config.setProtocol(Protocol.HTTP);
		AWSCredentials credentials = mapCredentials(requestUser, requestAWSAccessKeyId);
		AmazonS3Client client = new AmazonS3Client(credentials, config);
		client.setEndpoint(S3ProviderConfiguration.getS3Endpoint());
		S3ClientOptions ops = new S3ClientOptions();
		ops.setPathStyleAccess(this.usePathStyle);
		client.setS3ClientOptions(ops);
		return client;
	}
	
	/**
	 * Maps the request credentials to another set of credentials.
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
		LOG.debug("Initializing S3ProviderClient");		
		LOG.debug("Initialization of S3ProviderClient completed successfully");		
	}

	@Override
	public void check() throws EucalyptusCloudException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void checkPreconditions() throws EucalyptusCloudException {
		LOG.debug("Checking S3ProviderClient preconditions");		
		LOG.debug("Check S3ProviderClient preconditions completed successfully");
	}

	@Override
	public void start() throws EucalyptusCloudException {
		LOG.debug("Starting S3ProviderClient");		
		LOG.debug("Start of S3ProviderClient completed successfully");		
	}

	@Override
	public void stop() throws EucalyptusCloudException {
		LOG.debug("Stopping S3ProviderClient");		
		LOG.debug("Stop of S3ProviderClient completed successfully");		
	}

	@Override
	public void enable() throws EucalyptusCloudException {
		LOG.debug("Enabling S3ProviderClient");		
		LOG.debug("Enable of S3ProviderClient completed successfully");		
	}

	@Override
	public void disable() throws EucalyptusCloudException {
		LOG.debug("Disabling S3ProviderClient");		
		LOG.debug("Disable of S3ProviderClient completed successfully");
	}
	/**
	 * Evaluates IAM policy on resource. Always uses the S3 policy Vendor.
	 * @param resourceId
	 * @param resourceType
	 * @param policyAction
	 * @param ownerId
	 * @return
	 */
	protected boolean hasPermission(String resourceId, String resourceType, String policyAction, String ownerId) {		
		return Contexts.lookup().hasAdministrativePrivileges() || 
				Lookups.checkPrivilege(policyAction, 
						PolicySpec.VENDOR_S3,
						resourceType,
						resourceId,
						ownerId);		
	}

	/**
	 * Bucket metadata is all stored in the OSG DB.
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
	
	private boolean bucketHasSnapshots(String bucketName) throws Exception {
		EntityWrapper<ObjectInfo> dbSnap = null;

		try {
			dbSnap = EntityWrapper.get(ObjectInfo.class);
			ObjectInfo objInfo = new ObjectInfo();
			objInfo.setBucketName(bucketName);
			objInfo.setIsSnapshot(true);

			Criteria snapCount = dbSnap.createCriteria(ObjectInfo.class).add(Example.create(objInfo)).setProjection(Projections.rowCount());
			snapCount.setReadOnly(true);
			Long rowCount = (Long)snapCount.uniqueResult();
			dbSnap.rollback();
			if (rowCount != null && rowCount.longValue() > 0) {
				return true;
			}
			return false;
		} catch(Exception e) {
			if(dbSnap != null) {
				dbSnap.rollback();
			}
			throw e;
		}
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
		Context ctx = Contexts.lookup();
		Account account = ctx.getAccount();
		
		if (account == null) {
			throw new AccessDeniedException("no such account");
		}
		EntityWrapper<BucketInfo> db = EntityWrapper.get(BucketInfo.class);
		try {
			BucketInfo searchBucket = new BucketInfo();
			searchBucket.setOwnerCanonicalId(account.getCanonicalId());
			searchBucket.setHidden(false);
			
			List<BucketInfo> bucketInfoList = db.queryEscape(searchBucket);
			
			ArrayList<BucketListEntry> buckets = new ArrayList<BucketListEntry>();
			
			for (BucketInfo bucketInfo : bucketInfoList) {
				if (ctx.hasAdministrativePrivileges()) {						
					try {
						//TODO: zhill -- we should modify the bucket schema to indicate if the bucket is a snapshot bucket, or use a seperate type for snap containers
						if(bucketHasSnapshots(bucketInfo.getBucketName())) {
							continue;
						}
					} catch(Exception e) {
						LOG.debug(e, e);
						continue;
					}	
				}	
				if (ctx.hasAdministrativePrivileges() || 
						Lookups.checkPrivilege(PolicySpec.S3_LISTALLMYBUCKETS, 
								PolicySpec.VENDOR_S3,
								PolicySpec.S3_RESOURCE_BUCKET,
								bucketInfo.getBucketName(),
								account.getAccountNumber())) {
					
					buckets.add(new BucketListEntry(bucketInfo.getBucketName(),
							DateUtils.format(bucketInfo.getCreationDate().getTime(), 
									DateUtils.ALT_ISO8601_DATE_PATTERN)));
				}
			}
			db.commit();
			
			try {
				CanonicalUser owner = new CanonicalUser(account.getAccountNumber(), account.getName());
				ListAllMyBucketsList bucketList = new ListAllMyBucketsList();
				reply.setOwner(owner);
				bucketList.setBuckets(buckets);
				reply.setBucketList(bucketList);
			} catch (Exception ex) {
				LOG.error(ex);
				throw new AccessDeniedException("Account: " + account.getName() + " not found", ex);
			}			
		} catch (EucalyptusCloudException e) {
			db.rollback();
			throw e;
		} catch (Exception e) {
			LOG.debug(e, e);
			db.rollback();
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
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public CreateBucketResponseType createBucket(CreateBucketType request) throws EucalyptusCloudException {
		//TODO: move all of the DB and Authz logic out of here.
		CreateBucketResponseType reply = (CreateBucketResponseType) request.getReply();
		Context ctx = Contexts.lookup();
		Account account = ctx.getAccount();
		
		String bucketName = request.getBucket();
		String locationConstraint = request.getLocationConstraint();

		if (account == null) {
			throw new AccessDeniedException("Bucket", bucketName);
		}

		AccessControlList accessControlList = request.getAccessControlList();
		if (accessControlList == null) {
			accessControlList = new AccessControlList();
		}
		
		AccessControlPolicy accessControlPolicy = new AccessControlPolicy();
		accessControlPolicy.setAccessControlList(accessControlList);
		accessControlPolicy.setOwner(new CanonicalUser(account.getCanonicalId(),""));

		if (!checkBucketName(bucketName))
			throw new InvalidBucketNameException(bucketName);

		EntityWrapper<BucketInfo> db = EntityWrapper.get(BucketInfo.class);
		try {
			if (ObjectStorageProperties.shouldEnforceUsageLimits
					&& !Contexts.lookup().hasAdministrativePrivileges()) {
				BucketInfo searchBucket = new BucketInfo();
				searchBucket.setOwnerCanonicalId(account.getCanonicalId());
				List<BucketInfo> bucketList = db.queryEscape(searchBucket);
				if (bucketList.size() >= ObjectStorageGatewayInfo.getObjectStorageGatewayInfo().getStorageMaxBucketsPerAccount()) {
					db.rollback();
					throw new TooManyBucketsException(bucketName);
				}
			}

			BucketInfo bucketInfo = new BucketInfo(bucketName);
			List<BucketInfo> bucketList = db.queryEscape(bucketInfo);

			if (bucketList.size() > 0) {
				if (bucketList.get(0).getOwnerCanonicalId() .equals(account.getCanonicalId())) {
					// bucket already exists and you created it
					db.rollback();
					throw new BucketAlreadyOwnedByYouException(bucketName);
				}
				// bucket already exists
				db.rollback();
				throw new BucketAlreadyExistsException(bucketName);
			} else {
				if (ctx.hasAdministrativePrivileges()
						|| (Permissions.isAuthorized(PolicySpec.VENDOR_S3,
								PolicySpec.S3_RESOURCE_BUCKET, "",
								ctx.getAccount(), PolicySpec.S3_CREATEBUCKET,
								ctx.getUser()) && Permissions.canAllocate(
										PolicySpec.VENDOR_S3,
										PolicySpec.S3_RESOURCE_BUCKET, "",
										PolicySpec.S3_CREATEBUCKET, ctx.getUser(), 1L))) {
					// create bucket and set its acl
					BucketInfo bucket = new BucketInfo(account.getCanonicalId(),
							ctx.getUser().getUserId(), bucketName, new Date());
					try {
						bucket.setAcl(accessControlPolicy);
					} catch(Exception e) {
						LOG.error("Error setting ACL on bucket: " + e.getMessage(), e);
						throw new ObjectStorageException("Internal error");
					}
					bucket.setBucketSize(0L);
					bucket.setLoggingEnabled(false);
					bucket.setVersioning(ObjectStorageProperties.VersioningStatus.Disabled.toString());
					bucket.setHidden(false);
					if (locationConstraint != null)
						bucket.setLocation(locationConstraint);
					else
						bucket.setLocation("US");

					// call the storage manager to save the bucket to disk
					try {
						db.add(bucket);
						try {
							AmazonS3Client s3Client = getS3Client(ctx.getUser(), ctx.getUser().getUserId());
							s3Client.createBucket(bucketName);
						} catch(AmazonServiceException ex) {
							LOG.error("Got service error from backend: " + ex.getMessage(), ex);
							throw new EucalyptusCloudException(ex);
						} catch(AmazonClientException ex) {
							LOG.error("Got client error from internal Amazon Client: " + ex.getMessage(), ex);
							throw new EucalyptusCloudException(ex);
						}					
						db.commit();					
					} catch (Exception ex) {
						LOG.error(ex, ex);
						db.rollback();
						if (Exceptions.isCausedBy(ex, ConstraintViolationException.class)) {
							throw new BucketAlreadyExistsException(bucketName);
						} else {
							throw new EucalyptusCloudException(
									"Unable to create bucket: " + bucketName, ex);
						}
					}
				} else {
					LOG.error("Not authorized to create bucket by " + ctx.getUserFullName());
					db.rollback();
					throw new AccessDeniedException("Bucket", bucketName);
				}
			}
			reply.setBucket(bucketName);
			return reply;
		} finally {
			if(db != null && db.isActive()) {
				db.rollback();
			}
		}

	}

	@Override
	public DeleteBucketResponseType deleteBucket(DeleteBucketType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public GetBucketAccessControlPolicyResponseType getBucketAccessControlPolicy(
			GetBucketAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public PutObjectResponseType putObject(PutObjectType request, InputStream inputData) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public PostObjectResponseType postObject(PostObjectType request)
			throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public PutObjectInlineResponseType putObjectInline(
			PutObjectInlineType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public AddObjectResponseType addObject(AddObjectType request)
			throws EucalyptusCloudException {

		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public DeleteObjectResponseType deleteObject(DeleteObjectType request)
			throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public ListBucketResponseType listBucket(ListBucketType request) throws EucalyptusCloudException {
		ListBucketResponseType reply = (ListBucketResponseType) request.getReply();
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public GetObjectAccessControlPolicyResponseType getObjectAccessControlPolicy(
			GetObjectAccessControlPolicyType request)
					throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
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
	public GetObjectResponseType getObject(GetObjectType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
	}

	@Override
	public GetObjectExtendedResponseType getObjectExtended(GetObjectExtendedType request) throws EucalyptusCloudException {
		throw new NotImplementedException("NO U CANNOT HAS");
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
