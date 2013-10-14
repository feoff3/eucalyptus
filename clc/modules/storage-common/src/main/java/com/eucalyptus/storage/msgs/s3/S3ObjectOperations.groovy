package com.eucalyptus.storage.msgs.s3

import java.util.ArrayList;
import java.util.Date;

/*
 * NOTE: not used by OSG or Walrus yet.
 * These are S3 object operation messages with no euca-service specific semantics, just s3.
 * 
 */

public class GetObjectExtendedResponseType extends S3Response {
	Status status;
}

public class GetObjectAccessControlPolicyResponseType extends S3Request {
	AccessControlPolicy accessControlPolicy;
}

public class GetObjectAccessControlPolicyType extends S3Request {
	String versionId;
}

public class PutObjectResponseType extends S3Response {}

public class PostObjectResponseType extends S3Response {
	String redirectUrl;
	Integer successCode;
	String location;
	String bucket;
	String key;
}

public class PutObjectInlineResponseType extends S3Response {}

public class PutChunkType extends S3Request {}

public class PutChunkResponseType extends S3Response {}

public class PutObjectType extends S3Request {
	String contentLength;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String contentType;
	String contentDisposition;
	String contentMD5;

	def PutObjectType() {}
}

public class PostObjectType extends S3Request {
	String contentLength;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String successActionRedirect;
	Integer successActionStatus;
	String contentType;
}

public class CopyObjectType extends S3Request {
	String sourceBucket;
	String sourceObject;
	String sourceVersionId;
	String destinationBucket;
	String destinationObject;
	String metadataDirective;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String copySourceIfMatch;
	String copySourceIfNoneMatch;
	Date copySourceIfModifiedSince;
	Date copySourceIfUnmodifiedSince;
}

public class CopyObjectResponseType extends S3Response {
	String copySourceVersionId;
	String versionId;
}

public class PutObjectInlineType extends S3Request {
	String contentLength;
	ArrayList<MetaDataEntry> metaData  = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String base64Data;
	String contentType;
	String contentDisposition;
}

public class DeleteObjectType extends S3Request {}

public class DeleteObjectResponseType extends S3Response {
	String code;
	String description;
}

public class DeleteVersionType extends S3Request {
	String versionid;
}

public class DeleteVersionResponseType extends S3Response {
	String code;
	String description;
}

public class SetObjectAccessControlPolicyType extends S3Request {
	AccessControlList accessControlList;
	String versionId;
}

public class SetObjectAccessControlPolicyResponseType extends S3Response {
	String code;
	String description;
}

public class SetRESTObjectAccessControlPolicyType extends S3Request {
	AccessControlPolicy accessControlPolicy;
	String versionId;
}

public class SetRESTObjectAccessControlPolicyResponseType extends S3Response {
	String code;
	String description;
}

public class GetObjectType extends S3Request {
	Boolean getMetaData;
	Boolean getData;
	Boolean inlineData;
	Boolean deleteAfterGet;
	Boolean getTorrent;
	String versionId;

	def GetObjectType() {
	}

	def GetObjectType(final String bucketName, final String key, final Boolean getData, final Boolean getMetaData, final Boolean inlineData) {
		super( bucketName, key );
		this.getData = getData;
		this.getMetaData = getMetaData;
		this.inlineData = inlineData;
	}
}

public class GetObjectResponseType extends S3Response {
	Status status;
	String base64Data;
}

public class GetObjectExtendedType extends S3Request {
	Boolean getData;
	Boolean getMetaData;
	Boolean inlineData;
	Long byteRangeStart;
	Long byteRangeEnd;
	Date ifModifiedSince;
	Date ifUnmodifiedSince;
	String ifMatch;
	String ifNoneMatch;
	Boolean returnCompleteObjectOnConditionFailure;
}

public class AddObjectResponseType extends S3Response {}

public class AddObjectType extends S3Request {
	String objectName;
	String etag;
	AccessControlList accessControlList = new AccessControlList();
}