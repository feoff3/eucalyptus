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

package com.eucalyptus.objectstorage.msgs

import java.util.ArrayList;
import java.util.Date;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.DefaultHttpMessage;

import com.eucalyptus.auth.policy.PolicySpec;
import com.eucalyptus.component.annotation.ComponentMessage;
import com.eucalyptus.objectstorage.ObjectStorage;
import com.eucalyptus.objectstorage.policy.RequiresPermission;
import com.eucalyptus.objectstorage.policy.RequiresACLPermission;
import com.eucalyptus.objectstorage.policy.ResourceType;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;
import com.eucalyptus.storage.msgs.BucketLogData;
import com.eucalyptus.storage.msgs.s3.AccessControlList;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;
import com.eucalyptus.storage.msgs.s3.CanonicalUser;
import com.eucalyptus.storage.msgs.s3.DeleteMarkerEntry
import com.eucalyptus.storage.msgs.s3.ListAllMyBucketsList;
import com.eucalyptus.storage.msgs.s3.ListEntry
import com.eucalyptus.storage.msgs.s3.LoggingEnabled;
import com.eucalyptus.storage.msgs.s3.MetaDataEntry
import com.eucalyptus.storage.msgs.s3.PrefixEntry
import com.eucalyptus.storage.msgs.s3.S3ErrorMessage
import com.eucalyptus.storage.msgs.s3.S3GetDataResponse
import com.eucalyptus.storage.msgs.s3.S3Request
import com.eucalyptus.storage.msgs.s3.S3Response
import com.eucalyptus.storage.msgs.s3.Status
import com.eucalyptus.storage.msgs.s3.VersionEntry

import edu.ucsb.eucalyptus.msgs.BaseMessage;
import edu.ucsb.eucalyptus.msgs.BaseDataChunk;
import edu.ucsb.eucalyptus.msgs.StreamedBaseMessage;
import edu.ucsb.eucalyptus.msgs.ComponentMessageResponseType;
import edu.ucsb.eucalyptus.msgs.ComponentMessageType;
import edu.ucsb.eucalyptus.msgs.ComponentProperty;
import edu.ucsb.eucalyptus.msgs.EucalyptusData;
import edu.ucsb.eucalyptus.msgs.StatEventRecord;

public class ObjectStorageResponseType extends ObjectStorageRequestType {
	def ObjectStorageResponseType() {}
}

public class ObjectStorageStreamingResponseType extends StreamedBaseMessage {
	BucketLogData logData;
	def ObjectStorageStreamingResponseType() {}
}

@ComponentMessage(ObjectStorage.class)
public class ObjectStorageRequestType extends BaseMessage {
	protected String accessKeyID;
	protected Date timeStamp;
	protected String signature;
	protected String credential;
	BucketLogData logData;
	protected String bucket;
	protected String key;
	public ObjectStorageRequestType() {}
	public ObjectStorageRequestType( String bucket, String key ) {
		this.bucket = bucket;
		this.key = key;
	}
	public ObjectStorageRequestType(String accessKeyID, Date timeStamp, String signature, String credential) {
		this.accessKeyID = accessKeyID;
		this.timeStamp = timeStamp;
		this.signature = signature;
		this.credential = credential;
	}
	public String getAccessKeyID() {
		return accessKeyID;
	}
	public void setAccessKeyID(String accessKeyID) {
		this.accessKeyID = accessKeyID;
	}
	public String getCredential() {
		return credential;
	}
	public void setCredential(String credential) {
		this.credential = credential;
	}
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public void setTimestamp(Date stamp) {
		this.timeStamp = stamp;
	}
	public Date getTimestamp() {
		return this.timeStamp;
	}
}

//TODO: zhill, remove these types, should be handled by the Verb-specific pipelines
public class ObjectStorageDeleteType extends ObjectStorageRequestType {}
public class ObjectStorageDeleteResponseType extends ObjectStorageResponseType {}

public class ObjectStorageErrorMessageType extends BaseMessage {
	protected String message;
	protected String code;
	protected HttpResponseStatus status;
	protected String resourceType;
	protected String resource;
	protected String requestId;
	protected String hostId;
	BucketLogData logData;
	def ObjectStorageErrorMessageType() {}
	def ObjectStorageErrorMessageType(String message,
	String code,
	HttpResponseStatus status,
	String resourceType,
	String resource,
	String requestId,
	String hostId,
	BucketLogData logData) {
		this.message = message;
		this.code = code;
		this.status = status;
		this.resourceType = resourceType;
		this.resource = resource;
		this.requestId = requestId;
		this.hostId = hostId;
		this.logData = logData;
	}
	public HttpResponseStatus getStatus() {
		return status;
	}
	public String getCode() {
		return code;
	}
	public String getMessage() {
		return message;
	}
	public String getResourceType() {
		return resourceType;
	}
	public String getResource() {
		return resource;
	}
}

public class ObjectStorageRedirectMessageType extends ObjectStorageErrorMessageType {
	private String redirectUrl;

	def ObjectStorageRedirectMessageType() {
		this.code = 301;
	}

	def ObjectStorageRedirectMessageType(String redirectUrl) {
		this.redirectUrl = redirectUrl;
		this.code = 301;
	}

	public String toString() {
		return "WalrusRedirectMessage:" +  redirectUrl;
	}

	public String getRedirectUrl() {
		return redirectUrl;
	}
}


@RequiresPermission([PolicySpec.S3_GETBUCKETACL])
@ResourceType(PolicySpec.S3_RESOURCE_BUCKET)
@RequiresACLPermission([ObjectStorageProperties.Permission.READ_ACP])
public class GetBucketAccessControlPolicyType extends ObjectStorageRequestType {}

public class GetBucketAccessControlPolicyResponseType extends ObjectStorageResponseType {
	AccessControlPolicy accessControlPolicy;
}

@RequiresPermission([PolicySpec.S3_GETOBJECTACL])
@ResourceType(PolicySpec.S3_RESOURCE_OBJECT)
@RequiresACLPermission([ObjectStorageProperties.Permission.READ_ACP])
public class GetObjectAccessControlPolicyType extends ObjectStorageRequestType {
	String versionId;
}

public class GetObjectAccessControlPolicyResponseType extends ObjectStorageResponseType {
	AccessControlPolicy accessControlPolicy;
}

//TODO: this permission must be set on each bucket in the listing. @RequiresPermission([PolicySpec.S3_LISTALLMYBUCKETS])
//@ResourceType(PolicySpec.S3_RESOURCE_BUCKET)
public class ListAllMyBucketsType extends ObjectStorageRequestType {}
public class ListAllMyBucketsResponseType extends ObjectStorageResponseType {
	CanonicalUser owner;
	ListAllMyBucketsList bucketList;
}


//TODO: zhill, remove these, should be handled by verb-specific pipelines
public class ObjectStorageHeadRequestType extends ObjectStorageRequestType {}
public class ObjectStorageHeadResponseType extends ObjectStorageResponseType {}


public class HeadBucketType extends ObjectStorageHeadRequestType {}
public class HeadBucketResponseType extends ObjectStorageHeadResponseType{}

public class CreateBucketType extends ObjectStorageRequestType {
	AccessControlList accessControlList;
	String locationConstraint;

	//For unit testing
	public CreateBucketType() {
	}

	public CreateBucketType(String bucket) {
		this.bucket = bucket;
	}
}

public class CreateBucketResponseType extends ObjectStorageResponseType {
	String bucket;
}

public class DeleteBucketType extends ObjectStorageDeleteType {
}

public class DeleteBucketResponseType extends ObjectStorageDeleteResponseType {
	Status status;
}

public class ObjectStorageDataRequestType extends ObjectStorageRequestType {
	String randomKey;
	Boolean isCompressed;

	def ObjectStorageDataRequestType() {
	}

	def ObjectStorageDataRequestType( String bucket, String key ) {
		super( bucket, key );
	}

}

public class ObjectStorageDataResponseType extends ObjectStorageStreamingResponseType {
	String etag;
	String lastModified;
	Long size;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	Integer errorCode;
	String contentType;
	String contentDisposition;
	String versionId;
}

public class ObjectStorageDataGetRequestType extends ObjectStorageDataRequestType {
	protected Channel channel;

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	def ObjectStorageDataGetRequestType() {}

	def ObjectStorageDataGetRequestType(String bucket, String key) {
		super(bucket, key);
	}
}

public class ObjectStorageDataGetResponseType extends ObjectStorageDataResponseType {

	def ObjectStorageDataGetResponseType() {}
}

public class PutObjectResponseType extends ObjectStorageDataResponseType {}

public class PostObjectResponseType extends ObjectStorageDataResponseType {
	String redirectUrl;
	Integer successCode;
	String location;
	String bucket;
	String key;
}

public class PutObjectInlineResponseType extends ObjectStorageDataResponseType {}

public class PutChunkType extends ObjectStorageDataRequestType {}

public class PutChunkResponseType extends ObjectStorageDataResponseType {}

public class PutObjectType extends ObjectStorageDataRequestType {
	String contentLength;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String contentType;
	String contentDisposition;
	String contentMD5;
	byte[] data;

	def PutObjectType() {}
}

public class PostObjectType extends ObjectStorageDataRequestType {
	String contentLength;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String successActionRedirect;
	Integer successActionStatus;
	String contentType;
}
public class CopyObjectType extends ObjectStorageRequestType {
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

public class CopyObjectResponseType extends ObjectStorageDataResponseType {
	String copySourceVersionId;
	String versionId;
}

public class PutObjectInlineType extends ObjectStorageDataRequestType {
	String contentLength;
	ArrayList<MetaDataEntry> metaData  = new ArrayList<MetaDataEntry>();
	AccessControlList accessControlList = new AccessControlList();
	String storageClass;
	String base64Data;
	String contentType;
	String contentDisposition;
}

public class DeleteObjectType extends ObjectStorageDeleteType {}

public class DeleteObjectResponseType extends ObjectStorageDeleteResponseType {
	String code;
	String description;
}

public class DeleteVersionType extends ObjectStorageDeleteType {
	String versionid;
}

public class DeleteVersionResponseType extends ObjectStorageDeleteResponseType {
	String code;
	String description;
}

public class ListBucketType extends ObjectStorageRequestType {
	String prefix;
	String marker;
	String maxKeys;
	String delimiter;

	def ListBucketType() {
		prefix = "";
		marker = "";
		delimiter = "";
	}
}

public class ListBucketResponseType extends ObjectStorageResponseType {
	String name;
	String prefix;
	String marker;
	String nextMarker;
	int maxKeys;
	String delimiter;
	boolean isTruncated;
	ArrayList<MetaDataEntry> metaData;
	ArrayList<ListEntry> contents;
	ArrayList<PrefixEntry> commonPrefixes = new ArrayList<PrefixEntry>();

	private boolean isCommonPrefixesPresent() {
		return commonPrefixes.size() > 0 ? true : false;
	}
}

public class ListVersionsType extends ObjectStorageRequestType {
	String prefix;
	String keyMarker;
	String versionIdMarker;
	String maxKeys;
	String delimiter;

	def ListVersionsType() {
		prefix = "";
	}
}

public class ListVersionsResponseType extends ObjectStorageResponseType {
	String name;
	String prefix;
	String keyMarker;
	String versionIdMarker;
	String nextKeyMarker;
	String nextVersionIdMarker;
	int maxKeys;
	String delimiter;
	boolean isTruncated;
	ArrayList<VersionEntry> versions;
	ArrayList<DeleteMarkerEntry> deleteMarkers;
	ArrayList<PrefixEntry> commonPrefixes;
}

public class SetBucketAccessControlPolicyType extends ObjectStorageRequestType {
	AccessControlList accessControlList;
}

public class SetBucketAccessControlPolicyResponseType extends ObjectStorageResponseType {
	String code;
	String description;
}

public class SetObjectAccessControlPolicyType extends ObjectStorageRequestType {
	AccessControlList accessControlList;
	String versionId;
}

public class SetObjectAccessControlPolicyResponseType extends ObjectStorageResponseType {
	String code;
	String description;
}

public class SetRESTBucketAccessControlPolicyType extends ObjectStorageRequestType {
	AccessControlPolicy accessControlPolicy;
}

public class SetRESTBucketAccessControlPolicyResponseType extends ObjectStorageResponseType {
	String code;
	String description;
}

public class SetRESTObjectAccessControlPolicyType extends ObjectStorageRequestType {
	AccessControlPolicy accessControlPolicy;
	String versionId;
}

public class SetRESTObjectAccessControlPolicyResponseType extends ObjectStorageResponseType {
	String code;
	String description;
}

public class GetObjectType extends ObjectStorageDataGetRequestType {
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

public class GetObjectResponseType extends ObjectStorageDataGetResponseType {
	Status status;
	String base64Data;
}

public class GetObjectExtendedType extends ObjectStorageDataGetRequestType {
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

public class GetObjectExtendedResponseType extends ObjectStorageDataResponseType {
	Status status;
}

public class GetBucketLocationType extends ObjectStorageRequestType {}

public class GetBucketLocationResponseType extends ObjectStorageResponseType {
	String locationConstraint;
}

public class GetBucketLoggingStatusType extends ObjectStorageRequestType {
}

public class GetBucketLoggingStatusResponseType extends ObjectStorageResponseType {
	LoggingEnabled loggingEnabled;
}

public class SetBucketLoggingStatusType extends ObjectStorageRequestType {
	LoggingEnabled loggingEnabled;
}
public class SetBucketLoggingStatusResponseType extends ObjectStorageResponseType {}

public class GetBucketVersioningStatusType extends ObjectStorageRequestType {}
public class GetBucketVersioningStatusResponseType extends ObjectStorageResponseType {
	String versioningStatus;
}

public class SetBucketVersioningStatusType extends ObjectStorageRequestType {
	String versioningStatus;
}
public class SetBucketVersioningStatusResponseType extends ObjectStorageResponseType {}

public class AddObjectResponseType extends ObjectStorageDataResponseType {}
public class AddObjectType extends ObjectStorageDataRequestType {
	String objectName;
	String etag;
	AccessControlList accessControlList = new AccessControlList();
}

public class UpdateObjectStorageConfigurationType extends ObjectStorageRequestType {
	String name;
	ArrayList<ComponentProperty> properties;

	def UpdateObjectStorageConfigurationType() {}
}

public class UpdateObjectStorageConfigurationResponseType extends ObjectStorageResponseType {}

public class GetObjectStorageConfigurationType extends ObjectStorageRequestType {
	String name;

	def GetObjectStorageConfigurationType() {}

	def GetObjectStorageConfigurationType(String name) {
		this.name = name;
	}
}

public class GetObjectStorageConfigurationResponseType extends ObjectStorageRequestType {
	String name;
	ArrayList<ComponentProperty> properties;

	def GetObjectStorageConfigurationResponseType() {}
}

public class ObjectStorageComponentMessageType extends ComponentMessageType {
	@Override
	public String getComponent( ) {
		return "objectstorage";
	}
}
public class ObjectStorageComponentMessageResponseType extends ComponentMessageResponseType {}
