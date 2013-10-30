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

package com.eucalyptus.storage.msgs.s3

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.jboss.netty.handler.codec.http.HttpResponseStatus

import edu.ucsb.eucalyptus.msgs.BaseMessage
import edu.ucsb.eucalyptus.msgs.EucalyptusData;
import edu.ucsb.eucalyptus.msgs.StreamedBaseMessage

/**
 * Base type for all S3 requests
 * @author zhill
 *
 */
public class S3Request extends BaseMessage {
	protected String accessKeyID;
	protected Date timeStamp;
	protected String signature;
	protected String credential;
	protected String bucket;
	protected String key;

	public S3Request() {}

	public S3Request( String bucket, String key ) {
		this.bucket = bucket;
		this.key = key;
	}

	public S3Request(String accessKeyID, Date timeStamp, String signature, String credential) {
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

/**
 * Base type for all S3 responses
 */
public class S3Response extends BaseMessage {}

public class S3ErrorMessage extends BaseMessage {
	protected String message;
	protected String code;
	protected HttpResponseStatus status;
	protected String resourceType;
	protected String resource;
	protected String requestId;
	protected String hostId;
	protected String errorSource; //Client or Server

	def S3ErrorMessage() {}

	def S3ErrorMessage(String message,
	String code,
	HttpResponseStatus status,
	String resourceType,
	String resource,
	String requestId,
	String hostId) {
		this.message = message;
		this.code = code;
		this.status = status;
		this.resourceType = resourceType;
		this.resource = resource;
		this.requestId = requestId;
		this.hostId = hostId;
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

/*
 * Inserted from ObjectStorage.groovy
 */

public class S3GetDataResponse extends StreamedBaseMessage {
	String etag;
	String lastModified;
	Long size;
	ArrayList<MetaDataEntry> metaData = new ArrayList<MetaDataEntry>();
	Integer errorCode;
	String contentType;
	String contentDisposition;
	String versionId;

	def S3GetDataResponse() {}
}

public class S3Redirect extends S3ErrorMessage {
	private String redirectUrl;

	def S3Redirect() {
		this.code = 301;
	}

	def S3Redirect(String redirectUrl) {
		this.redirectUrl = redirectUrl;
		this.code = 301;
	}

	public String toString() {
		return "S3Redirect:" +  redirectUrl;
	}

	public String getRedirectUrl() {
		return redirectUrl;
	}
}

public class AccessControlPolicy extends EucalyptusData {
	CanonicalUser owner;
	AccessControlList accessControlList;

	AccessControlPolicy() {}
	AccessControlPolicy(CanonicalUser owner, AccessControlList acl) {
		this.owner = owner; this.accessControlList = acl;
	}
}

public class Status extends EucalyptusData {
	int code;
	String description;
}

public class CanonicalUser extends EucalyptusData {
	String ID; // account ID - numeric
	String DisplayName; // account display name (i.e. eucalyptus)

	public CanonicalUser() {}

	public CanonicalUser(String ID, String DisplayName) {
		this.ID = ID;
		this.DisplayName = DisplayName;
	}
}

public class Group extends EucalyptusData {
	String uri;

	public Group() {}

	public Group(String uri) {
		this.uri = uri;
	}
}

public class Grantee extends EucalyptusData {
	CanonicalUser canonicalUser;
	Group group;
	String type;
	String emailAddress;

	public Grantee() {}

	public Grantee(CanonicalUser canonicalUser) {
		this.canonicalUser = canonicalUser;
		this.type = "CanonicalUser";
	}

	public Grantee(Group group) {
		this.group = group;
		this.type = "Group";
	}

	public Grantee(String emailAddress) {
		this.emailAddress = emailAddress;
		this.type = "AmazonCustomerByEmail";
	}
}

public class Grant extends EucalyptusData {
	Grantee grantee;
	String permission;

	public Grant() {}

	public Grant(Grantee grantee, String permission) {
		this.grantee = grantee;
		this.permission = permission;
	}
}

public class AccessControlList extends EucalyptusData {
	ArrayList<Grant> grants = new ArrayList<Grant>();
}

public class ListAllMyBucketsList extends EucalyptusData {
	ArrayList<BucketListEntry> buckets = new ArrayList<BucketListEntry>();
}

public class BucketListEntry extends EucalyptusData {
	String name;
	String creationDate;

	public BucketListEntry() {
	}

	public BucketListEntry(String name, String creationDate) {
		this.name = name;
		this.creationDate = creationDate;
	}
}


public class TargetGrants extends EucalyptusData {
	ArrayList<Grant> grants = new ArrayList<Grant>();

	def TargetGrants() {}
	def TargetGrants(List<Grant> grants) {
		this.grants = grants;
	}
}

public class LoggingEnabled extends EucalyptusData {
	String targetBucket;
	String targetPrefix;
	TargetGrants targetGrants;

	def LoggingEnabled() {}
	def LoggingEnabled(TargetGrants grants) {
		targetGrants = grants;
	}
	def LoggingEnabled(String bucket, String prefix, TargetGrants grants) {
		targetBucket = bucket;
		targetPrefix = prefix;
		targetGrants = grants;
	}
}

public class MetaDataEntry extends EucalyptusData {
	String name;
	String value;
}

public class ListEntry extends EucalyptusData {
	String key;
	String lastModified;
	String etag;
	long size;
	CanonicalUser owner;
	String storageClass;
	
	def ListEntry() {}
	
	def ListEntry(String objKey, String modified, String eTag, long objSize, CanonicalUser objOwner, String objStorageClass) {
		this.key = objKey;
		this.lastModified = modified;
		this.etag = eTag;
		this.size = objSize;
		this.owner = objOwner;
		this.storageClass = objStorageClass;
	}
}

public class PrefixEntry extends EucalyptusData {
	String prefix;

	def PrefixEntry() {}

	def PrefixEntry(String prefix) {
		this.prefix = prefix;
	}
}

public class VersionEntry extends EucalyptusData {
	String key;
	String versionId;
	Boolean isLatest;
	String lastModified;
	String etag;
	long size;
	String storageClass;
	CanonicalUser owner;
}

public class DeleteMarkerEntry extends EucalyptusData {
	String key;
	String versionId;
	Boolean isLatest;
	String lastModified;
	CanonicalUser owner;
}

