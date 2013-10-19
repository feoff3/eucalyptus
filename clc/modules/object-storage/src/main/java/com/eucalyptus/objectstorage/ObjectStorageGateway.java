package com.eucalyptus.objectstorage;

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
import com.eucalyptus.objectstorage.msgs.PostObjectResponseType;
import com.eucalyptus.objectstorage.msgs.PostObjectType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineResponseType;
import com.eucalyptus.objectstorage.msgs.PutObjectInlineType;
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
import com.eucalyptus.util.EucalyptusCloudException;

/**
 * Primary interface for the OSG component.
 * @author zhill
 *
 */
public interface ObjectStorageGateway {

	public abstract UpdateObjectStorageConfigurationResponseType UpdateObjectStorageConfiguration(
			UpdateObjectStorageConfigurationType request)
			throws EucalyptusCloudException;

	public abstract GetObjectStorageConfigurationResponseType GetObjectStorageConfiguration(
			GetObjectStorageConfigurationType request)
			throws EucalyptusCloudException;

	public abstract HeadBucketResponseType HeadBucket(HeadBucketType request)
			throws EucalyptusCloudException;

	public abstract CreateBucketResponseType CreateBucket(
			CreateBucketType request) throws EucalyptusCloudException;

	public abstract DeleteBucketResponseType DeleteBucket(
			DeleteBucketType request) throws EucalyptusCloudException;

	public abstract ListAllMyBucketsResponseType ListAllMyBuckets(
			ListAllMyBucketsType request) throws EucalyptusCloudException;

	public abstract GetBucketAccessControlPolicyResponseType GetBucketAccessControlPolicy(
			GetBucketAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract PostObjectResponseType PostObject(PostObjectType request)
			throws EucalyptusCloudException;

	public abstract PutObjectInlineResponseType PutObjectInline(
			PutObjectInlineType request) throws EucalyptusCloudException;

	public abstract AddObjectResponseType AddObject(AddObjectType request)
			throws EucalyptusCloudException;

	public abstract DeleteObjectResponseType DeleteObject(
			DeleteObjectType request) throws EucalyptusCloudException;

	public abstract ListBucketResponseType ListBucket(ListBucketType request)
			throws EucalyptusCloudException;

	public abstract GetObjectAccessControlPolicyResponseType GetObjectAccessControlPolicy(
			GetObjectAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract SetBucketAccessControlPolicyResponseType SetBucketAccessControlPolicy(
			SetBucketAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract SetObjectAccessControlPolicyResponseType SetObjectAccessControlPolicy(
			SetObjectAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract SetRESTBucketAccessControlPolicyResponseType SetRESTBucketAccessControlPolicy(
			SetRESTBucketAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract SetRESTObjectAccessControlPolicyResponseType SetRESTObjectAccessControlPolicy(
			SetRESTObjectAccessControlPolicyType request)
			throws EucalyptusCloudException;

	public abstract GetObjectResponseType GetObject(GetObjectType request)
			throws EucalyptusCloudException;

	public abstract GetObjectExtendedResponseType GetObjectExtended(
			GetObjectExtendedType request) throws EucalyptusCloudException;

	public abstract GetBucketLocationResponseType GetBucketLocation(
			GetBucketLocationType request) throws EucalyptusCloudException;

	public abstract CopyObjectResponseType CopyObject(CopyObjectType request)
			throws EucalyptusCloudException;

	public abstract GetBucketLoggingStatusResponseType GetBucketLoggingStatus(
			GetBucketLoggingStatusType request) throws EucalyptusCloudException;

	public abstract SetBucketLoggingStatusResponseType SetBucketLoggingStatus(
			SetBucketLoggingStatusType request) throws EucalyptusCloudException;

	public abstract GetBucketVersioningStatusResponseType GetBucketVersioningStatus(
			GetBucketVersioningStatusType request)
			throws EucalyptusCloudException;

	public abstract SetBucketVersioningStatusResponseType SetBucketVersioningStatus(
			SetBucketVersioningStatusType request)
			throws EucalyptusCloudException;

	public abstract ListVersionsResponseType ListVersions(
			ListVersionsType request) throws EucalyptusCloudException;

	public abstract DeleteVersionResponseType DeleteVersion(
			DeleteVersionType request) throws EucalyptusCloudException;

}