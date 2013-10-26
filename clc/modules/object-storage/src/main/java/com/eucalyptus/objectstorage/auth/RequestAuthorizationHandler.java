package com.eucalyptus.objectstorage.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.eucalyptus.objectstorage.entities.S3AccessControlledEntity;
import com.eucalyptus.objectstorage.msgs.ObjectStorageRequestType;

/**
 * Does ACL + IAM + bucketpolicy (when implemented) checks to authorize a request based on the request
 * type annotations: 
 * {@link com.eucalyptus.objectstorage.policy.AdminOverrideAllowed}, 
 * {@link com.eucalyptus.objectstorage.policy.RequiresACLPermission}, 
 * {@link com.eucalyptus.objectstorage.policy.RequiresPermission}, and
 * {@link ResourceType}
 *
 */
public interface RequestAuthorizationHandler {
	/**
	 * Evaluates the authorization for the operation requested, evaluates IAM, ACL, and bucket policy (bucket policy not yet supported).
	 * @param request
	 * @param optionalResourceId optional (can be null) explicit resourceId to check. If null, the request is used to get the resource.
	 * @param optionalOwnerId optional (can be null) owner Id for the resource being evaluated.
	 * @param optionalResourceAcl option acl for the requested resource
	 * @param resourceAllocationSize the size for the quota check(s) if applicable
	 * @return
	 */
	public abstract <T extends ObjectStorageRequestType> boolean operationAllowed(@Nonnull T request, @Nullable final S3AccessControlledEntity bucketResourceEntity, @Nullable final S3AccessControlledEntity objectResourceEntity, long resourceAllocationSize) throws IllegalArgumentException;

}
