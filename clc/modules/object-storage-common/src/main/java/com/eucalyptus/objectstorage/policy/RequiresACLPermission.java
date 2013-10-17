package com.eucalyptus.objectstorage.policy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.eucalyptus.auth.policy.PolicyAction;
import com.eucalyptus.auth.policy.PolicySpec;
import com.eucalyptus.objectstorage.util.ObjectStorageProperties;

/**
 * Declares the set of ACL permissions required to execute the request.
 *
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiresACLPermission {
	/**
	 * The set of ACL permissions, from {@link ObjectStorageProperties.Permission} that
	 */
	ObjectStorageProperties.Permission[] bucket();
	ObjectStorageProperties.Permission[] object();

}
