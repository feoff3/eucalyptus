package com.eucalyptus.objectstorage.providers.s3;

import com.eucalyptus.configurable.ConfigurableClass;
import com.eucalyptus.configurable.ConfigurableField;
import com.eucalyptus.configurable.ConfigurableFieldType;

@ConfigurableClass( root = "objectstorage", description = "Configuration for S3-compatible backend", singleton=true)
public class S3ProviderConfiguration{
	
	@ConfigurableField( description = "External S3 endpoint.",
			displayName = "objectstorage.s3endpoint" )
	protected static String S3Endpoint  = "s3.amazonaws.com";
	
	@ConfigurableField( description = "External S3 Access Key.",
			displayName = "objectstorage.s3accesskey", 
			type = ConfigurableFieldType.KEYVALUEHIDDEN )
	protected static String S3AccessKey;
	
	@ConfigurableField( description = "External S3 Secret Key.",
			displayName = "objectstorage.s3secretkey", 
			type = ConfigurableFieldType.KEYVALUEHIDDEN )
	protected static String S3SecretKey;

	public static String getS3AccessKey() {
		return S3AccessKey;
	}

	public static void setS3AccessKey(String s3AccessKey) {
		S3AccessKey = s3AccessKey;
	}

	public static String getS3SecretKey() {
		return S3SecretKey;
	}

	public static void setS3SecretKey(String s3SecretKey) {
		S3SecretKey = s3SecretKey;
	}
	
	public static String getS3Endpoint() {
		return S3Endpoint;
	}

	public static void setS3Endpoint(String endPoint) {
		S3Endpoint = endPoint;
	}


}
