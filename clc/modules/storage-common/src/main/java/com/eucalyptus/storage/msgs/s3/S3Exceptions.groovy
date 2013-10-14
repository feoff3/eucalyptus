package com.eucalyptus.storage.msgs.s3

import com.eucalyptus.util.EucalyptusCloudException
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/*
 * S3 Error codes. See http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
 */

class S3Exception extends EucalyptusCloudException {
	String errorCode;
	String message;
	HttpResponseStatus status;
	String resource;
	String resourceType;
}

class S3ClientException extends S3Exception {
	def S3ClientException() {
		super();
	}
	def S3ClientException(String errorCode, String description, HttpResponseStatus statusCode) {
		super();
		this.code = errorCode;
		this.message = description;
		this.status = statusCode;
	}
}

class S3ServerException extends S3Exception {
	def S3ServerException() {
		super();
	}
	def S3ServerException(String errorCode, String description, HttpResponseStatus statusCode) {
		super();
		this.code = errorCode;
		this.message = description;
		this.status = statusCode;
	}
}

class AccessDenied extends S3ClientException {
	def AccessDenied() {
		super("AccessDenied", "Access Denied", HttpResponseStatus.FORBIDDEN);
	}

	def AccessDenied(String resource) {
		this();
		this.resource = resource;
	}
}

class AccountProblem extends S3ClientException {
	def AccountProblem() {
		super("AccountProblem", "There is a problem with your Eucalyptus account that prevents the operation from completing successfully. Please use Contact Us.", HttpResponseStatus.FORBIDDEN);
	}

	def AccountProblem(String resource) {
		this();
		this.resource = resource;
	}
}

class AmbiguousGrantByEmailAddress extends S3ClientException {
	def AmbiguousGrantByEmailAddress() {
		super("AmbiguousGrantByEmailAddress", "The e-mail address you provided is associated with more than one account.", HttpResponseStatus.BAD_REQUEST);
	}

	def AmbiguousGrantByEmailAddress(String resource) {
		this();
		this.resource = resource;
	}
}

class BadDigest extends S3ClientException {
	def BadDigest() {
		super("BadDigest", "The Content-MD5 you specified did not match what we received.", HttpResponseStatus.BAD_REQUEST);
	}
	def BadDigest(String resource) {
		this();
		this.resource = resource;
	}
}

class BucketAlreadyExists extends S3ClientException {
	def BucketAlreadyExists() {
		super("BucketAlreadyExists","The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.", HttpResponseStatus.CONFLICT);
	}
	def BucketAlreadyExists(String resource) {
		this();
		this.resource = resource;
	}
}

class BucketAlreadyOwnedByYou extends S3ClientException {
	def BucketAlreadyOwnedByYou() {
		super("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", HttpResponseStatus.CONFLICT);
	}
	def BucketAlreadyOwnedByYou(String resource) {
		this();
		this.resource = resource;
	}
}

class BucketNotEmpty extends S3ClientException {
	def BucketNotEmpty() {
		super("BucketNotEmpty", "The bucket you tried to delete is not empty.", HttpResponseStatus.CONFLICT);
	}
	def BucketNotEmpty(String resource) {
		this();
		this.resource = resource;
	}
}

class CredentialsNotSupported extends S3ClientException {
	def CredentialsNotSupported() {
		super("CredentialsNotSupported", "This request does not support credentials.", HttpResponseStatus.BAD_REQUEST);
	}
	def CredentialsNotSupported(String resource) {
		this();
		this.resource = resource;
	}
}

class CrossLocationLoggingProhibited extends S3ClientException {
	def CrossLocationLoggingProhibited() {
		super("CrossLocationLoggingProhibited", "Cross location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.", HttpResponseStatus.FORBIDDEN);
	}
	
	def CrossLocationLoggingProhibited(String resource) {
		this();
		this.resource = resource;
	}
}

class EntityTooSmall extends S3ClientException {
	def EntityTooSmall() {
		super("EntityTooSmall", "Your proposed upload is smaller than the minimum allowed object size.", HttpResponseStatus.BAD_REQUEST);
	}
	def EntityTooSmall(String resource) {
		this();
		this.resource = resource;
	}
}

class EntityTooLarge extends S3ClientException {
	def EntityTooLarge() {
		super("EntityTooLarge", "Your proposed upload exceeds the maximum allowed object size.", HttpResponseStatus.BAD_REQUEST);
	}
	def EntityTooLarge(String resource) {
		this();
		this.resource = resource;
	}
}

class ExpiredToken extends S3ClientException {
	def ExpiredToken() {
		super("ExpiredToken", "The provided token has expired.", HttpResponseStatus.BAD_REQUEST);
	}
	def ExpiredToken (String resource) {
		this();
		this.resource = resource;
	}
}
class IllegalVersioningConfigurationException extends S3ClientException {
	def IllegalVersioningConfigurationException() {
		super("IllegalVersioningConfigurationException", "Indicates that the Versioning configuration specified in the request is invalid.", HttpResponseStatus.BAD_REQUEST);
	}
	def IllegalVersioningConfigurationException(String resource) {
		this();
		this.resource = resource;
	}
}

class IncompleteBody extends S3ClientException {
	def IncompleteBody() {
		super("IncompleteBody", "You did not provide the number of bytes specified by the Content-Length HTTP header",HttpResponseStatus.BAD_REQUEST);
	}
	def IncompleteBody(String resource) {
		this();
		this.resource = resource;
	}
}
class IncorrectNumberOfFilesInPostRequest extends S3ClientException {
	def IncorrectNumberOfFilesInPostRequest() {
		super("IncorrectNumberOfFilesInPostRequest", "POST requires exactly one file upload per request.", HttpResponseStatus.BAD_REQUEST);
	}
	def IncorrectNumberOfFilesInPostRequest(String resource) {
		this();
		this.resource = resource;
	}
}
class InlineDataTooLarge extends S3ClientException {
	def InlineDataTooLarge() {
		super("InlineDataTooLarge", "Inline data exceeds the maximum allowed size.", HttpResponseStatus.BAD_REQUEST);
	}
	def InlineDataTooLarge(String resource) {
		this();
		this.resource = resource;
	}
}
class InternalError extends S3ServerException {
	def InternalError() {
		super("InternalError", "We encountered an internal error. Please try again.", HttpResponseStatus.INTERNAL_SERVER_ERROR);
	}
	def InternalError(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidAccessKeyId extends S3ClientException {
	def InvalidAccessKeyId() {
		super("InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records.", HttpResponseStatus.FORBIDDEN);
	}
	def InvalidAccessKeyId(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidAddressingHeader extends S3ClientException {
	def InvalidAddressingHeader () {
		super("InvalidAddressingHeader", "You must specify the Anonymous role.", null);
	}
	def InvalidAddressingHeader(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidArgument extends S3ClientException {
	def InvalidArgument () {
		super("InvalidArgument", "Invalid Argument", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidArgument(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidBucketName extends S3ClientException {
	def InvalidBucketName () {
		super("InvalidBucketName", "The specified bucket is not valid.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidBucketName(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidBucketState extends S3ClientException {
	def InvalidBucketState () {
		super("InvalidBucketState", "The request is not valid with the current state of the bucket.", HttpResponseStatus.CONFLICT);
	}
	def InvalidBucketState(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidDigest extends S3ClientException {
	def InvalidDigest () {
		super("InvalidDigest", "The Content-MD5 you specified was an invalid.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidDigest(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidLocationConstraint extends S3ClientException {
	def InvalidLocationConstraint () {
		super("InvalidLocationConstraint", "The specified location constraint is not valid. For more information about Regions, see How to Select a Region for Your Buckets.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidLocationConstraint(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidObjectState extends S3ClientException {
	def InvalidObjectState () {
		super("InvalidObjectState", "The operation is not valid for the current state of the object.", HttpResponseStatus.FORBIDDEN);
	}
	def InvalidObjectState(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidPart extends S3ClientException {
	def InvalidPart () {
		super("InvalidPart", "One or more of the specified parts could not be found. The part might not have been uploaded, or the specified entity tag might not have matched the part's entity tag.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidPart(String resource) {
		this();
		this.resource = resource;
	}
}

class InvalidPartOrder extends S3ClientException {
	def InvalidPartOrder () {
		super("InvalidPartOrder", "The list of parts was not in ascending order.Parts list must specified in order by part number.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidPartOrder(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidPayer extends S3ClientException {
	def InvalidPayer () {
		super("InvalidPayer", "All access to this object has been disabled.", HttpResponseStatus.FORBIDDEN);
	}
	def InvalidPayer(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidPolicyDocument extends S3ClientException {
	def InvalidPolicyDocument () {
		super("InvalidPolicyDocument", "The content of the form does not meet the conditions specified in the policy document.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidPolicyDocument(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidRange extends S3ClientException {
	def InvalidRange () {
		super("InvalidRange", "The requested range cannot be satisfied.", HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
	}
	def InvalidRange(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidRequest extends S3ClientException {
	def InvalidRequest () {
		super("InvalidRequest", "SOAP requests must be made over an HTTPS connection.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidRequest(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidSecurity extends S3ClientException {
	def InvalidSecurity () {
		super("InvalidSecurity", "The provided security credentials are not valid.", HttpResponseStatus.FORBIDDEN);
	}
	def InvalidSecurity(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidSOAPRequest extends S3ClientException {
	def InvalidSOAPRequest () {
		super("InvalidSOAPRequest", "The SOAP request body is invalid.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidSOAPRequest(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidStorageClass extends S3ClientException {
	def InvalidStorageClass () {
		super("InvalidStorageClass", "The storage class you specified is not valid.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidStorageClass(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidTargetBucketForLogging extends S3ClientException {
	def InvalidTargetBucketForLogging () {
		super("InvalidTargetBucketForLogging", "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidTargetBucketForLogging(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidToken extends S3ClientException {
	def InvalidToken () {
		super("InvalidToken", "The provided token is malformed or otherwise invalid.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidToken(String resource) {
		this();
		this.resource = resource;
	}
}
class InvalidURI extends S3ClientException {
	def InvalidURI () {
		super("InvalidURI", "Couldn't parse the specified URI.", HttpResponseStatus.BAD_REQUEST);
	}
	def InvalidURI(String resource) {
		this();
		this.resource = resource;
	}
}
class KeyTooLong extends S3ClientException {
	def KeyTooLong () {
		super("KeyTooLong", "Your key is too long.", HttpResponseStatus.BAD_REQUEST);
	}
	def KeyTooLong(String resource) {
		this();
		this.resource = resource;
	}
}
class MalformedACLError extends S3ClientException {
	def MalformedACLError () {
		super("MalformedACLError","The XML you provided was not well-formed or did not validate against our published schema.", HttpResponseStatus.BAD_REQUEST);
	}
	def MalformedACLError(String resource) {
		this();
		this.resource = resource;
	}
}
class MalformedPOSTRequest extends S3ClientException {
	def MalformedPOSTRequest () {
		super("MalformedPOSTRequest", "The body of your POST request is not well-formed multipart/form-data.", HttpResponseStatus.BAD_REQUEST);
	}
	def MalformedPOSTRequest(String resource) {
		this();
		this.resource = resource;
	}
}
class MalformedXML extends S3ClientException {
	def MalformedXML () {
		super("MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema.", HttpResponseStatus.BAD_REQUEST);
	}
	def MalformedXML(String resource) {
		this();
		this.resource = resource;
	}
}
class MaxMessageLengthExceeded extends S3ClientException {
	def MaxMessageLengthExceeded () {
		super("MaxMessageLengthExceeded", "Your request was too big.", HttpResponseStatus.BAD_REQUEST);
	}
	def MaxMessageLengthExceeded(String resource) {
		this();
		this.resource = resource;
	}
}
class MaxPostPreDataLengthExceededError extends S3ClientException {
	def MaxPostPreDataLengthExceededError () {
		super("MaxPostPreDataLengthExceededError", "Your POST request fields preceding the upload file were too large.", HttpResponseStatus.BAD_REQUEST);
	}
	def MaxPostPreDataLengthExceededError(String resource) {
		this();
		this.resource = resource;
	}
}
class MetadataTooLarge extends S3ClientException {
	def MetadataTooLarge () {
		super("MetadataTooLarge", "Your metadata headers exceed the maximum allowed metadata size.", HttpResponseStatus.BAD_REQUEST);
	}
	def MetadataTooLarge(String resource) {
		this();
		this.resource = resource;
	}
}
class MethodNotAllowed extends S3ClientException {
	def MethodNotAllowed () {
		super("MethodNotAllowed", "The specified method is not allowed against this resource.", HttpResponseStatus.METHOD_NOT_ALLOWED);
	}
	def MethodNotAllowed(String resource) {
		this();
		this.resource = resource;
	}
}
class MissingAttachment extends S3ClientException {
	def MissingAttachment () {
		super("MissingAttachment", "A SOAP attachment was expected, but none were found.", null);
	}
	def MissingAttachment(String resource) {
		this();
		this.resource = resource;
	}
}
class MissingContentLength extends S3ClientException {
	def MissingContentLength () {
		super("MissingContentLength", "You must provide the Content-Length HTTP header.", HttpResponseStatus.LENGTH_REQUIRED);
	}
	def MissingContentLength(String resource) {
		this();
		this.resource = resource;
	}
}
class MissingRequestBodyError extends S3ClientException {
	def MissingRequestBodyError () {
		super("MissingRequestBodyError", "Request body is empty.", HttpResponseStatus.BAD_REQUEST);
	}
	def MissingRequestBodyError(String resource) {
		this();
		this.resource = resource;
	}
}
class MissingSecurityElement extends S3ClientException {
	def MissingSecurityElement () {
		super("MissingSecurityElement", "The SOAP 1.1 request is missing a security element.", HttpResponseStatus.BAD_REQUEST);
	}
	def MissingSecurityElement(String resource) {
		this();
		this.resource = resource;
	}
}
class MissingSecurityHeader extends S3ClientException {
	def MissingSecurityHeader () {
		super("MissingSecurityHeader", "Your request was missing a required header.", HttpResponseStatus.BAD_REQUEST);
	}
	def MissingSecurityHeader(String resource) {
		this();
		this.resource = resource;
	}
}
class NoLoggingStatusForKey extends S3ClientException {
	def NoLoggingStatusForKey () {
		super("NoLoggingStatusForKey", "There is no such thing as a logging status sub-resource for a key.", HttpResponseStatus.BAD_REQUEST);
	}
	def NoLoggingStatusForKey(String resource) {
		this();
		this.resource = resource;
	}
}
class NoSuchBucket extends S3ClientException {
	def NoSuchBucket () {
		super("NoSuchBucket", "The specified bucket does not exist.", HttpResponseStatus.NOT_FOUND);
	}
	def NoSuchBucket(String resource) {
		this();
		this.resource = resource;
	}
}
class NoSuchKey extends S3ClientException {
	def NoSuchKey () {
		super("NoSuchKey", "The specified key does not exist.", HttpResponseStatus.NOT_FOUND);
	}
	def NoSuchKey(String resource) {
		this();
		this.resource = resource;
	}
}
class NoSuchLifecycleConfiguration extends S3ClientException {
	def NoSuchLifecycleConfiguration () {
		super("NoSuchLifecycleConfiguration", "The lifecycle configuration does not exist.", HttpResponseStatus.NOT_FOUND);
	}
	def NoSuchLifecycleConfiguration(String resource) {
		this();
		this.resource = resource;
	}
}
class NoSuchUpload extends S3ClientException {
	def NoSuchUpload () {
		super("NoSuchUpload", "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.", HttpResponseStatus.NOT_FOUND);
	}
	def NoSuchUpload(String resource) {
		this();
		this.resource = resource;
	}
}
class NoSuchVersion extends S3ClientException {
	def NoSuchVersion () {
		super("NoSuchVersion", "Indicates that the version ID specified in the request does not match an existing version.", HttpResponseStatus.NOT_FOUND);
	}
	def NoSuchVersion(String resource) {
		this();
		this.resource = resource;
	}
}
class NotImplemented extends S3ClientException {
	def NotImplemented () {
		super("NotImplemented", "A header you provided implies functionality that is not implemented.", HttpResponseStatus.NOT_IMPLEMENTED);
	}
	def NotImplemented(String resource) {
		this();
		this.resource = resource;
	}
}
class NotSignedUp extends S3ClientException {
	def NotSignedUp () {
		super("NotSignedUp", "Your account is not signed up for this service. You must sign up before you can use it.", HttpResponseStatus.FORBIDDEN);
	}
	def NotSignedUp(String resource) {
		this();
		this.resource = resource;
	}
}
class NotSuchBucketPolicy extends S3ClientException {
	def NotSuchBucketPolicy () {
		super("NotSuchBucketPolicy", "The specified bucket does not have a bucket policy.", HttpResponseStatus.NOT_FOUND);
	}
	def NotSuchBucketPolicy(String resource) {
		this();
		this.resource = resource;
	}
}
class OperationAborted extends S3ClientException {
	def OperationAborted () {
		super("OperationAborted", "A conflicting conditional operation is currently in progress against this resource. Please try again.", HttpResponseStatus.CONFLICT);
	}
	def OperationAborted(String resource) {
		this();
		this.resource = resource;
	}
}
class PermanentRedirect extends S3ClientException {
	def PermanentRedirect () {
		super("PermanentRedirect", "The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.", HttpResponseStatus.MOVED_PERMANENTLY);
	}
	def PermanentRedirect(String resource) {
		this();
		this.resource = resource;
	}
}
class PreconditionFailed extends S3ClientException {
	def PreconditionFailed () {
		super("PreconditionFailed", "At least one of the preconditions you specified did not hold.", HttpResponseStatus.PRECONDITION_FAILED);
	}
	def PreconditionFailed(String resource) {
		this();
		this.resource = resource;
	}
}
class Redirect extends S3ClientException {
	def Redirect () {
		super("Redirect", "Temporary redirect.", HttpResponseStatus.TEMPORARY_REDIRECT);
	}
	def Redirect(String resource) {
		this();
		this.resource = resource;
	}
}
class RestoreAlreadyInProgress extends S3ClientException {
	def RestoreAlreadyInProgress () {
		super("RestoreAlreadyInProgress", "Object restore is already in progress.", HttpResponseStatus.CONFLICT);
	}
	def RestoreAlreadyInProgress(String resource) {
		this();
		this.resource = resource;
	}
}
class RequestIsNotMultiPartContent extends S3ClientException {
	def RequestIsNotMultiPartContent () {
		super("RequestIsNotMultiPartContent", "Bucket POST must be of the enclosure-type multipart/form-data.", HttpResponseStatus.BAD_REQUEST);
	}
	def RequestIsNotMultiPartContent(String resource) {
		this();
		this.resource = resource;
	}
}
class RequestTimeout extends S3ClientException {
	def RequestTimeout () {
		super("RequestTimeout", "Your socket connection to the server was not read from or written to within the timeout period.", HttpResponseStatus.BAD_REQUEST);
	}
	def RequestTimeout(String resource) {
		this();
		this.resource = resource;
	}
}
class RequestTimeTooSkewed extends S3ClientException {
	def RequestTimeTooSkewed () {
		super("RequestTimeTooSkewed", "The difference between the request time and the server's time is too large.", HttpResponseStatus.FORBIDDEN);
	}
	def RequestTimeTooSkewed(String resource) {
		this();
		this.resource = resource;
	}
}
class RequestTorrentOfBucketError extends S3ClientException {
	def RequestTorrentOfBucketError () {
		super("RequestTorrentOfBucketError", "Requesting the torrent file of a bucket is not permitted.", HttpResponseStatus.BAD_REQUEST);
	}
	def RequestTorrentOfBucketError(String resource) {
		this();
		this.resource = resource;
	}
}
class SignatureDoesNotMatch extends S3ClientException {
	def SignatureDoesNotMatch () {
		super("SignatureDoesNotMatch", "The request signature we calculated does not match the signature you provided.", HttpResponseStatus.FORBIDDEN);
	}
	def SignatureDoesNotMatch(String resource) {
		this();
		this.resource = resource;
	}
}
class ServiceUnavailable extends S3ServerException {
	def ServiceUnavailable () {
		super("ServiceUnavailable", "Please reduce your request rate.", HttpResponseStatus.SERVICE_UNAVAILABLE);
	}
	def ServiceUnavailable(String resource) {
		this();
		this.resource = resource;
	}
}
class SlowDown extends S3ServerException {
	def SlowDown () {
		super("SlowDown","Please reduce your request rate.", HttpResponseStatus.SERVICE_UNAVAILABLE);
	}
	def SlowDown(String resource) {
		this();
		this.resource = resource;
	}
}
class TemporaryRedirect extends S3ClientException {
	def TemporaryRedirect () {
		super("TemporaryRedirect	You are being redirected to the bucket while DNS updates.", HttpResponseStatus.TEMPORARY_REDIRECT);
	}
	def TemporaryRedirect(String resource) {
		this();
		this.resource = resource;
	}
}
class TokenRefreshRequired extends S3ClientException {
	def TokenRefreshRequired () {
		super("TokenRefreshRequired", "The provided token must be refreshed.", HttpResponseStatus.BAD_REQUEST);
	}
	def TokenRefreshRequired(String resource) {
		this();
		this.resource = resource;
	}
}
class TooManyBuckets extends S3ClientException {
	def TooManyBuckets () {
		super("TooManyBuckets", "You have attempted to create more buckets than allowed.", HttpResponseStatus.BAD_REQUEST);
	}
	def TooManyBuckets(String resource) {
		this();
		this.resource = resource;
	}
}
class UnexpectedContent extends S3ClientException {
	def UnexpectedContent () {
		super("UnexpectedContent", "This request does not support content.", HttpResponseStatus.BAD_REQUEST);
	}
	def UnexpectedContent(String resource) {
		this();
		this.resource = resource;
	}
}
class UnresolvableGrantByEmailAddress extends S3ClientException {
	def UnresolvableGrantByEmailAddress () {
		super("UnresolvableGrantByEmailAddress", "The e-mail address you provided does not match any account on record.", HttpResponseStatus.BAD_REQUEST);
	}
	def UnresolvableGrantByEmailAddress(String resource) {
		this();
		this.resource = resource;
	}
}
class UserKeyMustBeSpecified extends S3ClientException {
	def UserKeyMustBeSpecified () {
		super("UserKeyMustBeSpecified", "The bucket POST must contain the specified field name. If it is specified, please check the order of the fields.", HttpResponseStatus.BAD_REQUEST);
	}
	def UserKeyMustBeSpecified(String resource) {
		this();
		this.resource = resource;
	}
}