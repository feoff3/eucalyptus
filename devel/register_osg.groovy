import com.eucalyptus.config.ConfigurationManager;
import com.eucalyptus.objectstorage.msgs.RegisterObjectStorageGatewayType;
import com.eucalyptus.objectstorage.msgs.RegisterObjectStorageGatewayResponseType;
import org.apache.log4j.Logger;
import com.eucalyptus.objectstorage.ObjectStorageGateway;

registrationHost = "10.111.5.44";
registrationPartition = "objectstorage";
registrationName = "osg";
LOG = Logger.getLogger(ObjectStorageGateway.class);

def registerOSG(String host, String partition, String name) {
  RegisterObjectStorageGatewayType request = new RegisterObjectStorageGatewayType();
  request.setPartition(partition);
  request.setHost(host);
  request.setName(name);
  request.setPort(8773);
  try {
    RegisterObjectStorageGatewayResponseType response = ConfigurationManager.registerComponent(request);
    return (response != null) ? response.get_return() : "got null back";
  } catch(Exception e) {
    LOG.debug("Exception registering OSG", e);
    return "Exception caught: " + e.getMessage() + " Detail: " + e.getCause();
  }
}

return registerOSG(registrationHost, registrationPartition, registrationName);

