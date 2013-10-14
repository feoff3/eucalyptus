import com.eucalyptus.config.ConfigurationManager;
import com.eucalyptus.objectstorage.msgs.DeregisterObjectStorageGatewayType;
import com.eucalyptus.objectstorage.msgs.DeregisterObjectStorageGatewayResponseType;
import org.apache.log4j.Logger;
import com.eucalyptus.objectstorage.ObjectStorageGateway;

registrationPartition = "objectstorage";
registrationName = "osg";
LOG = Logger.getLogger(ObjectStorageGateway.class);

def deregisterOSG(String partition, String name) {
  DeregisterObjectStorageGatewayType request = new DeregisterObjectStorageGatewayType();
  request.setPartition(partition);
  request.setName(name);
  try {
    DeregisterObjectStorageGatewayResponseType response = ConfigurationManager.deregisterComponent(request);
    return (response != null) ? response.get_return() : "got null back";
  } catch(Exception e) {
    LOG.debug("Exception deregistering OSG", e);
    return "Exception caught: " + e.getMessage() + " Detail: " + e.getCause();
  }
}

return deregisterOSG(registrationPartition, registrationName);

