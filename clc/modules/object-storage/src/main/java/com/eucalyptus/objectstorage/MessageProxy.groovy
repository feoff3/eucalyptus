package com.eucalyptus.objectstorage

import com.eucalyptus.objectstorage.msgs.ObjectStorageRequestType
import edu.ucsb.eucalyptus.msgs.BaseMessage
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.groovy.runtime.typehandling.GroovyCastException

/**
 * Based on CompositeHelper
 * Converts messages from ObjectStorageRequestType to another message type. Both must
 * have BaseMessage as the base class.
 *
 */
public class MessageProxy<T extends BaseMessage> {
	private static Logger LOG = Logger.getLogger( MessageProxy.class );

	private static final def baseMsgProps = BaseMessage.metaClass.properties.collect{ p -> p.name };

	/**
	 * Clones the source to dest on a property-name basis.
	 * Requires that both source and dest are not null. Will not
	 * set values to null in destination that are null in source
	 * @param source
	 * @param dest
	 * @return
	 */
	public static <O extends BaseMessage, I extends BaseMessage> O mapExcludeNulls( I source, O dest ) {
		def props = dest.metaClass.properties.collect{ p -> p.name };
		source.metaClass.properties.findAll{ it.name != "metaClass" && it.name != "class" && !baseMsgProps.contains(it.name) && props.contains(it.name) && source[it.name] != null }.each{ sourceField ->
			LOG.debug("${source.class.simpleName}.${sourceField.name} as ${dest.class.simpleName}.${sourceField.name}=${source[sourceField.name]}");
			try {
				dest[sourceField.name]=source[sourceField.name];
			} catch(GroovyCastException e) {
				LOG.trace("Cannot cast class. ", e);
			} catch(ReadOnlyPropertyException e) {
				LOG.trace("Cannot set readonly property.",e);
			}
		}
		return dest;
	}

	/**
	 * Clones the source to dest on a property-name basis.
	 * Requires that both source and dest are not null. Will
	 * include null values in the mapping.
	 * @param source
	 * @param dest
	 * @return
	 */	
	public static <O extends BaseMessage, I extends BaseMessage> O mapWithNulls( I source, O dest ) {
		def props = dest.metaClass.properties.collect{ p -> p.name };
		source.metaClass.properties.findAll{ it.name != "metaClass" && it.name != "class" && !baseMsgProps.contains(it.name) && props.contains(it.name) }.each{ sourceField ->
			LOG.debug("${source.class.simpleName}.${sourceField.name} as ${dest.class.simpleName}.${sourceField.name}=${source[sourceField.name]}");
			try {
				dest[sourceField.name]=source[sourceField.name];
			} catch(GroovyCastException e) {
				LOG.trace("Cannot cast class. ", e);
			} catch(ReadOnlyPropertyException e) {
				LOG.trace("Cannot set readonly property.",e);
			}
		}
		return dest;
	}


}
