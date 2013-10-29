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

package com.eucalyptus.objectstorage.entities;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.PersistenceContext;
import javax.persistence.Table;

import org.apache.log4j.Logger;
import org.hibernate.annotations.Cache;
import org.hibernate.annotations.CacheConcurrencyStrategy;
import org.hibernate.annotations.OptimisticLockType;
import org.hibernate.annotations.OptimisticLocking;
import com.eucalyptus.storage.msgs.s3.AccessControlPolicy;

@Entity
@OptimisticLocking(type = OptimisticLockType.NONE)
@PersistenceContext(name="eucalyptus_osg")
@Table( name = "objects" )
@Cache( usage = CacheConcurrencyStrategy.TRANSACTIONAL )
public class ObjectEntity extends S3AccessControlledEntity implements Comparable {	
	@Column( name = "object_key" )
    private String objectKey;

    @Column( name = "bucket_name" )
    private String bucketName;
    
    @Column(name="version_id")
    private String versionId; //VersionId is required to uniquely identify ACLs and auth

    @Column(name="size")
    private Long size;

    @Column(name="storage_class")
    private String storageClass;

    @Column(name="is_deleted")
    private Boolean deleted;
    
    @Column(name="is_last")
    private Boolean last;
            
    /**
     * Used to denote the object as a snapshot, for special access-control considerations.
     */
    @Column(name="is_snapshot")
    private Boolean isSnapshot;
 
 
    private static Logger LOG = Logger.getLogger( ObjectEntity.class );
    
    public ObjectEntity() {}

    public ObjectEntity(String bucketName, String objectKey, String versionId) {
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.versionId = versionId;
    }
    
	@Override
	protected String getResourceFullName() {
		return getBucketName() + "/" + getObjectKey();
	}

    public String getObjectKey() {
        return objectKey;
    }

    public void setObjectKey(String objectKey) {
        this.objectKey = objectKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public String getStorageClass() {
        return storageClass;
    }

    public void setStorageClass(String storageClass) {
        this.storageClass = storageClass;
    }
    
    /*
    public Map<String, String> getUserMetadataMap() throws Exception {    	    	
    	JSONObject jsonMap = (JSONObject)JSONSerializer.toJSON(this.getUserMetadata());
    	return (Map<String,String>)jsonMap;
    }
    */
    
    public Boolean getDeleted() {
		return deleted;
	}

	public void setDeleted(Boolean deleted) {
		this.deleted = deleted;
	}

	public String getVersionId() {
		return versionId;
	}

	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}

	public int compareTo(Object o) {
        return this.objectKey.compareTo(((ObjectEntity)o).getObjectKey());
    }

	
	public Boolean getLast() {
		return last;
	}

	public void setLast(Boolean last) {
		this.last = last;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((bucketName == null) ? 0 : bucketName.hashCode());
		result = prime * result
				+ ((objectKey == null) ? 0 : objectKey.hashCode());
		result = prime * result
				+ ((versionId == null) ? 0 : versionId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ObjectEntity other = (ObjectEntity) obj;
		if (bucketName == null) {
			if (other.bucketName != null)
				return false;
		} else if (!bucketName.equals(other.bucketName))
			return false;
		if (objectKey == null) {
			if (other.objectKey != null)
				return false;
		} else if (!objectKey.equals(other.objectKey))
			return false;
		if (versionId == null) {
			if (other.versionId != null)
				return false;
		} else if (!versionId.equals(other.versionId))
			return false;
		return true;
	}

	public Boolean getIsSnapshot() {
		return isSnapshot;
	}

	public void setIsSnapshot(Boolean isSnapshot) {
		this.isSnapshot = isSnapshot;
	}

	public void setResourceAcl(AccessControlPolicy policy) {
		
	}
}
