package com.eucalyptus.objectstorage;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple type for describing paginated listings
 *
 * @param <T>
 */
public class PaginatedResult<T> {
	protected List<T> entityList;
	protected List<String> commonPrefixes;
	protected boolean isTruncated;
	
	public PaginatedResult() {
		this.entityList = new ArrayList<T>();
		this.commonPrefixes = new ArrayList<String>();
		this.isTruncated = false;
	}
	
	public PaginatedResult(List<T> entries, List<String> commonPrefixes, boolean truncated) {
		this.entityList = entries;
		this.commonPrefixes = commonPrefixes;
		this.isTruncated = truncated;
	}
	
	public List<T> getEntityList() {
		return entityList;
	}
	public void setEntityList(List<T> entityList) {
		this.entityList = entityList;
	}
	public List<String> getCommonPrefixes() {
		return commonPrefixes;
	}
	public void setCommonPrefixes(List<String> commonPrefixes) {
		this.commonPrefixes = commonPrefixes;
	}
	
	public boolean getIsTruncated() {
		return this.isTruncated;		
	}
	
	public void setIsTruncated(boolean t) {
		this.isTruncated = t;
	}
}