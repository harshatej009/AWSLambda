package com.nexus;

public class Mapping {
	private String id;
	private String source;
	private String target;
	private String updatedBy;
	
	
	public Mapping(String id, String source, String target, String updatedBy) {
		super();
		this.id = id;
		this.source = source;
		this.target = target;
		this.updatedBy = updatedBy;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	public String getTarget() {
		return target;
	}
	public void setTarget(String target) {
		this.target = target;
	}
	public String getUpdatedBy() {
		return updatedBy;
	}
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	@Override
	public String toString() {
		return "Mapping [id=" + id + ", source=" + source + ", target=" + target + ", updatedBy=" + updatedBy + "]";
	}
	
}
