package com.enumeration;

public enum QACOUNTERS {
	SUCCESS("success"),
	ERROR("error");
	
	String name;
	
	QACOUNTERS(String name) {
		this.name = name;
	}
	
//	public String toString() {
//		return name;
//	}
}
