package com.hbase.api.domain;

import java.util.ArrayList;
import java.util.List;

public class HData {
	private String familyColumu;
	private List<String> nameList = new ArrayList<String>();
	private List<Object> valueList = new ArrayList<Object>();

	public String getFamilyColumu() {
		return familyColumu;
	}

	public void setFamilyColumu(String familyColumu) {
		this.familyColumu = familyColumu;
	}

	public List<String> getNameList() {
		return nameList;
	}

	public void setNameList(List<String> nameList) {
		this.nameList = nameList;
	}

	public List<Object> getValueList() {
		return valueList;
	}

	public void setValueList(List<Object> valueList) {
		this.valueList = valueList;
	}
}
