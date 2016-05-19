package com.hbase.api.constant;

import java.util.ResourceBundle;

import com.hbase.api.utils.PropertiesResolve;

public class HbaseConstant {

	private static ResourceBundle config = PropertiesResolve.getProperties("hbase-site");
    public static String QUORUM = config.getString("QUORUM");
}
