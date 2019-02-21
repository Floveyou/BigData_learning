package com.share.test;

import com.share.qq.util.DataUtil;
import com.share.qq.util.PropertiesUtil;
import org.junit.Test;

/**
 * Created by Administrator on 2018/9/10.
 */
public class TestProperties {
	@Test
	public void testPropRead(){
		System.out.println(PropertiesUtil.getStringValue("qq.server.channel.blocking.mode"));
	}

	@Test
	public void testNumberUtil(){
		int n= 255 ;
		System.out.println(DataUtil.bytes2Int(DataUtil.int2Bytes(n)));

	}
}
