package com.cn.me.demo;

import org.junit.Test;

public class WordCountTest {
	@Test
	public void test(){
		try {
			WordCount.main(new String[]{});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
