package com.asgn3;

import java.util.regex.Pattern;

import org.apache.hadoop.hdfs.server.balancer.Matcher;

public class PatternTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
	
		String str = "<title>1</title> <text>[[3[[5]]]][[5]]</text>";
		
		Pattern pattern0 = Pattern.compile("<title>(.*?)</title>");
		java.util.regex.Matcher matcher0 = pattern0.matcher(str);
		while (matcher0.find()) {
			System.out.println("title= "+matcher0.group(1));
		}

		Pattern pattern = Pattern.compile("<text>(.*?)</text>");
		java.util.regex.Matcher matcher = pattern.matcher(str);
		
		String prAndURLList = "PRInit" + ",";
		while (matcher.find()) {
			String str1 = matcher.group(1);
			Pattern pattern1 = Pattern.compile("\\[\\[(.*?)\\]\\]");
			java.util.regex.Matcher matcher1 = pattern1.matcher(str1);
			while (matcher1.find()) {
				String url = matcher1.group(1).replace("[[", "").replace("]]", "");
				prAndURLList +=  url + "#####";
			 }

			//System.out.println("found");
			//System.out.println(matcher.group(1));
//			String str1 = matcher.group(1);
//			Pattern pattern1 = Pattern.compile("[[(.*?)]]");
//			java.util.regex.Matcher matcher1 = pattern1.matcher(str1);
//			while (matcher1.find()) {
//			System.out.println(matcher1.group(1));
//			 }
		}
		//prAndURLList += "#####";

		System.out.println(prAndURLList);

	}
}
