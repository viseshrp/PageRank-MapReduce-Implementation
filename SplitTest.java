package com.asgn3;

public class SplitTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String[] urlPRAndOutlinkList = "1url_split_delimiter	0.0,,,,,3#####5#####".split("url_split_delimiter");
		System.out.println(urlPRAndOutlinkList[0]);
		
		System.out.println(urlPRAndOutlinkList[1].trim());
		
		//String prAndOutlinkList = urlPRAndOutlinkList[1].trim();

	}

}
