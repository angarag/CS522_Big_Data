package com.mars.bigdata.hadoop;

public class Util {
	public static Double giveMeLastQuantity(String input) {
		if(input.lastIndexOf(" ")!=-1) {
			
			try { 
				  
				String str= input.substring(input.lastIndexOf(" ")+1);	
	            double val = Double.parseDouble(str); 
	            return val;
	        } 
	        catch (Exception e) { 
	   return null;
	        } 
		}
		else return null;
	}
	public static String giveMeFirstQuantity(String input) {
		String[] arr=input.split(" ");
		if(arr==null)
			return null;
		else return arr[0];
	}
	  public static void main(String[] args){
		  String s = "64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] \"GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1\" 401 12846\n"; 
		  //s="hahah 22";
		  String line=giveMeFirstQuantity(s);
		  System.out.println(line);
		  double d=giveMeLastQuantity(s);
		  System.out.println(d);
	  }
}
