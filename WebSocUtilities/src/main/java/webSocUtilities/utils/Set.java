package utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;

public class Set {
	
	
	public static HashSet<String> getSet(String fileName) throws Exception{
		HashSet<String> set = new HashSet<String>();
		BufferedReader reader = new BufferedReader(new FileReader(fileName));
		String line;
		while (null!= (line=reader.readLine())) {
			set.add(line);
		}
		return set;
	}
	
	public static double JaccardDistance(HashSet<String> set1, HashSet<String> set2) {
		HashSet<String> intSet = new HashSet<String>(set1);
		HashSet<String> unionSet = new HashSet<String>(set1);
		
		unionSet.addAll(set2);
		intSet.retainAll(set2);
		
		double intSize = (double) intSet.size();
		double unionSize = (double) unionSet.size();
		
		return intSize/unionSize;
		
	}
	
	public static void main(String[] args) throws Exception{
		if (args.length<2) {
			System.out.println("Atleast 2 files needed");
			System.exit(0);
		}
		
		ArrayList<HashSet<String>> sets = new ArrayList<HashSet<String>>();
		
		for (int i=0;i<args.length;i++) {
			String fileName = "sid_code/"+args[i];
			
			HashSet<String> set = getSet(fileName);
			sets.add(set);
			
		}
		
		for (int i=0;i<args.length-1;i++) {
			double distance = JaccardDistance(sets.get(i), sets.get(i+1));
			System.out.println(args[i]+"  "+args[i+1]+" : "+distance);
		}
		
	}
	
	
}
