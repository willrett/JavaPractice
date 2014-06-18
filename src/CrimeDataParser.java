/**
 * Crime Data Parser
 * Data Source: datasf.org
 * Crime: Incidence Data
 */

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;


public class CrimeDataParser {

	public String parser (String xmlContent)
	{
		String parsedStr = new String();
		
		try{
			InputSource is = new InputSource(new StringReader(xmlContent));  
	    	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	    	DocumentBuilder db = dbf.newDocumentBuilder();
	    	Document doc = db.parse(is);
	    	doc.getDocumentElement().normalize();
	    	
	    	
	    	NodeList schemaLst = doc.getElementsByTagName("SimpleField");
	    	int schemaNum = schemaLst.getLength();
	    	//System.out.println("Schema Num: " + schemaNum);
	    	if(schemaNum > 0 && schemaLst != null ){
	    		for(int i=0;i<schemaNum;i++) {
	    			Node schemaName = schemaLst.item(i);
	    			Element e = (Element) schemaName;
	    			String name = e.getAttribute("name");
	    			System.out.print(name + ", ");
	    			parsedStr += (name + " ");
	    		}
	    		//System.out.print("x" + " y");
	    		//System.out.println();
	    		parsedStr += ("x"+" y"+ " z"+"\n");
	    	}
	    	 
	    	NodeList placemarkLst = doc.getElementsByTagName("Placemark");
	    	int placemarkNum = placemarkLst.getLength();
	    	//System.out.println("place mark Num = " + placemarkNum);
	    	if(placemarkLst!=null && placemarkNum > 0)	{
	    		for(int i = 0;i<placemarkNum;i++)	{
	    			Node placemark = placemarkLst.item(i);
	    			Element placemarkEle = (Element) placemark;
	    			NodeList simpledataLst = placemarkEle.getElementsByTagName("SimpleData");
	    			NodeList pointLst = placemarkEle.getElementsByTagName("coordinates");
	    			
	    			int simpledataNum = simpledataLst.getLength();
	    			//System.out.println(simpledataNum);
	    			for(int j = 0;j<simpledataNum;j++) {                 

	                    Element dataNode = (Element) simpledataLst.item(j);
	                    String simpledata = dataNode.getChildNodes().item(0).getNodeValue();
	                    String nodeName = dataNode.getAttribute("name");

	                    //System.out.println(simpledata + " " + nodeName);
	                    if (nodeName.equalsIgnoreCase("Category") || 
	                    	nodeName.equalsIgnoreCase("Description"))
	                    	parsedStr += ("\""+simpledata + "\""+", ");
	                    else
	                        parsedStr += simpledata + ", ";
              }

   			
	    			int coordinateNum = pointLst.getLength();
	    			for(int j=0;j<coordinateNum;j++)	{
	    				String coordinate = pointLst.item(j).getChildNodes().item(0).getNodeValue();
	    				System.out.print(coordinate + " ");
	    				parsedStr +=(coordinate + ", ");
	    			}
	    			
	    			System.out.println();
	    			parsedStr +=("\n");
	    		}
	    	}
	    	
		} catch (IOException e) {
			System.err.println(e);
		} catch (Exception e) {
			e.printStackTrace();
    }
    
	return parsedStr;
	}
	
	String readURL(String urlStr)
	{
		String xmlStr = "";
		try {
	        URL url = new URL(urlStr);
	        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));  
	        String response = null;
	        while ((response = in.readLine()) != null){
	        	xmlStr += response;
	        }
	        xmlStr.trim();
	        in.close(); 
	    } catch (MalformedURLException e) {
	    	System.err.println(e);
	    } catch (IOException e) {
	    	System.err.println(e);
	    } catch (Exception e) {
	    	e.printStackTrace();
	    }
	    
	    return xmlStr;
	}

	

	String readFile(String filename)
    {
		String filecontent = new String();

    try{
        FileInputStream fstream = new FileInputStream(filename);
        DataInputStream in = new DataInputStream(fstream);
        	BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String strLine;

        while ((strLine = br.readLine()) != null)   {
          System.out.println (strLine);
          filecontent += strLine;
        	}
        filecontent.trim();
        in.close();
        }catch (Exception e){//Catch exception if any
          System.err.println("Error: " + e.getMessage());
        }
        return filecontent;

    }



	public static void main(String[] args)
	{
		//String url = "http://apps.sfgov.org/datafiles/Police/CrimeIncident90Bayview.kml";
		String file = "CrimeIncident90Bayview[1].txt";
		
		CrimeDataParser datahandler = new CrimeDataParser();
		String readContent = datahandler.readFile(file);
		String finalresult = null;
		
		//System.out.println(readContent);
		finalresult = datahandler.parser(readContent);
		
		//System.out.print(finalresult);

		try{
			FileWriter fstream = new FileWriter("CrimeDataParseOutBayview.txt");
				BufferedWriter out = new BufferedWriter(fstream);
			out.write(finalresult);
			out.close();
		}catch(Exception e){
			System.err.println("Error: " + e.getMessage());
		}
		
	}
	
}
