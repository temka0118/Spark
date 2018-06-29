package kAnonymity2RDF;

import java.io.IOException;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;

public class kAnonymityMinGenRDF {
	
	
	public static void main(String[] args) throws IOException {
		readRdf();
	}
	
	
	public static void readRdf() {
		

		try { 
			   Model model = FileManager.get().loadModel("ZCTA.ttl");
			   StmtIterator iter = model.listStatements();
			
			   while (iter.hasNext(  )) {

				   	Statement stmt = iter.next();
				   	System.out.println(stmt);
				   	Resource res2 = stmt.getSubject(  );
				    Property prop = stmt.getPredicate(  );
				    RDFNode node = stmt.getObject(  );
				    
				    
			   }
			} 
		 	catch (Exception e) {
			            System.out.println("Failed: " + e);
		   }
	}
}
