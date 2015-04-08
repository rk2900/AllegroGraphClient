package knowledgebase;

import java.util.LinkedList;

import com.franz.agraph.jena.AGGraph;
import com.franz.agraph.jena.AGGraphMaker;
import com.franz.agraph.jena.AGModel;
import com.franz.agraph.jena.AGQuery;
import com.franz.agraph.jena.AGQueryExecutionFactory;
import com.franz.agraph.jena.AGQueryFactory;
import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class ClientManagement {
	public static final String SERVER_URL = "http://172.16.2.21:10035";
	public static final String CATALOG_ID = "dbpedia2014";
	public static final String REPOSITORY_ID = "dbpedia";
	public static final String USERNAME = "dbpedia";
	public static final String PASSWORD = "apex";
	public static final String TEMPORARY_DIRECTORY = "";
	
	static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";
	static final String DBPEDIA_NS = "http://dbpedia.org/resource/";
	static final String DBPEDIA_OWL_NS = "http://dbpedia.org/ontology/";
	static final String DBPEDIA_PROP_NS = "http://dbpedia.org/property/";
	
	private static AGModel model = null;
	
	private static LinkedList<AGRepositoryConnection> toCloseConnection = new LinkedList<AGRepositoryConnection>();
	
	protected static void closeBeforeExit(AGRepositoryConnection conn) {
		toCloseConnection.add(conn);
	}
	
	static void close(AGRepositoryConnection conn) {
		try {
			conn.close();
		} catch (Exception e) {
			System.err.println("Error closing repository connection: "+e);
			e.printStackTrace();
		}
	}
	
	protected static void closeAllConnections() {
		for (AGRepositoryConnection conn : toCloseConnection) {
			close(conn);
		}
	}
	
	public static AGModel getAgModel() throws Exception{
		if(ClientManagement.model == null) {
			AGServer server = new AGServer(SERVER_URL, USERNAME, PASSWORD);
			System.out.println("Available catalogs: "+server.listCatalogs());
			AGCatalog catalog = server.getCatalog(CATALOG_ID);
			AGRepository repository = catalog.openRepository(REPOSITORY_ID);
			AGRepositoryConnection connection = repository.getConnection();
			AGGraphMaker maker = new AGGraphMaker(connection);
			System.out.println("\nGot a graph maker for the connection.");
			AGGraph graph = maker.getGraph();
			AGModel model = new AGModel(graph);
			ClientManagement.model = model;
		}
		return model;
	}
	
	public AGRepositoryConnection getConnection() {
		AGRepositoryConnection conn = null;
		if(!toCloseConnection.isEmpty()) {
			return toCloseConnection.getLast();
		} else {
			//TODO
		}
		return conn;
	}
	
	public static ResultSet query(String sparql, boolean visible) {
		ResultSet rs = null;
		try {
			getAgModel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		if (visible) {
			System.out.println("\t-"+sparql);
		}
		AGQuery query = AGQueryFactory.create(sparql);
		QueryExecution qe = AGQueryExecutionFactory.create(query, model);
		rs = qe.execSelect();
		return rs;
	}
	
	public static void clearAll() {
		closeAllConnections();
		ClientManagement.model.close();
	}
	
	public static void main(String[] args) throws Exception {
		ResultSet results;
		
		AGModel model = ClientManagement.getAgModel();
		
		try {
			String queryString = "SELECT ?s ?p  WHERE {"
					+ "?s ?p <http://en.wikipedia.org/wiki/Netherlands>.} LIMIT 10";
			AGQuery sparql = AGQueryFactory.create(queryString);
			QueryExecution qe = AGQueryExecutionFactory.create(sparql, model);
			try {
				results = qe.execSelect();
//				while (results.hasNext()) {
//					QuerySolution result = results.next();
//					RDFNode s = result.get("s");
//					RDFNode p = result.get("p");
//					// System.out.format("%s %s %s\n", s, p, o);
//					System.out.println(s + "\t" + p);
//				}
			} finally {
				qe.close();
			}
		} finally {
			model.close();
		}
		
		//test print after close
		while(results.hasNext()) {
			QuerySolution result = results.next();
			RDFNode s = result.get("s");
			System.out.println(s);
		}
		
	}

}
