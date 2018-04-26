package eu.hobbit.mocha.systems.graphdb;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.query.resultio.sparqljson.SPARQLResultsJSONWriter;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryProvider;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.hobbit.mocha.systems.graphdb.util.Constants;

/**
 * @author Vassilis Papakonstantinou (papv@ics.forth.gr)
 */
public class GraphDBSystemAdapter extends AbstractSystemAdapter {
			
	private static final Logger LOGGER = LoggerFactory.getLogger(GraphDBSystemAdapter.class);
	
	private AtomicInteger totalReceived = new AtomicInteger(0);
	private AtomicInteger totalSent = new AtomicInteger(0);
	private Semaphore allVersionDataReceivedMutex = new Semaphore(0);

	// used to check if bulk loading phase has finished in  order to proceed with the querying phase
	private boolean dataLoadingFinished = false;
	private int loadingNumber = 0;
	private String datasetFolderName;
	public String graphDBContainerName = null;
	
	private RepositoryManager repositoryManager = null;
	private Repository repository = null;
	private RepositoryConnection repositoryConnection = null;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing GraphDB test system...");
        super.init();	
        datasetFolderName = "/graphdb/data/";
        File theDir = new File(datasetFolderName);
		theDir.mkdir();
		
		internalInit();
		
		LOGGER.info("GraphDB initialized successfully .");
    }

	/**
     * Internal initialization function. It builds the graphDB free 8.5 image and runs
     * the container.
	 * @throws InterruptedException 
     * 
     */
    public void internalInit() throws InterruptedException {
    	graphDBContainerName = createContainer("git.project-hobbit.eu:4567/papv/triplestores/graphdb-free:8.5", new String[] { });
		
		// Instantiate a remote repository manager and initialize it
    	repositoryManager = RepositoryProvider.getRepositoryManager("http://" + graphDBContainerName + ":7200");
		repositoryManager.initialize();
		
		// Wait for the GraphDB server to become online
	    LOGGER.info("Waiting for the GraphDB server to become online...");
		boolean serverOnline = false;
	    while(!serverOnline) {
	    	try {
	    		repositoryManager.getAllRepositories();
			    serverOnline = true;
		    } catch (Exception e) {
				TimeUnit.SECONDS.sleep(5);
		    }
	    }
	    LOGGER.info("GraphDB server is now online.");

		// Instantiate a repository graph model
	    TreeModel graph = new TreeModel();
	      
		// Read repository configuration file
	    InputStream config = null;
	    RDFParser rdfParser = null;
	    try {
			config = new FileInputStream("/graphdb/config/repo-config.ttl");
			rdfParser = Rio.createParser(RDFFormat.TURTLE);
			rdfParser.setRDFHandler(new StatementCollector(graph));
			rdfParser.parse(config, RepositoryConfigSchema.NAMESPACE);
		} catch(FileNotFoundException e) {
			LOGGER.error("Configuration file not found.", e);
		} catch (RDFParseException | RDFHandlerException | IOException e) {
			LOGGER.error("An error occured while parsing config file", e);
		} finally {
			try {
				config.close();
			} catch (IOException e) {
				LOGGER.error("An error occured while closing config file input stream.", e);
			}
		}
	    
	    // Retrieve the repository node as a resource
	    Resource repositoryNode =  Models.subject(graph
	    		.filter(null, RDF.TYPE, RepositoryConfigSchema.REPOSITORY))
	    		.orElseThrow(() -> new RuntimeException(
	    				"Oops, no <http://www.openrdf.org/config/repository#mocha-repo> subject found!"));
	    
	    // Create a repository configuration object and add it to the repositoryManager		
	    RepositoryConfig repositoryConfig = RepositoryConfig.create(graph, repositoryNode);
	    repositoryManager.addRepositoryConfig(repositoryConfig);
	    
	    // Get the repository from repository manager, note the repository id set in configuration .ttl file
	    repository = repositoryManager.getRepository("mocha-repo");
	    
	    // Open a connection to this repository
	    repositoryConnection = repository.getConnection();
    }
	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {
		if (dataLoadingFinished == false) {
			ByteBuffer dataBuffer = ByteBuffer.wrap(data);
			String fileName = RabbitMQUtils.readString(dataBuffer);

			// read the data contents
			byte[] dataContentBytes = new byte[dataBuffer.remaining()];
			dataBuffer.get(dataContentBytes, 0, dataBuffer.remaining());
			
			if (dataContentBytes.length != 0) {
				try {
					if (fileName.contains("/")) {
						fileName = fileName.replaceAll("[^/]*[/]", "");
					}
					FileUtils.writeByteArrayToFile(new File(datasetFolderName + File.separator + fileName), dataContentBytes);
				} catch (IOException e) {
					LOGGER.error("Exception while writing data file", e);
				}
			}
			
			if(totalReceived.incrementAndGet() == totalSent.get()) {
				allVersionDataReceivedMutex.release();
			}
		} else {
			ByteBuffer buffer = ByteBuffer.wrap(data);
			String insertQuery = RabbitMQUtils.readString(buffer); 
			
		    Update tupleQuery = repositoryConnection.prepareUpdate(insertQuery);
		    tupleQuery.execute();
		}	
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		if(dataLoadingFinished) {
			LOGGER.info("Task " + tId + " received from task generator");
			
			// read the query
			ByteBuffer buffer = ByteBuffer.wrap(data);
			String queryString = RabbitMQUtils.readString(buffer);
			
			if (queryString.contains("INSERT DATA")) {
				Update tupleQuery = repositoryConnection.prepareUpdate(queryString);
			    tupleQuery.execute();
			    try {
					this.sendResultToEvalStorage(tId, RabbitMQUtils.writeString(""));
				} catch (IOException e) {
					LOGGER.error("Got an exception while sending results.", e);
				}
			} else {
				TupleQuery tupleQuery = repositoryConnection.prepareTupleQuery(QueryLanguage.SPARQL, queryString);
			    ByteArrayOutputStream queryResponseBos = null;
			    try (TupleQueryResult result = tupleQuery.evaluate()) {
			    	queryResponseBos = new ByteArrayOutputStream();
			    	TupleQueryResultHandler writer = new SPARQLResultsJSONWriter(queryResponseBos);
			    	tupleQuery.evaluate(writer);  
			    } catch (Exception e) {
					LOGGER.error("Task " + tId + " failed to execute.", e);
				}

				byte[] results = queryResponseBos.toByteArray();
				LOGGER.info("Task " + tId + " executed successfully.");
				
				try {
					sendResultToEvalStorage(tId, results);
					LOGGER.info("Results sent to evaluation storage.");
				} catch (IOException e) {
					LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
				}
			}		    
		} 
	}
	
	private void loadVersion(String graphURI) {
		LOGGER.info("Loading data on " + graphURI + "...");
		IRI context = repository.getValueFactory().createIRI(graphURI);
		// start a transaction
		repositoryConnection.begin();
		try {
			File[] dataFiles = new File(datasetFolderName).listFiles();
			// Add the first file
			for(File inputFile : dataFiles) {
				repositoryConnection.add(inputFile, graphURI, RDFFormat.NTRIPLES, context);
			}
			repositoryConnection.commit();
		}
		catch (RepositoryException | RDFParseException | IOException e) {
			LOGGER.error("An error occured while loading data on " + graphURI + ". Rolling back...", e);
			repositoryConnection.rollback();
		}
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
    	if (command == Constants.BULK_LOAD_DATA_GEN_FINISHED) {
    		ByteBuffer buffer = ByteBuffer.wrap(data);
            int numberOfMessages = buffer.getInt();
            boolean lastLoadingPhase = buffer.get() != 0;
   			LOGGER.info("Received signal that all data of version " + loadingNumber + " successfully sent from all data generators (#" + numberOfMessages + ")");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
   			// release before acquire, so it can immediately proceed to bulk loading
   			if(totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allVersionDataReceivedMutex.release();
			}
			
			LOGGER.info("Wait for receiving all data of version " + loadingNumber + ".");
			try {
				allVersionDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for all data of version " + loadingNumber + " to be recieved.", e);
			}
			
			LOGGER.info("All data of version " + loadingNumber + " received. Proceed to the loading of such version.");
			loadVersion("http://graph.version." + loadingNumber);
			
			File theDir = new File(datasetFolderName);
			for (File f : theDir.listFiles()) {
				f.delete();
			}
			
			LOGGER.info("Send signal to Benchmark Controller that all data of version " + loadingNumber + " successfully loaded.");
			try {
				sendToCmdQueue(Constants.BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				LOGGER.error("Exception while sending signal that all data of version " + loadingNumber + " successfully loaded.", e);
			}
			
			loadingNumber++;
			dataLoadingFinished = lastLoadingPhase;
    	}
    	super.receiveCommand(command, data);
    }
	
	@Override
    public void close() throws IOException {
		LOGGER.info("Closing System Adapter...");
		
		// Shutdown connection, repository and manager
		if(repositoryConnection != null) {
			repositoryConnection.close();
		}
		if(repository != null) {
			repository.shutDown();
		}
		if(repositoryManager != null) {
			repositoryManager.shutDown();
		}
		
		// stop the graphdb container
		stopContainer(graphDBContainerName);
		
        super.close();
        LOGGER.info("System Adapter closed successfully.");

    }
}
