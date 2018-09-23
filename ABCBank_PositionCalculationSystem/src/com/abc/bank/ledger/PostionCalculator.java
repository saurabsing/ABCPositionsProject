/*********************************************************************************************************
 * File Name      : PostionCalculator.java
 * Project        : ABC Bank Position Calculation System
 * Description    : Position Calculation process takes start of day positions and transaction files as input, apply transactions
 *  				on positions based on transaction type (B/S) and account type (I/E), and computes end of day positions. Depending on the 
 *  				direction of the transaction (Buy/Sell) each transaction is recorded as debit and credit into external and internal accounts in the “Positions” file.
 *
 * Input          :Positions File: contains start positions for instruments
 *				   Transactions Files: contains transactions for a day
 *
 * Output		  :End of Day Positions file: contains end positions for instruments
 * 				   End of Day Error file: contains error records for positions file instruments,if any
 *				   Prints instruments with largest and lowest net transaction volumes for the day
 *
 * Modification Log
 * Date                 Author                               Description
 * -------------------------------------------------------------------------
 * Sep 23, 2018          Saurabh Singh                         Created
 * -------------------------------------------------------------------------
 *******************************************************************************************************/

package com.abc.bank.ledger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonMappingException;


import com.abc.bank.beans.Instrument;
import com.abc.bank.beans.Transaction;


enum AccountType {E,I  } ;
enum TransactionType {B,S} ;

/**
 * Driver class to load the input files and join daily transactions with daily start positions.
 * Transactions file is in json format and expected to be large in size.
 * Daily start Position file has unique (instrument ,account type) records and so small in size to fit in memory
 * The problem involves - do a left outer join of daily positions  file with bigger transaction file.
 * 
 * -As it is required to find a delta number from all transactions of an instrument based on transaction type (buy or sell)
 *  it is possible to reduce this file to very small file with unique records (instrument name) -> total change in quantity 
 *  consider Buy as +ve change ,Sell as -ve change.Then this result set can be easily stored in memory as a Map.
 *  
 * -HashMap is created as each individual record from transactions is streamed through.
 * 
 * -Stream  through each record in positions file and get delta quantity from Map and update each position based on account type.
 *  output this record to final output file.
 *  
 * -In case of any error in daily positions lines , put these lines/records in an error file for later investigations.
 * 
 * -In case of error in transaction file, halt the program as it could lead to wrong results
 * 
 * @author Saurabh Singh.
 * @version 1.0
 */

public class PostionCalculator {
	
	/** static variables to store maximum,minimum net transaction quantity and corresponding instrument names */	
	private static long maxTxnVolume = 0 ;
	private static long minTxnVolume = Long.MAX_VALUE ;	
	private static String maxVolInstrumentName ;
	private static String minVolInstrumentName ;	
		
	/***
     * To do basic validations,call the position calculation methods and print max ,min net volume instrument names
     * @param transactionsFileName
     */
	public static void main(String[] args) {
		
		/**validation of no. of command line arguments*/
		if (args[0].length() < 4 ) {
			System.out.println("Pass the FOUR command line arguments as Input_StartOfDay_Positions.txt,Input_Transactions.txt,"
					+ "Expected_EndOfDay_Positions.txt,Input_StartOfDay_Positions_Error_Records.txt");
			  System.exit(1);
		}
		/**validation of  file names in command line arguments*/
		if (!args[0].toLowerCase().contains("Input_StartOfDay_Positions.txt".toLowerCase())) {
			System.out.println("Pass the Input_StartOfDay_Positions.txt as first command line parameter.");
			  System.exit(1);
		}
		if (!args[1].toLowerCase().contains("Input_Transactions.txt".toLowerCase())) {
			System.out.println("Pass the Input_Transactions.txt as second command line parameter.");
			  System.exit(1);
		}
		if (!args[2].toLowerCase().contains("Expected_EndOfDay_Positions.txt".toLowerCase())) {
			System.out.println("Pass the Expected_EndOfDay_Positions.txt as third command line parameter.");
			  System.exit(1);
		}
		if (!args[3].toLowerCase().contains("Input_StartOfDay_Positions_Error_Records.txt".toLowerCase())) {
			System.out.println("Pass the Input_StartOfDay_Positions.txt as first command line parameter.");
			  System.exit(1);
		}
		
		
		String startPositionsFileName = args[0]; 		
		String transactionsFileName = args[1];		
		String outputFileName = args[2] ;		
		String errorsOutputFileName = args[3];
		
		/**validation of empty files*/
		if ((new File(startPositionsFileName)).length() == 0 ){				
			 System.out.println("Start of Day Position file does not have data.Validate positions file.");
			  System.exit(1);				
		}
		
		if (new File(transactionsFileName).length() == 0){ 
			  System.out.println("Transaction file doe not have data.Validate transaction file.");
			  System.exit(1);
		  }
		
		
		
		/**call createTransactionsMapper method to create Map of Intruments -> Transaction data object with delta values*/
		Map<String,Transaction> aggregatedTxnPerInstrumentMap = createTransactionsMapper(transactionsFileName);
		
		/**validation on transaction map size*/
		if (aggregatedTxnPerInstrumentMap.size() <= 0) {
			System.out.println("Key/Value Map could not be created from transaction file.Validate transaction file");
			System.exit(1);
		}
			
		/**write new positions to a end of day output file using transaction map lookups*/
		boolean isProcessed = writeInstrumentRecord(startPositionsFileName,outputFileName,errorsOutputFileName, aggregatedTxnPerInstrumentMap) ;
		
		/**If output file generated, Print largest/lowest net transaction volume instruments*/
			if (isProcessed) {
				System.out.println("End of day position calculation for instruments file is completed.");
				if (maxTxnVolume != 0) 
					System.out.println(maxVolInstrumentName + " has largest net transaction volume " + maxTxnVolume);
				if (minTxnVolume != Long.MAX_VALUE) 					
					System.out.println(minVolInstrumentName + "  has lowest net transaction volume  " + minTxnVolume);
				
			}else {
				System.out.println("End of day position calculation process is finished with issues.");
			}
		

	}
	
	
	
	/***
     * To aggregate delta transaction quantity instrument wise and store in a Map.
     * Jackson streaming API is used for processing file as event stream 
     * @param transactionsFileName
     * @return aggregatedTxnPerInstrumentMap
     */

	static Map<String,Transaction> createTransactionsMapper(String transactionsFileName)  
	{
		/**Map to hold aggregated txn volume per instrument */
		Map<String,Transaction> aggregatedTxnPerInstrumentMap = new HashMap<String,Transaction>();
		
		/**Jackson API Json Factory */
		JsonFactory jsonFactory = new JsonFactory();
		JsonParser jsonParser = null;
		 
		try {			
			  /**create JsonParser to parse transaction file */
			  jsonParser = jsonFactory.createParser(new File(transactionsFileName));
				
			  /**declare transaction object and transaction object field values */
				Transaction transactionObject = null;	
				String instrumentName = "";
				String transactionType = "";
				long transactionQuantity = 0;
			    
				JsonToken currentToken = null;
				
			/**loop till END of Json Array is reached as file has an array of Objects */
			while ((currentToken =jsonParser.nextToken()) != JsonToken.END_ARRAY) {
				
				/**token is transaction record object, initialize txn object properties to default values
				 to clear previous txn object values, if any */
				if(currentToken == JsonToken.START_OBJECT) {
					 transactionObject = null;	
					 instrumentName = "";
					 transactionType = "";
					 transactionQuantity = 0;							
				}
				
				/**parse individual field values for current transaction object */
				 while ((currentToken =jsonParser.nextToken()) != JsonToken.END_OBJECT) {
			          //get the current token
					 String fieldname = jsonParser.getCurrentName();
					
			            if("Instrument".equals(fieldname)){
			               jsonParser.nextToken();
			               instrumentName=jsonParser.getText();        	 
			            }
			            if("TransactionType".equals(fieldname)){
			               jsonParser.nextToken();
			               transactionType=jsonParser.getText();        	 
			            }
			            if("TransactionQuantity".equals(fieldname)){
			               jsonParser.nextToken();
			               transactionQuantity=jsonParser.getNumberValue().longValue(); 			               
			            }
			            			            			            
			         }
				
				 /**store parsed values in Transaction object once end of Objcet tag is reached
				   and store it in Map*/
				 if(currentToken == JsonToken.END_OBJECT) {
						
						String key = instrumentName ;
		            	
		            	transactionObject =	aggregatedTxnPerInstrumentMap.get(key);
		            	/**If first appearance of Insturment name, create txn object */
		            	if (transactionObject == null) {
		            		transactionObject = new Transaction();
		            		transactionObject.setInstrumentName(instrumentName);
		            		transactionObject.setTransactionType(transactionType);
		            		
		            		/**If txn type is SELL store it as -ve value */
		            		 if (transactionType.equalsIgnoreCase(TransactionType.S.toString()))
			            			transactionQuantity = -(transactionQuantity) ;				            	
		            		transactionObject.setTransactionQuantity(transactionQuantity);
		            		
		            	} else {
		            		/**If txn object for instrument exists in Map update the total quantity based
		            		  on transaction type Buy/Sell 
		            		  BUY -> add up +ve transaction quantity
		            		  Sell -> add up -ve transaction quantity
		            		  */
		            		if (transactionType.equalsIgnoreCase(TransactionType.B.toString()))
		            			transactionQuantity += transactionObject.getTransactionQuantity() ;				            		
		            		else if (transactionType.equalsIgnoreCase(TransactionType.S.toString()))
		            			transactionQuantity = -(transactionQuantity) + (transactionObject.getTransactionQuantity()) ;	
		            		transactionObject.setTransactionQuantity(transactionQuantity);			            		
		            	}
		            	
		            	/**Store the txn object in a Map  */
		            	aggregatedTxnPerInstrumentMap.put(key, transactionObject) ;			            	
		            
				}
				 
					
			}
			
			
		} catch (JsonParseException ex) {
			System.out.println("Parsing exception while processing transactions file." + ex.getMessage());
			  System.exit(1); 
	      } catch (JsonMappingException ex) {
	    	  System.out.println("Json mapping exception while processing transactions file." + ex.getMessage());
	    	  System.exit(1);
	      }  catch (IOException ex) {
	    	  System.out.println("Json mapping exception while processing transactions file." + ex.getMessage());	    	  
	    	  System.exit(1);
		}  catch (Exception ex) {			
			System.out.println("An Exception generated in streaming transactions record" + ex.getMessage());
			System.exit(1);
		}finally {
			try {
				/**Close the JsonParser at end of txn file*/
				if (jsonParser != null)
					jsonParser.close();
				
			} catch (IOException ex) {
				System.out.println("An Exception generated while closing File Writers." + ex.getMessage());
                 
			}
		}
		
		/**Txn Map with unique (Instrument name) > (total delta quantity )  is stored*/
		return aggregatedTxnPerInstrumentMap;

		
	}
	
	 
	/***
     * To create end of day positions by joining positions and transaction map data and create error position rows if any 
     * @param startPositionsFileName
     * @param outputFileName
     * @param errorsOutputFileName
     * @param aggregatedTxnPerInstrumentMap
     * @return isProcessed
     */
	static boolean writeInstrumentRecord (String startPositionsFileName,String outputFileName,String errorsOutputFileName, Map<String,Transaction> aggregatedTxnPerInstrumentMap) {
		
		/**Declaration for the Reader/Writer for instrument positions input and output files */
		String positionFileLine = "";
		
		FileReader startPositionsFileReader = null;
		BufferedReader startPositionsBufferedReader = null;
		
		BufferedWriter endPositionsBufferedWriter = null;
		FileWriter endPositionsFileWriter = null;
		
		BufferedWriter errorInstRecordBufferedWriter = null;
		FileWriter errorInstRecordFileWriter = null;		
		
		try {				
			
		/**Initialize the Reader/Writer for instrument positions input and output files */
		endPositionsFileWriter = new FileWriter(outputFileName);
		endPositionsBufferedWriter = new BufferedWriter(endPositionsFileWriter);
		
		errorInstRecordFileWriter = new FileWriter(errorsOutputFileName);
		errorInstRecordBufferedWriter = new BufferedWriter(errorInstRecordFileWriter);
					
		startPositionsFileReader = new FileReader(startPositionsFileName);
		startPositionsBufferedReader = new BufferedReader(startPositionsFileReader) ;
		
		/**Ignore the header line */
		positionFileLine = startPositionsBufferedReader.readLine() ; 
		
		/**Instrument object to hold streaming positions record values*/
		Instrument instrumentObject = new  Instrument();
		
		/**Write the header for output positions file */
		endPositionsBufferedWriter.write("Instrument,Account,AccountType,Quantity,Delta");
		
		/**Write the header for output error records file */
		errorInstRecordBufferedWriter.write("Instrument,Account,AccountType,Quantity,Delta");		
		
		/**Read positions file line by line */
		while((positionFileLine = startPositionsBufferedReader.readLine()) != null){
		    
			/**Array to store individual field values */
			String[] positionsArray= null;
			
			String instrumentName = "" ;
			long accountNumber = 0 ;
			String accountType = "";
			long quantity = 0;
				
			/**Split and parse positions line to individual fields */
			try {
			 positionsArray= positionFileLine.split(",");
			
			 instrumentName = positionsArray[0] ;
			 accountNumber = Long.valueOf(positionsArray[1]);
			 accountType = positionsArray[2];
			 quantity = Long.valueOf(positionsArray[3]);
			 
			}
			/**Write the particular row/line of instrument position to an error file in case of any error */
			catch (ArrayIndexOutOfBoundsException ex) {
				System.out.println("An Exception generated while splitting line to field values." + ex.getMessage());
				errorInstRecordBufferedWriter.newLine();
				errorInstRecordBufferedWriter.write(positionFileLine);				
				continue;				
			} catch (NumberFormatException  ex) {
				System.out.println("An Exception generated in parsing to number." + ex.getMessage());
				errorInstRecordBufferedWriter.newLine();
				errorInstRecordBufferedWriter.write(positionFileLine);
				continue;
			}catch (PatternSyntaxException  ex) {
				System.out.println("An Exception generated in splitting pattern." + ex.getMessage());
				errorInstRecordBufferedWriter.newLine();
				errorInstRecordBufferedWriter.write(positionFileLine);
				continue;
			}catch (Exception  ex) {
				System.out.println("An Exception generated in parsing instrument line to object." + ex.getMessage());
				errorInstRecordBufferedWriter.newLine();
				errorInstRecordBufferedWriter.write(positionFileLine);
				continue;
			} finally {
				/**flush any error records to error file */
				errorInstRecordBufferedWriter.flush();
			}
			
			/**store parsed values in instrument object */
			instrumentObject.setInstrumentName(instrumentName);				
			instrumentObject.setAccountNumber(accountNumber);
			instrumentObject.setAccountType(accountType);
			instrumentObject.setQuantity(quantity);
			/**set delta qunatity to 0 */
			instrumentObject.setDeltaQuantity(0);
					
			/**Get the delta of transaction volume for particular instrument form tranasction map
			  and update the instrument positions record with new quantity and the delta based on account type
			  If acc type is E add the delta (as B/S type txn is taken as +ve/-ve respectively while filling txn map) 
			   If acc type is I subtract the delta			 
			  */
			Transaction transactionBuyObject =	aggregatedTxnPerInstrumentMap.get(instrumentName);
        	if (transactionBuyObject != null) {
        		long deltaQuantity = transactionBuyObject.getTransactionQuantity();
                		
        		if (accountType.equalsIgnoreCase(AccountType.E.toString())){ 
        			quantity += deltaQuantity;
        			instrumentObject.setDeltaQuantity(deltaQuantity);
        		}
        		else if  (accountType.equalsIgnoreCase(AccountType.I.toString())) {
        			quantity -= deltaQuantity;
        			instrumentObject.setDeltaQuantity(-deltaQuantity);
        		}
        		
        		instrumentObject.setQuantity(quantity);
        	
        		/**get the Maximum delta txn quantity and corresponding instrument  */    				
        		if (maxTxnVolume < Math.abs(deltaQuantity) ) {
        			maxTxnVolume = Math.abs(deltaQuantity) ;
        			maxVolInstrumentName = instrumentName ;                	
        		}
        		
        		/**get the minimum delta txn quantity and corresponding instrument  */    			
        		if (minTxnVolume > Math.abs(deltaQuantity) ) {
        			minTxnVolume = Math.abs(deltaQuantity) ;
        			minVolInstrumentName = instrumentName ;	
        		}
        		
        		
        	}
			
        	/**write the end position of instrument to output file */			
        	endPositionsBufferedWriter.newLine();    		
        	endPositionsBufferedWriter.write(instrumentObject.toString());
        	endPositionsBufferedWriter.flush();
        	
	}
	
		/**check for any errors and display error message halting execution */		
	} catch (FileNotFoundException e) {
		System.out.println("Start of day positions input file is not found.");
		  System.exit(1);
	} catch (IOException e) {
		System.out.println("An IO excpetion occured while processing start of day positions input file.");
		  System.exit(1);
	}  catch (Exception ex) {			
		System.out.println("An Exception generated in wirting Instrument record method." + ex.getMessage());
		System.exit(1);
	}finally {

		try {
			/**Close all open resources */
			
			if (startPositionsBufferedReader != null)
				startPositionsBufferedReader.close();

			if (endPositionsBufferedWriter != null)
				endPositionsBufferedWriter.close();
			
			if (endPositionsFileWriter != null)
				endPositionsFileWriter.close();			
			
			if (startPositionsFileReader != null)
				startPositionsFileReader.close();
			
			
			if (errorInstRecordFileWriter != null)
				errorInstRecordFileWriter.close();
			
			if (errorInstRecordBufferedWriter != null)
				errorInstRecordBufferedWriter.close();			
			
		} catch (IOException ex) {			
			System.out.println("An IO Exception generated while closing File Writers.");
			
		}			
		
	}
		return true;
        	
		
	}
	

}
