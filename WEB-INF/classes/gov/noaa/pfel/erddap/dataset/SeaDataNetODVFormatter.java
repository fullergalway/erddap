package gov.noaa.pfel.erddap.dataset;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cohort.array.ByteArray;
import com.cohort.array.CharArray;
import com.cohort.array.DoubleArray;
import com.cohort.array.FloatArray;
import com.cohort.array.IntArray;
import com.cohort.array.LongArray;
import com.cohort.array.PrimitiveArray;
import com.cohort.array.ShortArray;
import com.cohort.array.StringArray;
import com.cohort.util.Calendar2;
import com.cohort.util.SimpleException;
import com.cohort.util.String2;

import gov.noaa.pfel.coastwatch.pointdata.Table;
import gov.noaa.pfel.erddap.util.EDStatic;
import gov.noaa.pfel.erddap.variable.EDV;
// TODO Add these lines to EDDTable.java
/*
            else if (fileTypeName.equals(".SDNodvTxt")) 
                SeaDataNetODVFormatter.saveAsSeaDataNetODV(outputStreamSource, twawm, datasetID, publicSourceUrl(), 
                    infoUrl(), reallyVerbose);
*/

public class SeaDataNetODVFormatter {
	static String getGeneralDapHtmlInstructions(final String tErddapUrl){
		return             
			"  <p><strong><a rel=\"bookmark\" href=\"https://www.seadatanet.org/\">SeaDataNet (SDN) <strong>" +
            "<a rel=\"bookmark\" href=\"https://odv.awi.de/\">Ocean Data View" +
                    EDStatic.externalLinkHtml(tErddapUrl) + "</a> .SDNodvTxt</strong>\n" +
            "   - <a class=\"selfLink\" id=\"ODV\" href=\"#ODV\" rel=\"bookmark\">SDN ODV</a> users can download profile, time-series and trajectory data in a\n" +
            "    <a rel=\"help\" href=\"href=\"https://www.seadatanet.org/Standards/Data-Transport-Formats\">SDN compliant ODV Spreadsheet Format .txt file" +
                    EDStatic.externalLinkHtml(tErddapUrl) + "</a>\n" +
            "  by requesting tabledap's .SDNodvTxt fileType.\n" +
            "  Only Data Variables that have been marked up with SeaDataNet attributes in the dataset metadata will be loaded into the file.\n" +
            "  After the resulting file (with the extension .txt) has downloaded to your computer:\n" +
            "  <ol>\n" +
            "  <li>Open ODV.\n" +
            "  <li>Use <kbd>Import : SeaDataNet Formats...</kbd>.\n" +
            "    <ul>\n" +
            "    <li>Browse to select the .txt file(s) you created in ERDDAP and click on <kbd>Open</kbd>.\n" + 
            "    <li>Once the Import is successful click on <kbd>Open</kbd>.\n" +
            "      The location of the data in the file(s) should now be visible on a map in ODV.\n" +
            "    </ul>\n" +
            "  <li>See ODV's <kbd>Help</kbd> menu for more help using ODV.\n" +
            "  </ol>\n" +
            "\n";
		
	}

    /**
     * Find a column having one the given names, or if not then try ignoring case, and if
     * no match then try for a column with a name containing one of the given names.
     * @param table
     * @param names
     * @return
     */
    static int findOrGuessColumn(Table table, String ... names){
         for(int i=0;i<names.length;i++){
              if(names[i] == null){
                continue;
              }
              int col = table.findColumnNumber(names[i]);
              if(col >=0) return col;
         }
        int nCols = table.nColumns();
        for (int col = 0; col < nCols; col++) {
            String lcColName = table.getColumnName(col).toLowerCase();
            for(int i=0;i<names.length;i++){
                if (names[i] != null && lcColName.equals(names[i].toLowerCase())){
                   return col;
                }
            }
        }
        for (int col = 0; col < nCols; col++) {
            String lcColName = table.getColumnName(col).toLowerCase();
            for(int i=0;i<names.length;i++){
                if (names[i] != null && lcColName.indexOf(names[i].toLowerCase()) >=0 ){
                   return col;
                }
            }
        }
        return -1;
    }	
	    /**
     * Data type decoding method shared between the default and SeaDataNet ODV implementations.
     * @param pa
     * @param colName
     * @return
     * @throws SimpleException
     */
    private static String getODVType(PrimitiveArray pa, String colName) throws SimpleException{
        String type = null;
        if      (pa instanceof ByteArray)   type = "BYTE";
        else if (pa instanceof CharArray)   type = "TEXT:2"; //2017-04-21 was SHORT
        else if (pa instanceof ShortArray)  type = "SHORT";   //!
        else if (pa instanceof IntArray)    type = "INTEGER";
        else if (pa instanceof LongArray)   type = "DOUBLE";  //!
        else if (pa instanceof FloatArray)  type = "FLOAT";
        else if (pa instanceof DoubleArray) type = "DOUBLE";
        //maxStringLenth + 1 byte used to hold string length
        //since 1 byte used for string length, max length must be 255
        else if (pa instanceof StringArray) type = "TEXT:" +
            Math.min(255, ((StringArray)pa).maxStringLength() + 1);
        else throw new SimpleException(EDStatic.errorInternal +
            "Unexpected data type=" + pa.elementClassString() +
            " for column=" + colName + ".");
        return type;
    }

    
	    /**
     * Implemented to append data or metadata onto the SeaDataNet Ocean Data View output.
     * @author rfuller
     *
     */
    private static interface ODVAppender{
    	/**
    	 * Append a value to the output.
    	 * @param sb
    	 * @param row
    	 * @param useCache
    	 * @throws java.io.IOException
    	 */
       void append(StringBuilder sb, int row, boolean useCache) throws java.io.IOException;
    }
     private static ODVAppender getLowerCaseODVAppender(final ODVAppender appender){
    	 return (sb,i,b) -> {
    		final StringBuilder sb1 = new StringBuilder();
			appender.append(sb1, i, b);
			sb.append(sb1.toString().toLowerCase());
    	 };

    }
    /**
     * 
     * @param table
     * @param pas
     * @param usedCols
     * @param headers
     * @param valueAttr
     * @param variableAttr
     * @param defaultValue
     * @param suggestedVariables
     * @return
     */
    private static ODVAppender getODVMetadataAppender(Table table, PrimitiveArray[] pas, List<Integer> usedCols, List<Integer> keyCols, StringBuilder headers,
           String valueAttr, String variableAttr, String defaultValue, String ... suggestedVariables){

        if(valueAttr != null && table.globalAttributes().getString(valueAttr) != null){
          final String value = table.globalAttributes().getString(valueAttr);
          return (sb,i,b) -> sb.append(value);
        }
        String variable = null;
        if(variableAttr != null)
            variable = table.globalAttributes().getString(variableAttr);

        if(variable == null){
            headers.append("// warning: no value found for global attribute "+(valueAttr==null?"":valueAttr)
                +((valueAttr!=null && variableAttr !=null)?" nor ": " ") + (variableAttr==null?"":variableAttr)).append("\r\n");
        }

        List<String> variables = new ArrayList<String>(Arrays.asList(suggestedVariables));
        variables.add(0,variable);
        int col = findOrGuessColumn(table, (String[]) variables.toArray(new String[variables.size()]));
        if (col < 0)
           return (sb,i,b) -> sb.append(defaultValue);

        usedCols.add(col);
        if(keyCols != null)
        	keyCols.add(col);
        return (sb,i,b) -> sb.append(pas[col].getTsvString(i)); //a json string
    }
    
    private static String getOdvMetadataSignature(String metadata, final boolean isTimePrimaryVar) {
		String signature = metadata;
		if(isTimePrimaryVar) {
			// remove the time element (4th col)
			final String[] parts = signature.split("\t");
			parts[3] = null;
			signature = String.join("\t",Arrays.stream(parts).filter(s -> s != null).toArray(String[]::new));
		}
		return signature;
	}
    /**
     * Writes the output in SeaDataNet ODV format. This output method is activated if the administrator
     * has set the SDN_EDMO_CODE attribute, and the user has requested ODV data format.
     * 
     * @param outputStreamSource
     * @param twawm  all the results data, with missingValues stored as destinationMissingValues
     *    or destinationFillValues  (they are converted to NaNs)
     * @param tDatasetID
     * @param tPublicSourceUrl
     * @param tInfoUrl
     * @throws Throwable 
     */
    public static void saveAsSeaDataNetODV(OutputStreamSource outputStreamSource,
         TableWriterAll twawm, final String tDatasetID, final String tPublicSourceUrl, final String tInfoUrl, final boolean reallyVerbose) throws Throwable {
        if (reallyVerbose) String2.log("EDDTable.saveAsSeaDataNetODV");
        long time = System.currentTimeMillis();
        final String sdnEdmoCode = twawm.globalAttributes().getString("SDN_EDMO_CODE");
        if(sdnEdmoCode == null){
            throw new SimpleException(EDStatic.errorInternal +
                "saveAsSeaDataNetODV error: SDN_EDMO_CODE not in defined as a global attribute.");
        }

        final String timeFormatMask = twawm.globalAttributes().getString("SDN_ISO8601_format") == null?
        		"YYYY-MM-DDThh:mm:ss.sss" : twawm.globalAttributes().getString("SDN_ISO8601_format");
        final String timeP01 = twawm.globalAttributes().getString("SDN_ISO8601_parameter_urn") == null?
        		"SDN:P01::DTUT8601" : twawm.globalAttributes().getString("SDN_ISO8601_parameter_urn");
        final String timeP06 = twawm.globalAttributes().getString("SDN_ISO8601_units_urn") == null?
        		"SDN:P06::TISO" : twawm.globalAttributes().getString("SDN_ISO8601_units_urn");

        List<ODVAppender> appenders = new ArrayList<ODVAppender>();
        //make sure there isn't too much data before getting outputStream
        Table table = twawm.cumulativeTable(); //it checks memory usage
        twawm.releaseResources();        
        final int lonCol = table.findColumnNumber(EDV.LON_NAME);
        final int latCol = table.findColumnNumber(EDV.LAT_NAME);
        final int timeCol = table.findColumnNumber(EDV.TIME_NAME);

        //ensure there is longitude, latitude, time data in the request (else it is useless in ODV)
        if ( lonCol  < 0 || latCol < 0 || timeCol < 0)
            throw new SimpleException(EDStatic.queryError +
                MessageFormat.format(EDStatic.queryErrorLLT, ".SDNodvTxt"));

        
        int nCols = table.nColumns();
        int nRows = table.nRows();
        PrimitiveArray pas[] = new PrimitiveArray[nCols];
        for (int col = 0; col < nCols; col++) {
            pas[col] = table.getColumn(col);
        }
        


        Map<Integer,String> colHeaderMap = new HashMap<Integer,String>();
        final boolean isTimeStamp[] = new boolean[nCols];  //includes time var
        final String units[] = new String[nCols]; //includes time var
        final String colNames[] = new String[nCols]; //includes time var
        final String localNames[] = new String[nCols]; //includes time var
        final String labels[] = new String[nCols]; //includes time var
        final String output_format[] = new String[nCols]; //includes time var
        final boolean[] isQualityCheck = new boolean[nCols];
        final int[] qcCol = new int[nCols];
        final String[] qcValue = new String[nCols];
        List<Integer> usedCols = new ArrayList<Integer>();
        
        StringBuilder headers = new StringBuilder();
        List<Integer> keyColumns = new ArrayList<Integer>();
        final ODVAppender localCdiIdAppender = getODVMetadataAppender(table, pas, usedCols,  keyColumns, headers, 
                "SDN_LOCAL_CDI_ID", "SDN_LOCAL_CDI_ID_variable", "Missing_SDN_LOCAL_CDI_ID");
        final ODVAppender lcLocalCdiIdAppender = getLowerCaseODVAppender(localCdiIdAppender);
        final ODVAppender edmoCodeAppender = getODVMetadataAppender(table, pas, usedCols, null, headers,
                "SDN_EDMO_CODE", "SDN_EDMO_variable", "Missing_SDN_EDMO_CODE");
        table.sort(keyColumns.stream().mapToInt(i -> i).toArray());
        { // ADD sdn_references. Usually only one, but could be more.
        	StringBuilder sb = new StringBuilder();
        	// put sdn_reference at the top of the headers..
        	String tmpHeaders = headers.toString();
        	headers.setLength(0);
        	List<String> references = new ArrayList<String>();
            for (int row = 0; row < nRows; row++) {
                sb.setLength(0);
                sb.append("//<sdn_reference xlink:type=\"SDN:L23::CDI\" xlink:role=\"isDescribedBy\"");
                sb.append(" xlink:href=\"https://cdi.seadatanet.org/report/edmo/");
                edmoCodeAppender.append(sb, row, false);
                sb.append("/");
                lcLocalCdiIdAppender.append(sb, row, false);
                if(nRows > 1) {
                	sb.append("\" sdn:scope=\"");
                	edmoCodeAppender.append(sb, row, false);
                	sb.append(":");
                	localCdiIdAppender.append(sb, row, false);
                }
                sb.append("\"/>\r\n");
	            if(!references.contains(sb.toString())) {
	            	references.add(sb.toString());
	            	headers.append(sb.toString());
	            }
	        }
            // license
            if(table.globalAttributes().getString("licenseUrl") != null) {
            	headers.append("//<sdn_reference xlink:type=\"SDN:L23::DTLCNC\" xlink:role=\"isDescribedBy\" xlink:href=\"");
            	headers.append(table.globalAttributes().getString("licenseUrl"));
            	headers.append("\"/>\r\n");
            }
            headers.append(tmpHeaders);
        }

        List<Integer> missingConfigs = new ArrayList<Integer>();

        for (int col = 0; col < nCols; col++) {
            qcCol[col] = -1;
            qcValue[col] = "0";
            StringBuilder sb = new StringBuilder();
            String colName = table.getColumnName(col);
            colNames[col] = colName;
            String long_name = table.columnAttributes(col).getString("long_name");
            String standard_name = table.columnAttributes(col).getString("long_name");
            localNames[col] = (long_name == null? (standard_name == null ? colName : standard_name) : long_name);
            String sdnP01 = table.columnAttributes(col).getString("sdn_parameter_urn");
            String sdnP06 = table.columnAttributes(col).getString("sdn_units_urn");
            String sdnL22 = table.columnAttributes(col).getString("sdn_instrument_urn");
            String sdnL33 = table.columnAttributes(col).getString("sdn_fall_rate_urn");
            String sformat = table.columnAttributes(col).getString("sdn_odv_format");
            String sdnOutputP01 = table.columnAttributes(col).getString("sdn_odv_parameter_urn");
            String sdnOutputP06 = table.columnAttributes(col).getString("sdn_odv_units_urn");
            String attrUnits = table.columnAttributes(col).getString("units");
            isTimeStamp[col] = (col == timeCol) || EDV.TIME_UNITS.equals(attrUnits); //units may be null

            if(isTimeStamp[col]) {
            	output_format[col] = sformat == null? timeFormatMask : sformat;
            	if(Calendar2.epochSecondsToLimitedIsoStringT(output_format[col], 1563191147868.0, "bad") == "bad"){
                    headers.append("// WARNING: invalid sdn_odv_format ["+output_format[col]+"] for "+colName+"\r\n");
                    output_format[col] = "YYYY-MM-DDThh:mm:ss.sss";
            	}
            	sdnP01 = sdnOutputP01 == null ? timeP01 : sdnOutputP01;
            	sdnP06 = sdnOutputP06 == null ? timeP06 : sdnOutputP06;
            }else if(sformat != null){
                try{
                   String.format(sformat,1.0);
                   output_format[col] = sformat;
                }catch(Exception e){
                    headers.append("// WARNING: invalid sdn_odv_format ["+sformat+"] for "+colName+"\r\n");
                }
            	sdnP01 = sdnOutputP01 == null ? sdnP01 : sdnOutputP01;
            	sdnP06 = sdnOutputP06 == null ? sdnP06 : sdnOutputP06;
            }
            sb.append("//<subject>SDN:LOCAL:");
            sb.append(localNames[col]);
            sb.append("</subject>");
            sb.append("<object>");
            sb.append(sdnP01 == null? "Missing sdn_instrument_urn attribute": (sdnP01));
            sb.append("</object>");
            sb.append("<units>");
            sb.append(sdnP06 == null? "Missing sdn_fall_rate_urn attribute" : (sdnP06));
            sb.append("</units>");
            if(sdnL22 != null)
                sb.append("<instrument>").append(sdnL22).append("</instrument>");
            if(sdnL33 != null)
                sb.append("<fall_rate>").append(sdnL33).append("</fall_rate>");
            sb.append("\r\n");
            colHeaderMap.put(col,sb.toString());
            labels[col] = table.columnAttributes(col).getString("sdn_label");
            
            if (attrUnits == null || attrUnits.length() == 0) {
                attrUnits = "";
            } else {
                //ODV doesn't care about units standards. UDUNITS or UCUM are fine
                //ODV doesn't allow internal brackets; 2010-06-15 they say use parens
                attrUnits = String2.replaceAll(attrUnits, '[', '(');
                attrUnits = String2.replaceAll(attrUnits, ']', ')');
                attrUnits = " [" + attrUnits + "]";
            }
            units[col] = attrUnits;
            

            if(sdnP01 == null || sdnP06 == null) {
            	missingConfigs.add(col);
            	continue;
            }
            
            // Quality Check Column
            String qcColName = table.columnAttributes(col).getString("sdn_qc_variable");
            if(qcColName != null){
               int q = table.findColumnNumber(qcColName);
               if(q < 0){
                  headers.append("// WARNING: a qc column for "+colName+".sdn_qc_variable="+qcColName+" was not found in the data\r\n");
               }else{
                  isQualityCheck[q] = true;
                  qcCol[col] = q;
               }
            }
            String qcv = table.columnAttributes(col).getString("sdn_qc");
            if(qcv != null) qcValue[col] = qcv;
            

        }


        ODVAppender tab = (sb,i,b) -> sb.append("\t");
        

        //START OF METADATA SECTION
        //final int usedCol[] = {-1};
        appenders.add(getODVMetadataAppender(table, pas, usedCols, null, headers, "SDN_CRUISE", "SDN_CRUISE_variable", "Missing SDN_CRUISE" ,"cruise"));
        appenders.add(tab);
        appenders.add(getODVMetadataAppender(table, pas, usedCols, null, headers, "SDN_STATION", "SDN_STATION_variable", "Missing SDN_STATION" , "station_id", "station"));
        appenders.add(tab);
        // According to Rob Thomas, types other than * are only for backward compatibility.
        appenders.add((sb,i,b) -> sb.append("*\t")); // Type

        appenders.add((sb,i,b) -> {
           sb.append(Calendar2.epochSecondsToLimitedIsoStringT(output_format[timeCol], pas[timeCol].getDouble(i), ""));
        });
        appenders.add(tab);
        appenders.add((sb,i,b) -> {
            sb.append(pas[lonCol].getTsvString(i));
        });
        appenders.add(tab);
        appenders.add((sb,i,b) -> {
               sb.append( pas[latCol].getTsvString(i));
         });

        appenders.add(tab);
        appenders.add(localCdiIdAppender);
        appenders.add(tab);
		appenders.add(edmoCodeAppender);
        appenders.add(tab);
        appenders.add(getODVMetadataAppender(table, pas, usedCols, null, headers, "SDN_BOT_DEPTH", "SDN_BOT_DEPTH_variable", "Missing SDN_BOT_DEPTH"));


        usedCols.add(latCol);
        usedCols.add(lonCol);
        usedCols.add(timeCol);

        // don't complain about missing configs for qc variables.
        for(int i = missingConfigs.size()-1;i>=0;i--) {
        	final int col = missingConfigs.get(i);
        	if(usedCols.contains(col)|| isQualityCheck[col]) {
        		missingConfigs.remove(i);
        	}else {
        		usedCols.add(col);
        	}
        }
        // warn if any variables without odv configs were requested.
        if(missingConfigs.size()>0) {
        	final boolean plural = missingConfigs.size()>1;
        	final String havehas = plural ? " have" : " has";
        	final String theyit = plural ? " they" : " it";
        	final String andThis = plural ? (" and " +colNames[missingConfigs.remove(missingConfigs.size()-1)]): "";
        	StringBuilder sb = new StringBuilder();
        	sb.append("//\r\n// Dataset variable");
        	sb.append(plural ? "s ":" ");
        	sb.append(colNames[missingConfigs.remove(0)]);
        	while(missingConfigs.size()>0) {
        		sb.append(", ");
        		sb.append(colNames[missingConfigs.remove(0)]);
        	}
        	sb.append(andThis);
        	sb.append(havehas);
        	sb.append(" not been included in the output because\r\n//");
        	sb.append(theyit);
        	sb.append(havehas);
        	sb.append(" no SeaDataNet P01/P06 attributes in the ERDDAP configuration;");
        	sb.append(theyit);
        	sb.append(" can be downloaded in another format option from:\r\n// ");
        	sb.append(EDStatic.erddapUrl + "/tabledap/" + tDatasetID + ".html");
        	sb.append("\r\n//\r\n");
        	headers.append(sb);
        }

        
        List<Integer> dataCols = new ArrayList<Integer>();
        String primaryVar =  table.globalAttributes().getString("SDN_primary_variable");
        if(primaryVar == null){
        	// Is it a TimeSeries? If yes, then time is the primary variable.
        	if(table.globalAttributes().getString("featureType") == "TimeSeries" || table.globalAttributes().getString("cdm_data_type") == "TimeSeries") {
        		primaryVar = table.getColumnName(timeCol);
        	}else {
        		headers.append("// warning: global attribute SDN_primary_variable is not defined\r\n");
        	}
        }

        int primaryVarCol = findOrGuessColumn(table, primaryVar, EDV.ALT_NAME, "depth", "altitude", "position", "pressure", "sigma");
        if(primaryVarCol < 0)

           primaryVarCol = findOrGuessColumn(table, "ship", "station");


        if(primaryVarCol < 0){
            if(primaryVar != null)
                headers.append("// warning:  SDN_primary_variable "+primaryVar+" not in query\r\n");
        }else{
           dataCols.add(primaryVarCol);
           usedCols.add(primaryVarCol);
           keyColumns.add(primaryVarCol);
           table.sort(keyColumns.stream().mapToInt(i -> i).toArray());
        }
        final boolean timeIsPrimaryVar = primaryVarCol == timeCol;
        final StringBuilder sbColumnHeader = new StringBuilder();
        sbColumnHeader.append("Cruise\tStation\tType\t");
        sbColumnHeader.append(output_format[timeCol]);
        sbColumnHeader.append("\tLongitude [degrees_east]\tLatitude [degrees_north]\tLOCAL_CDI_ID\tEDMO_code\tBot. Depth [m]");
        // ANY Remaining METAVAR (Text) values to appear to the left of the primary var

        for(int i=0; i<nCols; i++){
           if(usedCols.contains(i) || isQualityCheck[i]){
               continue;
           }
           final int col = i;
           String type = getODVType(pas[col],colNames[col]);
           if(false && type.startsWith("TEXT:")){
        	   // Rob Thomas wanted these to appear before the primary_var column, but this
        	   // was not supported by the validation tools at the time (2019).
             usedCols.add(col);
        	 sbColumnHeader.append("\t");
        	 sbColumnHeader.append(localNames[col]).append(":METAVAR:").append(type);
        	 appenders.add(tab);
             appenders.add( (sb,row,b) -> {
                sb.append(pas[col].getTsvString(row));
             });
           }else {
        	   dataCols.add(col);
           }
        }
        
        
        // write out metadata only if something like station changes...
        // this appender will replace header with blank tabs
        final ODVAppender[] mda = (ODVAppender[]) appenders.toArray(new ODVAppender[appenders.size()]);
        final String prevMetadataSignature[] = {null};
        appenders.add((sb,row,b) -> {
        	final String proposedMetadata = sb.toString();
            String signature = getOdvMetadataSignature(proposedMetadata, timeIsPrimaryVar);
            if(prevMetadataSignature[0] != null){
            	boolean hideMetadata = true;
                if(!prevMetadataSignature[0].equals(signature)){
                	// signature may have changed by having hidden some repeat values at line 2.
                	// rebuild signature without using cache...
                    StringBuilder sb2 = new StringBuilder();
                    for(int i=0; i< mda.length;i++){
                        mda[i].append(sb2,row,false);
                    }
                    String signature2 = getOdvMetadataSignature(sb2.toString(), timeIsPrimaryVar);
                    if(!prevMetadataSignature[0].equals(signature2)) {
                    	hideMetadata = false;
                    }
                }
                if(hideMetadata) {
                    sb.setLength(0);
                    // Just output the tabs...
                    sb.append(proposedMetadata.replaceAll("[^\t]",""));
                }
            }
            prevMetadataSignature[0] = signature;
        }
        );


        //END OF METADATA SECTION


        headers.append("//SDN_parameter_mapping\r\n");

        for(int i=0; i<dataCols.size(); i++){
            final int col = dataCols.get(i);
            headers.append(colHeaderMap.get(col));

            //String coName = table.getColumnName(col);
            String type = getODVType(pas[col],colNames[col]);

            sbColumnHeader.append("\t");
            if(labels[col] != null){
                 sbColumnHeader.append(labels[col]);
            }else{
            	if(isTimeStamp[col]) {
            		sbColumnHeader.append(localNames[col]).append(" [").append(output_format[col]).append("]");
            	}else{
                    String primary = i == 0 ? ":PRIMARYVAR" : "";
                    sbColumnHeader.append(localNames[col]).append(units[col]).append(primary).append(":").append(type);
            	}
            }
            sbColumnHeader.append("\tQV:SEADATANET");
            appenders.add( (sb,row,b) -> {
               sb.append("\t");
               String value;
               if(isTimeStamp[col]){
                   value = Calendar2.epochSecondsToLimitedIsoStringT(output_format[col], pas[col].getDouble(row), "");
               }else{
                   value = pas[col].getTsvString(row);
                   if(output_format[col] != null && String2.isNumber(value)){
                       value = String.format(output_format[col],String2.parseDouble(value));
                   }
               }
               sb.append(value).append("\t");
               if(value.length() == 0){
                 sb.append("9");
               }else{
                 String qc = qcValue[col];
                 if(qcCol[col] >= 0){
                    qc = pas[qcCol[col]].getTsvString(row);
                 }
                 if(qc.length()==0){
                   qc = "0";
                 }
                 sb.append(qc);
               }
            });
        }

        headers.append("//\r\n");

        //open an OutputStream
        OutputStreamWriter writer = new OutputStreamWriter(outputStreamSource.outputStream(
            String2.ISO_8859_1)); //ODV User's Guide 16.3 says ASCII (7bit), so might as well go for compatible common 8bit
        // write the headers
        writer.write(headers.toString());
        sbColumnHeader.append("\r\n");
        writer.write(sbColumnHeader.toString());
        // process the lines of data delegating to the appenders already assigned,
        // writing to the output after each row of data.
        StringBuilder sb = new StringBuilder();
        ODVAppender[] appender = (ODVAppender[]) appenders.toArray(new ODVAppender[appenders.size()]);
        for (int row = 0; row < nRows; row++) {
          sb.setLength(0);
          for(int i=0; i< appender.length;i++){
            appender[i].append(sb,row,true);
          }
          writer.write(sb.toString());
          writer.write("\r\n");
        }

        writer.flush(); //essential
        if (reallyVerbose) String2.log("  EDDTable.saveAsSeaDataNetODV done. TIME=" +
            (System.currentTimeMillis() - time) + "\n");
    }
}
