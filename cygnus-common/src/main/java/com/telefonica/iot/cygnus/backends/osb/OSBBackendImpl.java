/**
 * Copyright 2015-2017 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FIWARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */

package com.telefonica.iot.cygnus.backends.osb;

import com.telefonica.iot.cygnus.log.CygnusLogger;

/**
 *
 * @author Vicente Lozano
 * 
 *         OSB related operations.
 *   
 */
@SuppressWarnings("restriction")
public class OSBBackendImpl implements OSBBackend {

    private static final CygnusLogger LOGGER = new CygnusLogger(OSBBackendImpl.class);

    private String urlOsb,apiKey,jsonPost;

   
	/**
     * Constructor.
     * 
     * @param mysqlHost
     * @param mysqlPort
     * @param mysqlUsername
     * @param mysqlPassword
     */
    public OSBBackendImpl(String urlOsb, String apiKey) {
    	this.urlOsb = urlOsb;
		this.apiKey = apiKey;
    } // MySQLBackendImpl
    
    

	@Override
	public void setInformation(String jsonPost) {
		// TODO Auto-generated method stub
		this.jsonPost = jsonPost;
		LOGGER.info("SetInformation- jsonPost (" + jsonPost + ")");
		
	}

	@Override
	public void buildPost() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void sendRequestPostToOSB() {
		// TODO Auto-generated method stub
		
	}
	
	 public String getUrlOsb() {
		return urlOsb;
	}



	public void setUrlOsb(String urlOsb) {
		this.urlOsb = urlOsb;
	}



	public String getApiKey() {
		return apiKey;
	}



	public void setApiKey(String apiKey) {
		this.apiKey = apiKey;
	}



	public String getJsonPost() {
		return jsonPost;
	}



	public void setJsonPost(String jsonPost) {
		this.jsonPost = jsonPost;
	}	
	

} // OSBBackendImpl
