import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.aw20.net.HttpDelete;
import org.aw20.net.HttpGet;
import org.aw20.net.HttpPost;
import org.aw20.net.HttpResult;
import org.aw20.util.DateUtil;

import com.amazonaws.util.json.Jackson;


/**
 * Utility class for operating with SalesForce's API
 * 
 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm
 * 
 * It handles the common functions, including fetching, updating, creating and deleting objects.  It 
 * utilizes introspection to determine the correct URL's to use.   The only URL that is hardcoded is
 * the "login.salesforce.com" domain; all other domains are communicated via the callbacks
 * 
 */
public class SFRestApi {

	private	String username;
	private String password;
	private String clientId;
	private String clientSecret;
	
	private Map<String,String>	sessionMap;
	private Map<String, Map<String, Object>>	objectMetaMap;
	
	private String sfApiUsage = null;
	
	public SFRestApi( String username, String password, String clientId, String clientSecret ) throws SFRestApiException {
		this.username = username;
		this.password = password;
		this.clientId	= clientId;
		this.clientSecret	= clientSecret;
		
		doSetup();
	}

	
	private void doSetup() throws SFRestApiException {
		doSessionExecution();
		doDescribeObjects();
	}
	
	
	/**
	 * Logs the user into salesforce, getting the necessary usage tokens
	 */
	@SuppressWarnings("unchecked")
	private void doSessionExecution() throws SFRestApiException {
		
		HttpPost	post	= new HttpPost("https://login.salesforce.com/services/oauth2/token");
		
		Map<String,String> params	= new HashMap<>();
		params.put("grant_type", "password");
		params.put("username", username );
		params.put("password", password);
		params.put("client_id", clientId);
		params.put("client_secret", clientSecret);
		
		try {
			post.setPostParams( params );
			
			HttpResult result = post.execute();

			if ( result.getResponseCode() == 200 ){
				sessionMap = Jackson.fromJsonString( result.getBodyAsString(), Map.class );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
	}
	

	@SuppressWarnings("unchecked")
	private void doDescribeObjects() throws SFRestApiException {
		objectMetaMap	= new HashMap<>();
		
		HttpGet get = new HttpGet( sessionMap.get("instance_url") + "/services/data/v37.0/sobjects/" );
		get.addRequestHeader("Authorization", getAuthorization() );
		
		try{
			HttpResult result = get.execute();
		
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 200 ){
				Map<String,Object> rawMap = Jackson.fromJsonString( result.getBodyAsString(), Map.class );
				
				for ( Map<String,Object> sbobject : (List<Map<String,Object>>)rawMap.get("sobjects") ){
					objectMetaMap.put( (String)sbobject.get("name"), sbobject);
				}

			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
		
	}


	private String getAuthorization() {
		return sessionMap.get("token_type") + " " + sessionMap.get("access_token");
	}

	
	@SuppressWarnings("unchecked")
	private String getObjectUrl(Map<String, Object> objMetadata, String key) {
		return ((Map<String, String>)objMetadata.get("urls")).get(key);
	}

	public String getApiUsage(){
		return sfApiUsage;
	}
	
	/**
	 * Returns the object instance, give the type and the id
	 * @param object
	 * @param id
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> get(String object, String id) throws SFRestApiException {
		
		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}
		
		// Get the object from SalesForce
		HttpGet get = new HttpGet( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") + "/" + id );
		get.addRequestHeader("Authorization", getAuthorization() );
		
		try{
			HttpResult result = get.execute();
			
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");

			if ( result.getResponseCode() == 200 ){
				return Jackson.fromJsonString( result.getBodyAsString(), Map.class );
			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				return get( object, id );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
	}



	/**
	 * Called to update a particular object
	 * 
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_update_fields.htm
	 * 
	 * @param object
	 * @param id
	 * @param fieldData
	 * @throws SFRestApiException
	 */
	public void update(String object, String id, Map<String, Object> fieldData ) throws SFRestApiException {
	
		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}

		HttpPost	post	= new HttpPost( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") + "/" + id + "?_HttpMethod=PATCH" );
		post.addRequestHeader("Authorization", getAuthorization() );
		post.addRequestHeader("Content-Type", "application/json");
		post.setBodyAsByte( Jackson.toJsonString(fieldData).getBytes() );
		
		try{
			HttpResult result = post.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 204 ){
				return ;
			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				update( object, id, fieldData );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
	}
	
	
	
	/**
	 * Execute the gven SQQL against the database, retrieving back all the rows, including
	 * all the paging that is required
	 * 
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_query.htm
	 * 
	 * @param query
	 * @return
	 * @throws SFRestApiException
	 */
	public List<Map<String,Object>>	query( String query ) throws SFRestApiException {
		List<Map<String,Object>> results	= new ArrayList<>();
		
		try {
			query( "/services/data/v20.0/query/?q=" + URLEncoder.encode(query, "UTF8"), results );
		} catch (UnsupportedEncodingException e) {
			throw new SFRestApiException( e.getMessage() );
		}
		
		return results;
	}
	

	
	@SuppressWarnings("unchecked")
	private void	query( String uri, List<Map<String,Object>> results ) throws SFRestApiException {
		
		try{
			// Get the object from SalesForce
			HttpGet get = new HttpGet( sessionMap.get("instance_url") + uri );
			get.addRequestHeader("Authorization", getAuthorization() );

			HttpResult result = get.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 200 ){
				Map<String,Object> rawMap = Jackson.fromJsonString( result.getBodyAsString(), Map.class );				
				
				if ( rawMap.containsKey("records") ){
					for ( Map<String,Object> sbobject : (List<Map<String,Object>>)rawMap.get("records") ){
						results.add(sbobject);
					}
				}
				
				if ( rawMap.containsKey("nextRecordsUrl") ){
					query( (String)rawMap.get("nextRecordsUrl"), results );
				}

			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				query( uri, results );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}			
		
	}
	
	
	
	/**
	 * For the given object return all the ID's that have updated within a given time frame
	 * 
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_get_updated.htm
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_getdeleted.htm
	 * 
	 * @param object
	 * @param from
	 * @param to
	 * @return
	 * @throws SFRestApiException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<String>	getUpdated( String object, long from, long to ) throws SFRestApiException{
		
		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}
		
		try{
			
			StringBuilder	sb	= new StringBuilder();
			sb.append( "start=" );
			sb.append( URLEncoder.encode( DateUtil.getDateString(from, "yyyy-MM-dd'T'HH:mm:ss") + "+00:00" , "UTF8"));
			
			sb.append( "&end=" );
			sb.append( URLEncoder.encode( DateUtil.getDateString(to, "yyyy-MM-dd'T'HH:mm:ss") + "+00:00" , "UTF8"));

			// Get the object from SalesForce
			HttpGet get = new HttpGet( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") + "/updated?" + sb.toString() );
			get.addRequestHeader("Authorization", getAuthorization() );

			
			HttpResult result = get.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 200 ){
				Map rawMap = Jackson.fromJsonString(result.getBodyAsString(), Map.class );
				return (List<String>)rawMap.get("ids");
			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				return getUpdated( object, from, to );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}

		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}			
	}
	
	
	
	
	/**
	 * For the given object return all the ID's that have been deleted within a given time frame
	 * 
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_get_deleted.htm
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_getdeleted.htm
	 * 
	 * @param object
	 * @param from
	 * @param to
	 * @return
	 * @throws SFRestApiException
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public List<Map<String,String>>	getDeleted( String object, long from, long to ) throws SFRestApiException{
		
		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}
		
		try{
			
			StringBuilder	sb	= new StringBuilder();
			sb.append( "start=" );
			sb.append( URLEncoder.encode( DateUtil.getDateString(from, "yyyy-MM-dd'T'HH:mm:ss") + "+00:00" , "UTF8"));
			
			sb.append( "&end=" );
			sb.append( URLEncoder.encode( DateUtil.getDateString(to, "yyyy-MM-dd'T'HH:mm:ss") + "+00:00" , "UTF8"));

			// Get the object from SalesForce
			HttpGet get = new HttpGet( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") + "/deleted?" + sb.toString() );
			get.addRequestHeader("Authorization", getAuthorization() );

			
			HttpResult result = get.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 200 ){
				Map rawMap = Jackson.fromJsonString(result.getBodyAsString(), Map.class );
				return (List<Map<String,String>>)rawMap.get("deletedRecords");
			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				return getDeleted( object, from, to );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}

		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}			
	}
	
	
	
	/**
	 * Creates a new object returning back the new Id of that object.
	 * 
	 * @param object
	 * @param fieldData
	 * @throws SFRestApiException
	 */
	@SuppressWarnings({ "rawtypes" })
	public String create(String object, Map<String, Object> fieldData ) throws SFRestApiException {
		
		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}

		HttpPost	post	= new HttpPost( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") );
		post.addRequestHeader("Authorization", getAuthorization() );
		post.addRequestHeader("Content-Type", "application/json");
		post.setBodyAsByte( Jackson.toJsonString(fieldData).getBytes() );
		
		try{
			HttpResult result = post.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");
			
			if ( result.getResponseCode() == 201 ){
				Map rawMap = Jackson.fromJsonString(result.getBodyAsString(), Map.class );
				
				if ( (boolean)rawMap.get("success") ){
					return (String)rawMap.get("id");
				}else{
					throw new SFRestApiException( result.getBodyAsString() );
				}
				
			}else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				return create( object, fieldData );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
	}
	
	
	
	/**
	 * Deletes the given object id
	 * 
	 * https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/dome_delete_record.htm
	 * 
	 * @param object
	 * @param id
	 * @throws SFRestApiException
	 */
	public void delete(String object, String id ) throws SFRestApiException {

		// Pull out the metadata
		Map<String, Object>	objMetadata	= objectMetaMap.get(object);
		if ( objMetadata == null ){
			throw new SFRestApiException("object type unknown: " + object );
		}

		HttpDelete delete = new HttpDelete( sessionMap.get("instance_url") + getObjectUrl(objMetadata,"sobject") + "/" + id );
		delete.addRequestHeader("Authorization", getAuthorization() );
		
		try{
			HttpResult result = delete.execute();
			sfApiUsage	= result.getHeaderFirstValue("Sforce-Limit-Info");

			if ( result.getResponseCode() == 204 ){
				return;
			} else if ( result.getResponseCode() == 401 ){
				doSessionExecution();
				delete( object, id );
			}else{
				throw new SFRestApiException( result.getResponseCode(), result.getBodyAsString() );
			}
			
		} catch (IOException e) {
			throw new SFRestApiException( e.getMessage() );
		}
		
	}
	
	
}
