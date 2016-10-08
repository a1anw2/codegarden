public class SFRestApiException extends Exception {
	private static final long serialVersionUID = -2669936140168822764L;

	private int responseCode = -1;
	
	public SFRestApiException(int responseCode, String bodyAsString) {
		super( responseCode + " " + bodyAsString );
		
		this.responseCode	= responseCode;
	}


	public SFRestApiException(String message) {
		super( message );
	}

	public int getResponseCode(){
		return responseCode;
	}
	
}
