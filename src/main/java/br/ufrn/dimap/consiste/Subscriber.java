package br.ufrn.dimap.consiste;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Subscriber implements MqttCallback{
	
	public static void main(String[] args) {
		
		String query = "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> " + 
				"PREFIX qodisco: <http://consiste.dimap.ufrn.br/ontologies/qodisco#> " +
				"PREFIX DUL: <http://www.loa-cnr.it/ontologies/DUL.owl#> " +
				"SELECT ?sensor ?observedProperty WHERE { "+
				 "?sensor a ssn:SensingDevice; DUL:hasLocation qodisco:Building01 ; ssn:observes ?observedProperty . }";
		
		String qodiscoUrl = System.getProperty("url")!=null ? System.getProperty("url") : "localhost:8080/qodisco/api";
		String username = System.getProperty("username")!=null ? System.getProperty("username") : "admin" ;
		String password = System.getProperty("password")!=null ? System.getProperty("username") : "password";
		String domain = System.getProperty("domain") != null ? System.getProperty("domain") : "Default";
		
		JSONObject response = new Subscriber().asyncSearch(qodiscoUrl, query, domain, username, password);
		if (response != null){
			new Subscriber().subscribe(response.get("brokerAddress").toString(), response.get("topic").toString());
		}		
	}
	
	public JSONObject asyncSearch(String qodiscoUrl, String query, String domainName, String username, String password) {
		DefaultHttpClient httpClient = new DefaultHttpClient();
		URIBuilder uri = new URIBuilder();
		uri.setScheme("http").setHost(qodiscoUrl).setPath("/async-search")
			.setParameter("domain", domainName);
		
		String response = new String();
		
		try {
			HttpGet getMethod;
			getMethod = new HttpGet(uri.build());
			getMethod.addHeader("query", query);
			
			Credentials credentials = new UsernamePasswordCredentials(username, password);
			httpClient.getCredentialsProvider().setCredentials(AuthScope.ANY, credentials);
			
			HttpResponse httpResponse = httpClient.execute(getMethod);
			
			HttpEntity responseEntity = httpResponse.getEntity();
			if (responseEntity != null) {
				response = EntityUtils.toString(responseEntity);
			}
		} catch (URISyntaxException | IOException e) {
			e.printStackTrace();
		}
		
		JSONObject json = null;
		try {
			json = (JSONObject) new JSONParser().parse(response);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return json;
	}
	
	public void subscribe(String broker, String topic) {
		String clientId = "paho-java-subscriber";
		
		System.out.println("Broker: " + broker);
		System.out.println("Topic: " + topic);
		
		try {
			final MqttClient sampleClient = new MqttClient(broker, clientId, new MemoryPersistence());
			System.out.println("paho-client connecting to broker: " + broker);
			sampleClient.connect();
			sampleClient.setCallback(this);
			sampleClient.subscribe(topic);
			
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					System.out.println("stopping subscriber...");
					try {
						sampleClient.disconnect();
					} catch (MqttException e) {
						e.printStackTrace();
					}
					System.out.println("moquette subscriber stopped");
				}
			});
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void connectionLost(Throwable arg0) {
		
	}

	public void deliveryComplete(IMqttDeliveryToken arg0) {
		
	}

	public void messageArrived(String topic, MqttMessage message) throws Exception {
		System.out.println("Message arrived: " + message);
	}

}
