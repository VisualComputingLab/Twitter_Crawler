package gr.iti.vcl.crawl.impl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import java.io.IOException;
import java.util.HashMap;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import twitter4j.FilterQuery;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

/**
 *
 * @author Michalis Lazaridis <lazar@iti.gr>
 */
public class CrawlerImpl {
    
    public static String TWITTER_STREAM_COMMAND_CREATE = "create";
    public static String TWITTER_STREAM_COMMAND_REMOVE = "remove";
    
    private HashMap<String, TwitterStream> activeTwitterStreams;
    private HashMap<String, Connection> activeConnections;
    
    public CrawlerImpl()
    {        
        activeTwitterStreams = new HashMap<String, TwitterStream>();
        activeConnections = new HashMap<String, Connection>();
    }
    
    private Configuration createConfiguration(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret)
    {
        ConfigurationBuilder confBuilder = new ConfigurationBuilder();		
			
        confBuilder.setDebugEnabled(true);
        confBuilder.setOAuthConsumerKey(consumerKey);
        confBuilder.setOAuthConsumerSecret(consumerSecret);
        confBuilder.setOAuthAccessToken(accessToken);
        confBuilder.setOAuthAccessTokenSecret(accessTokenSecret);
        confBuilder.setJSONStoreEnabled(true);
        
        
        return confBuilder.build();
    }
    
    public JSONObject postService(JSONObject jsonObject)
    {
        JSONObject resultObject = new JSONObject();        
        String command;
        JSONArray jarr;
        String allkwordinastring = "";
        
        try
        {
            command = jsonObject.getString("command");
            int int_id = 0;
            String receipt = null, tmps;
            
            if (command.equals(TWITTER_STREAM_COMMAND_CREATE))
            {
                // create twitter configuration
                
                String consumerKey = jsonObject.getJSONObject("twitter").getString("consumerKey");
                String consumerSecret = jsonObject.getJSONObject("twitter").getString("consumerSecret");
                String accessToken = jsonObject.getJSONObject("twitter").getString("accessToken");
                String accessTokenSecret = jsonObject.getJSONObject("twitter").getString("accessTokenSecret");
                
                Configuration config = createConfiguration(consumerKey, consumerSecret, accessToken, accessTokenSecret);
                               
                
                // read filter parameters
                String [] kwords = null;
                long [] users = null;
                double [][] locations = null;
                
                jarr = jsonObject.optJSONArray("keywords");
                final boolean doNotPutKeywordsInBody = jsonObject.optBoolean("doNotPutKeywordsInBody", false);
                
                if (jarr == null || jarr.length() == 0)
                {
                    err("No keywords given, aborting");
                    resultObject.put("Status", "Error");
                    resultObject.put("Message", "No keywords given");
                    return resultObject;
                }
                
                kwords = new String[jarr.length()];
                for (int n = 0; n < jarr.length(); n++)
                {
                    tmps = jarr.getString(n);
                    int_id += tmps.hashCode();
                    kwords[n] = tmps;
                    allkwordinastring += tmps + ":";
                }
                receipt = Integer.toHexString(int_id);
                
                if (jsonObject.has("users"))
                {
                    jarr = jsonObject.getJSONArray("users");
                    users = new long[jarr.length()];
                    for (int n = 0; n < jarr.length(); n++)
                    {
                        users[n] = jarr.getLong(n);
                    }
                }
                else if (jsonObject.has("locations"))
                {
                    jarr = jsonObject.getJSONArray("locations");
                    if (jarr.length() > 0 && jarr.length() % 4 == 0)
                    {
                        locations = new double[jarr.length()][2];
                        for (int n = 0; n < jarr.length(); n++)
                        {
                            locations[n/2][n%2] = jarr.getLong(n);
                        }
                    }
                    else
                    {
                        err("Wrong locations parameter");
                        resultObject.put("Status", "Error");
                        resultObject.put("Message", "Wrong locations parameter");
                        return resultObject;
                    }
                }
                                
                if (activeTwitterStreams.containsKey(receipt) || activeConnections.containsKey(receipt))
                {
                    err("Key already there: " + receipt);
                    resultObject.put("Status", "Error");
                    resultObject.put("Message", "Key already there: " + receipt);
                    return resultObject;
                }
                
                // create queue
                
                String rabbitHost = jsonObject.getJSONObject("rabbit").getString("host");
                String rabbitQueue = jsonObject.getJSONObject("rabbit").getString("queue");
                
                ConnectionFactory factory = new ConnectionFactory();
                factory.setHost(rabbitHost);
                Connection connection;
                Channel channel;
                
                try 
                {
                    connection = factory.newConnection();
                    channel = connection.createChannel();
                    channel.queueDeclare(rabbitQueue, false, false, false, null);
                    // for fair dispatch
                    channel.basicQos(1);
                    activeConnections.put(receipt, connection);
                } 
                catch (IOException ex) 
                {
                    err("IOException during queue creation: " + ex);
                    resultObject.put("Status", "Error");
                    resultObject.put("Message", "IOException during queue creation: " + ex);
                    return resultObject;
                }
                
                
                // create twitter stream and listener

                final String [] kwordarray = kwords;
                final Channel fchannel = channel;
                final String fRabbitQueue = rabbitQueue;

                StatusListener listener = new StatusListener() 
                {   
                    //When a tweet, that contains at least one of the queries, is uploaded:
                    @Override
                    public void onStatus(Status status) 
                    {
                        String statusString = DataObjectFactory.getRawJSON(status);

                        try 
                        {
                            if (!doNotPutKeywordsInBody)
                            {
                                JSONArray jsonarr = new JSONArray();
                                for (int k = 0; k < kwordarray.length; k++)
                                {
                                    if (status.getText().toLowerCase().indexOf(kwordarray[k]) >= 0)
                                        jsonarr.put(kwordarray[k]);
                                }

                                JSONObject json = new JSONObject(statusString);
                                json.put("keywords", jsonarr);
                                statusString = json.toString();
                            }

                            fchannel.basicPublish( "", fRabbitQueue, MessageProperties.PERSISTENT_TEXT_PLAIN, statusString.getBytes());
                        } 
                        catch (IOException ex) 
                        {
                            err("IOException during sending twitter message to rabbitmq: " + ex);
                        }
                        catch (JSONException ex) 
                        {
                            err("JSONException during sending twitter message to rabbitmq: " + ex);
                        } 
                    }

                    @Override
                    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
                    @Override
                    public void onTrackLimitationNotice(int numberOfLimitedStatuses)
                    {
                        err("onTrackLimitationNotice in StatusListener: numberOfLimitedStatuses = " + numberOfLimitedStatuses);
                    }
                    @Override
                    public void onScrubGeo(long userId, long upToStatusId) {}
                    @Override
                    public void onException(Exception ex)
                    {
                        err("Exception in StatusListener: " + ex);
                    }
                };

                TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
                twitterStream.addListener(listener);
                FilterQuery fq = new FilterQuery();
                fq.setIncludeEntities(true);
                
                if (users != null && users.length > 0)
                {
                    fq.follow(users);
                    log("I will listen for " + users.length + " users");
                }
                else if (locations != null && locations.length > 1)
                {
                    fq.locations(locations);
                    log("I will listen for " + (locations.length / 2) + " locations");
                }
                else
                {
                    fq.track(kwordarray);
                    log("I will listen for the following keywords: " + allkwordinastring);
                }
                
                twitterStream.filter(fq);
                

                activeTwitterStreams.put(receipt, twitterStream);
                
                resultObject.put("Status", "OK");
                resultObject.put("Receipt", receipt);
            }
            else if (command.equals(TWITTER_STREAM_COMMAND_REMOVE))
            {
                receipt = jsonObject.getString("receipt");
                
                if (receipt.equalsIgnoreCase("last"))
                {
                    receipt = activeTwitterStreams.keySet().iterator().next();
                }
                
                TwitterStream twitterStream = activeTwitterStreams.remove(receipt);
                Connection conn = activeConnections.remove(receipt);
                
                if (twitterStream != null)
                {
                    log("Cleaning up twitter stream");
                    twitterStream.cleanUp();
                    twitterStream = null;
                }
                
                if (conn != null)
                {
                    log("Closing rabbitmq connection and channels");
                    try 
                    {
                        conn.close();
                        conn = null;
                    }
                    catch (IOException ex) 
                    {
                        err("IOException during closing rabbitmq connection and channels: " + ex);
                        resultObject.put("Status", "Error");
                        resultObject.put("Message", "IOException during closing rabbitmq connection and channels: " + ex);
                        return resultObject;
                    }
                }
                
                resultObject.put("Status", "OK");
            }
            else
            {
                err("Command not recognized, aborting");
                resultObject.put("Status", "Error");
                resultObject.put("Message", "Command not recognized: " + command);
            }
        }
        catch (JSONException e)
        {
            err("JSONException during starting/stopping crawlers: " + e);
            return null;
        }
        
        return resultObject;
    }
    
    private void log(String message)
    {
        System.out.println("Crawler:INFO:" + message);
    }
    
    private void err(String message)
    {
        System.err.println("Crawler:ERROR:" + message);
    }
}
