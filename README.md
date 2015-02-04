
A java web crawler wrapping the Twitter Streaming API, written in Netbeans 7.3.1
  
Prerequisites:

- A tomcat server (>= 7) for deploying the service (http://tomcat.apache.org/download-70.cgi).
- A RabbitMQ server (>= 2.7.1) for storing the results (http://www.rabbitmq.com/).
- Twitter authorization tokens (https://dev.twitter.com/oauth/overview/application-owner-access-tokens).

The project can be directly opened in Netbeans, built and deployed on a Tomcat server.

Then, the service can be started as follows:

POST http://TOMCAT_SERVER:8080/Twitter_Crawler/resources/crawl

Content-Type: application/json

  {

    "command": "create",

    "twitter":
    {
      "consumerKey": CONSUMER_KEY,
      "consumerSecret": CONSUMER_SECRET, 
      "accessToken": ACCESS_TOKEN, 
      "accessTokenSecret": ACCESS_TOKEN_SECRET
    },
  "rabbit": 
  {
    "host": RABBIT_HOST_IP,
    "queue": RABBIT_QUEUE_NAME_FOR_STORING_RESULTS
  },
  "keywords": ["thessaloniki", "salonica", "saloniki", "θεσσαλονίκη", "θεσσαλονικη", "selanik"]
}

The service can be stopped as follows:

POST http://TOMCAT_SERVER:8080/Twitter_Crawler/resources/crawl

Content-Type: application/json

{

"command": "remove",

"receipt": "last"

}
