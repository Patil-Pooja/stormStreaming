package extwitter;

import java.util.Arrays;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;



public class TwitterHashtagStorm {
   public static void main(String[] args) throws Exception{
      String consumerKey = "qUH1vLpmI7xtuBOsegpbeEXsy";
      String consumerSecret = "ycDRx13z5L8iTJoK4yHhV4pBy41Pw2008P0uSGJ25waGkN43cB";
		
      String accessToken = "2650657855-pozv6mCF5B3wuxGhwL5Sad2yoCaaWbHrLrCq2Uq";
      String accessTokenSecret = "KTnFwk7ztCyMD5ZbgVth02orClJ5ynEVmD1BmGLHGMyiv";
		
     // String[] arguments = args.clone();
      String[] keyWords = {"rivers","Football"};//Arrays.copyOfRange(arguments, 4, arguments.length);
		
      Config config = new Config();
      config.setDebug(true);
		
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
         consumerSecret, accessToken, accessTokenSecret, keyWords));

      builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt())
         .shuffleGrouping("twitter-spout");

      builder.setBolt("twitter-hashtag-counter-bolt", new HashtagCounterBolt())
         .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag"));
			
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("TwitterHashtagStorm", config,
         builder.createTopology());
      Thread.sleep(10000);
      cluster.shutdown();
   }
}
