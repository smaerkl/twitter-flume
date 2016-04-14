/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package uk.co.cathtanconsulting.twitter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.GeoLocation;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

/**
 * Demo Flume source that connects via Streaming API to the 1% sample twitter
 * firehose, continously downloads tweets, converts them to Avro format and
 * sends Avro events to a downstream Flume sink.
 * 
 * Requires the consumer and access tokens and secrets of a Twitter developer
 * account
 */

/**
 * @author tristan
 *
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TwitterSource extends AbstractSource implements EventDrivenSource,
		Configurable, StatusListener {

	private TwitterStream twitterStream;

	private ByteArrayOutputStream outputStream;
	private Encoder encoder;
	private DatumWriter<TweetRecord> avroDatumWriter;
	private long docCount = 0;
	private long startTime = 0;
	private long exceptionCount = 0;
	private long totalBytesEmitted = 0;
	private long skippedDocs = 0;
	
	private boolean useBinaryEncoder;
	
	// Fri May 14 02:52:55 +0000 2010
	private static final SimpleDateFormat formatterTo = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ss'Z'");
	private DecimalFormat numFormatter = new DecimalFormat("###,###.###");

	private int reportInterval;
	private int statsInterval;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(TwitterSource.class);

	public TwitterSource() {
	}

	@Override
	public void configure(Context context) {
		String consumerKey = context.getString("consumerKey");
		String consumerSecret = context.getString("consumerSecret");
		String accessToken = context.getString("accessToken");
		String accessTokenSecret = context.getString("accessTokenSecret");

		twitterStream = new TwitterStreamFactory().getInstance();
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(new AccessToken(accessToken,
				accessTokenSecret));
		twitterStream.addListener(this);

		avroDatumWriter = new SpecificDatumWriter<TweetRecord>(
				TweetRecord.class);

		reportInterval = context.getInteger("reportInterval", 100);
		statsInterval = context.getInteger("statsInterval",reportInterval*10);
	    
	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting twitter source 1.01 {} ...", this);
		docCount = 0;
		startTime = System.currentTimeMillis();
		exceptionCount = 0;
		totalBytesEmitted = 0;
		skippedDocs = 0;
		outputStream = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
		twitterStream.sample();
		LOGGER.info("Twitter source {} started.", getName());
		// This should happen at the end of the start method, since this will
		// change the lifecycle status of the component to tell the Flume
		// framework that this component has started. Doing this any earlier
		// tells the framework that the component started successfully, even
		// if the method actually fails later.
		super.start();
	}

	@Override
	public synchronized void stop() {
		LOGGER.info("Twitter source {} stopping...", getName());
		twitterStream.shutdown();
		super.stop();
		LOGGER.info("Twitter source {} stopped.", getName());
	}

	/* (non-Javadoc)
	 * @see twitter4j.StatusListener#onStatus(twitter4j.Status)
	 * 
	 * Logic factored out into extractRecord to allow recursion for retweets
	 * 
	 */
	public void onStatus(Status status) {
		extractRecord(status);
	}

	/**
	 * @param status
	 * 
	 * Generate a TweetRecord object and submit to 
	 * 
	 */
	private void extractRecord(Status status) {
		TweetRecord record = new TweetRecord();

		//Basic attributes
		record.setId(status.getId());
		//Using SimpleDateFormat "yyyy-MM-dd'T'HH:mm:ss'Z'"
		record.setCreatedAtStr(formatterTo.format(status.getCreatedAt()));
		//Use millis since epoch since Avro doesn't do dates
		record.setCreatedAtLong(status.getCreatedAt().getTime());
		record.setTweet(status.getText());
		
		//User based attributes - denormalized to keep the Source stateless
		//but also so that we can see user attributes changing over time.
		//N.B. we could of course fork this off as a separate stream of user info
		User user = status.getUser();
		record.setUserId(user.getId());
		record.setUserScreenName(user.getScreenName());
		record.setUserFollowersCount(user.getFollowersCount());
		record.setUserFriendsCount(user.getFriendsCount());
		record.setUserLocation(user.getLocation());

		//If it is zero then leave the value null
		if (status.getInReplyToStatusId() != 0) {
			record.setInReplyToStatusId(status.getInReplyToStatusId());
		}

		//If it is zero then leave the value null
		if (status.getInReplyToUserId() != 0) {
			record.setInReplyToUserId(status.getInReplyToUserId());
		}

		//Do geo. N.B. Twitter4J doesn't give use the geo type
		GeoLocation geo = status.getGeoLocation();
		if (geo != null) {
			record.setGeoLat(geo.getLatitude());
			record.setGeoLong(geo.getLongitude());
		}

		//If a status is a retweet then the original tweet gets bundled in
		//Because we can't guarantee that we'll have the original we can
		//extract the original tweet and process it as we have done this time
		//using recursion. Note: we will end up with dupes.
		Status retweetedStatus = status.getRetweetedStatus();
		if (retweetedStatus != null) {
			record.setRetweetedStatusId(retweetedStatus.getId());
			record.setRetweetedUserId(retweetedStatus.getUser().getId());
			extractRecord(retweetedStatus);
		}

		//Submit the populated record onto the channel
		processRecord(record);
	}

	/**
	 * @param record
	 * 
	 * Take a TweetRecord object, encode it and place it on the channel
	 * 
	 */
	private void processRecord(TweetRecord record) {
		try {
			//Using the datum writer (DatumWriter<TweetRecord>) and the
			//encoder that we've already configured, write the passed
			//in record to the ByteArrayOutputStream "outputStream"
			avroDatumWriter.write(record, encoder);
			encoder.flush();
		
			//Build a Flume event based on the bytes on the OutputStream
			Event event = EventBuilder.withBody(outputStream.toByteArray());

			//Submit the event onto the channel
			getChannelProcessor().processEvent(event); 

			//Do a load of stats related stuff
			totalBytesEmitted+=outputStream.size();
			outputStream.reset();
		
			docCount++;
			if ((docCount % reportInterval) == 0) {
				LOGGER.info(String.format("Processed %s docs",
						numFormatter.format(docCount)));
			}
			if ((docCount % statsInterval) == 0) {
				logStats();
			}
			
		} catch (IOException e) {
			LOGGER.error("Problem encoding TweetRecord", e);
		}
	}


	
	private void logStats() {
		double mbEmitted = totalBytesEmitted / (1024 * 1024.0);
		long seconds = (System.currentTimeMillis() - startTime) / 1000;
		seconds = Math.max(seconds, 1);
		LOGGER.info(String.format(
				"Total docs indexed: %s, total skipped docs: %s",
				numFormatter.format(docCount), numFormatter.format(skippedDocs)));
		LOGGER.info(String.format("    %s docs/second",
				numFormatter.format(docCount / seconds)));
		LOGGER.info(String.format("Run took %s seconds and processed:",
				numFormatter.format(seconds)));
		LOGGER.info(String.format(
				"    %s MB/sec sent to channel",
				numFormatter.format(((float) totalBytesEmitted / (1024 * 1024))
						/ seconds)));
		LOGGER.info(String.format("    %s MB text sent to channel",
				numFormatter.format(mbEmitted)));
		LOGGER.info(String.format("There were %s exceptions ignored: ",
				numFormatter.format(exceptionCount)));
	}

	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		// Do nothing...
	}

	public void onScrubGeo(long userId, long upToStatusId) {
		// Do nothing...
	}

	public void onStallWarning(StallWarning warning) {
		LOGGER.info(warning.getMessage());
	}

	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		LOGGER.info("Track Limitatation Notice: Number of Limited Status: " + numberOfLimitedStatuses);
	}

	public void onException(Exception e) {
		LOGGER.error("Exception while streaming tweets", e);
		exceptionCount++;
	}
}
