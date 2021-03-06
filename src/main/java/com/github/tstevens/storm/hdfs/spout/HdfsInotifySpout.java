package com.github.tstevens.storm.hdfs.spout;

import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HdfsInotifySpout extends BaseRichSpout {

    private static final long serialVersionUID = 7252687842097019979L;

    public static final String STREAM_ID = "hdfs-events";
	public static final String EVENT_TYPE_FIELD = "type";
    public static final String PATH_FIELD = "path";
    public static final String SIZE_FIELD = "size";
    public static final String TIME_FIELD = "time";

    private ISpoutOutputCollector collector;
    private String watchedPath;
    private URI hdfsUri;

    private HdfsAdmin dfs;
    private DFSInotifyEventInputStream stream;
    private long lastReadTxId;

    public HdfsInotifySpout(URI hdfsUri, String watchedPath){
        this.watchedPath = Objects.requireNonNull(watchedPath);
        this.hdfsUri = Objects.requireNonNull(hdfsUri);
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        lastReadTxId = 0;

        Configuration conf = new Configuration();

        try {
			dfs = new HdfsAdmin(hdfsUri, conf);
			stream = dfs.getInotifyEventStream();
		} catch (IOException e) {
			collector.reportError(e);
		}
    }
    
    @Override
    public void nextTuple() {
        try {
            EventBatch batch;
            while ((batch = stream.poll(100, TimeUnit.MILLISECONDS)) !=null ){ // TODO Add jitter to wait time
                for(Event raw_event : batch.getEvents()){
                    raw_event.getEventType();
                    switch (raw_event.getEventType()){
                        case CLOSE:
                            CloseEvent closeEvent = (CloseEvent) raw_event;
                            if(closeEvent.getPath().startsWith(watchedPath)){
                                collector.emit(STREAM_ID, new Values(closeEvent.getPath(), closeEvent.getFileSize(), new Date(closeEvent.getTimestamp()), closeEvent.getEventType().toString()), null);
                            }
                            break;
                        default:
                            //TODO Handle other event types
                            break;
                    }
                }
                lastReadTxId = batch.getTxid();
            }
        } catch (IOException e) {
            collector.reportError(e);
        } catch (MissingEventsException e) {
            // Log? missed updates but able to continue
            System.out.println("Missed events: " + e.getMessage());
        } catch (InterruptedException e){
            //Ignore and finish
        }
    }
    
    @Override
    public void deactivate() {
        stream = null;
    }
    
    @Override
    public void activate() {
        try {
            stream = lastReadTxId != 0 ? dfs.getInotifyEventStream(lastReadTxId) : dfs.getInotifyEventStream();
        } catch (IOException e) {
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(STREAM_ID, new Fields( PATH_FIELD, SIZE_FIELD, TIME_FIELD, EVENT_TYPE_FIELD));
    }
}
