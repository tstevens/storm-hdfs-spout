package com.github.tstevens.storm.hdfs.spout;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
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

    private static final String PATH_FIELD = "path";
    private static final String SIZE_FIELD = "size";
    private static final String TIME_FIELD = "time";

    private ISpoutOutputCollector collector;
    private String watchedPath;
    private String streamId;
    private String hdfsUri;

    private HdfsAdmin dfs;
    private DFSInotifyEventInputStream stream;

    public HdfsInotifySpout(String hdfsUri, String watchedPath, String streamId){
        this.watchedPath = watchedPath;
        this.streamId = streamId;
        this.hdfsUri = hdfsUri;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            URI uri = new URI(hdfsUri);
            Configuration conf = new Configuration();
    
            this.dfs = new HdfsAdmin(uri, conf);
            this.stream = this.dfs.getInotifyEventStream();
        } catch(IOException | URISyntaxException e){
            collector.reportError(e);
        }
    }
    
    @Override
    public void nextTuple() {
        try {
            Event raw_event = null;
            while ((raw_event = this.stream.poll(100, TimeUnit.MILLISECONDS)) !=null ){
                if(raw_event instanceof CloseEvent){
                    CloseEvent closeEvent = (CloseEvent) raw_event;
                    if(closeEvent.getPath().startsWith(watchedPath)){
                        collector.emit(streamId, new Values(closeEvent.getPath(), closeEvent.getFileSize(), new Date(closeEvent.getTimestamp())), null);
                    }
                }
            }
        } catch (IOException e) {
            collector.reportError(e);
        } catch (MissingEventsException e) {
            // Log? missed updates but able to continue
        } catch (InterruptedException e){
            //Ignore and finish
        }
    }
    
    @Override
    public void deactivate() {
        this.stream = null;
    }
    
    @Override
    public void activate() {
        try {
            this.stream = this.dfs.getInotifyEventStream();
        } catch (IOException e) {
            collector.reportError(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(streamId, new Fields(PATH_FIELD, SIZE_FIELD, TIME_FIELD));
    }
}