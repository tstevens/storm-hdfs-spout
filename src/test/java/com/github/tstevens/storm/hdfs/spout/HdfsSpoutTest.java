package com.github.tstevens.storm.hdfs.spout;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.TupleCaptureBolt;
import backtype.storm.topology.TopologyBuilder;

public class HdfsSpoutTest {

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    private static MiniDFSCluster hdfsCluster;

    private static String hdfsURI;

    @BeforeClass
    public static void beforeClass() throws IOException{
        File baseDir = folder.newFolder("hdfs").getAbsoluteFile();

        Configuration conf = new Configuration(true);

        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);

        hdfsCluster = builder.build();
        hdfsCluster.waitActive();

        hdfsURI = "hdfs://localhost:"+ hdfsCluster.getNameNodePort() + "/";
    }

    @AfterClass
    public static void afterClass(){
        if(hdfsCluster != null){
            hdfsCluster.shutdown();
        }
        hdfsURI = null;
    }

    @Test
    public void testSpout(){
        HdfsInotifySpout spout = new HdfsInotifySpout(hdfsURI, "/", "test");

        TupleCaptureBolt capture = new TupleCaptureBolt();

        Config conf = new Config();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", spout, 1).setNumTasks(1);
        builder.setBolt("bolt", capture, 1).setNumTasks(1).shuffleGrouping("spout", HdfsInotifySpout.STREAM_ID);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test-topo", conf, builder.createTopology());

        try {
            Thread.sleep(2500);

            hdfsCluster.getFileSystem().createNewFile(new Path("/test.txt"));

            Thread.sleep(200);
        } catch (IllegalArgumentException | IOException | InterruptedException e) {
            Assert.fail(e.getMessage());
        }

        cluster.shutdown();

        Assert.assertThat(capture.getResults().size(), CoreMatchers.is(1));
    }
}
