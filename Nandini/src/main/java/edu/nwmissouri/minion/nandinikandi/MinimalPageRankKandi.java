package edu.nwmissouri.minion.nandinikandi;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankKandi {
    public static void main(String[] args) {

        // Create a PipelineOptions object. This object lets us set various execution
        // myOptions for our pipeline, such as the runner you wish to use. This example
        // will run with the DirectRunner by default, based on the class path configured
        // in its dependencies.
        PipelineOptions myOptions = PipelineOptionsFactory.create();
    
        Pipeline p = Pipeline.create(myOptions);
   
        String folder="web04";
        PCollection<KV<String,String>> pcolKV1 = kandiMapper1(p,"go.md",folder);
        PCollection<KV<String,String>> pcolKV2 = kandiMapper1(p,"python.md",folder);
        PCollection<KV<String,String>> pcolKV3 = kandiMapper1(p,"java.md",folder);
        PCollection<KV<String,String>> pcolKV4 = kandiMapper1(p,"README.md",folder);
        PCollection<KV<String,String>> pcolKV5 = kandiMapper1(p,"c.md",folder);
               
        PCollectionList<KV<String, String>> pCollList = PCollectionList.of(pcolKV1).and(pcolKV2).and(pcolKV3).and(pcolKV4).and(pcolKV5);
        PCollection<KV<String, String>> l = pCollList.apply(Flatten.<KV<String,String>>pCollections());
        PCollection<KV<String, Iterable<String>>> grouped =l.apply(GroupByKey.create());
        PCollection<String> pColLinkString = grouped.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
        pColLinkString.apply(TextIO.write().to("kandiOutput"));  
        p.run().waitUntilFinish();
            
      }
    
      private static PCollection<KV<String, String>> kandiMapper1(Pipeline p,String dataFile ,String dataFolder ) {
        String dataPath = dataFolder + "/" + dataFile;
        //p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
    
        PCollection<String> pcolInputLines=p.apply(TextIO.read().from(dataPath));
        PCollection<String> pcolLinkLines= pcolInputLines.apply(Filter.by((String line)->line.startsWith("[")));
        PCollection<String> pcolLinks=pcolLinkLines.apply(MapElements
        .into(TypeDescriptors.strings())
        .via(
        (String linkline)->
        linkline.substring(linkline.indexOf("(")+1,linkline.length()-1)));

        // from README.md to Key Value Pairs
        PCollection<KV<String,String>> pColKeyValuePairs=pcolLinks.apply(MapElements

        .into(
        TypeDescriptors.kvs(
            TypeDescriptors.strings(),TypeDescriptors.strings()
            )
            )

        .via(
        outlink->KV.of(dataFile,outlink)
    
        ));
        return pColKeyValuePairs;
    
      }
}
