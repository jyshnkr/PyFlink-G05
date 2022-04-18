package edu.nwmissouri.groupOfFive.nandinikandi;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankKandi {

    // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

    public static void main(String[] args) {

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
        // Convert to a custom Value object (RankedPage) in preparation for Job 2
        //PCollection<KV<String, RankedPage>> job2in = grouped.apply(ParDo.of(new Job1Finalizer()));
        PCollection<String> pColLinkString = grouped.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
        pColLinkString.apply(TextIO.write().to("kandiOutput"));  
        p.run().waitUntilFinish();
            
      }
    
      private static PCollection<KV<String, String>> kandiMapper1(Pipeline p,String dataFile ,String dataFolder ) {
        String dataPath = dataFolder + "/" + dataFile;
       
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
