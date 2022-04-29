package edu.nwmissouri.groupOfFive.nandinikandi;

import java.io.File;
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

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if (voters instanceof Collection) {
        votes = ((Collection<VotingPage>)voters).size();
      }
      for(VotingPage vp : voters) {
        String pageName = vp.getNameIn();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> arrVP = new  ArrayList<VotingPage>();
        arrVP.add(contributor);
        receiver.output(KV.of(vp.getNameIn(), new RankedPage(pageName, pageRank, arrVP)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      String contributingPageName = element.getKey();
      Iterable<RankedPage> rankedPages = element.getValue();
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
      for (RankedPage pg : rankedPages) {
        if (pg != null) {
          for (VotingPage vp : pg.getVoters()){
            newVoters.add(vp);
            updatedRank += (dampingFactor) * vp.getRank() / (double) vp.getVotes();
          }
        }
      }
      receiver.output(KV.of(contributingPageName, new RankedPage(contributingPageName, updatedRank, newVoters)));
    }
  }


  private static PCollection<KV<String, String>> kandiMapper1(Pipeline p,String dataFile ,String dataFolder ) {
    String dataPath = dataFolder + "/" + dataFile;
    
    PCollection<String> pcolInputLines=p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLinkLines= pcolInputLines.apply(Filter.by((String line)->line.startsWith("[")))
    .apply(
      MapElements.into(TypeDescriptors.strings())
          .via((String linkLine) -> linkLine.strip()));
          
    PCollection<String> pcolLinks=pcolLinkLines.apply(MapElements
    .into(TypeDescriptors.strings())
    .via(
    (String linkline)->
    linkline.substring(linkline.indexOf("(")+1,linkline.length()-1)));

    // from README.md to Key Value Pairs
    PCollection<KV<String,String>> pColKeyValuePairs=pcolLinks.apply(
      MapElements.into(
        TypeDescriptors.kvs(
          TypeDescriptors.strings(),TypeDescriptors.strings()
        )
      ).via((String outlink) -> KV.of(dataFile,outlink)));
    return pColKeyValuePairs;    
  }

  /**
 * Run one iteration of the Job 2 Map-Reduce process
 * Notice how the Input Type to Job 2.
 * Matches the Output Type from Job 2.
 * How important is that for an iterative process?
 * 
 * @param kvReducedPairs - takes a PCollection<KV<String, RankedPage>> with initial ranks.
 * @return - returns a PCollection<KV<String, RankedPage>> with updated ranks.
 */
  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
    PCollection<KV<String, RankedPage>> kvReducedPairs) {
  
    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));
    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());
    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));
    return updatedOutput;

}

  public static  void deleteFiles(){
    final File file = new File("./");
    for (File f : file.listFiles()){
      if(f.getName().startsWith("kandiOutput")){
        f.delete();
      }
    }
  }

  public static void main(String[] args) {
    deleteFiles();
    PipelineOptions myOptions = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(myOptions);

    String folder="web04";
    PCollection<KV<String,String>> pcolKV1 = kandiMapper1(p,"go.md",folder);
    PCollection<KV<String,String>> pcolKV2 = kandiMapper1(p,"python.md",folder);
    PCollection<KV<String,String>> pcolKV3 = kandiMapper1(p,"java.md",folder);
    PCollection<KV<String,String>> pcolKV4 = kandiMapper1(p,"README.md",folder);
            
    PCollectionList<KV<String, String>> pCollList = PCollectionList.of(pcolKV1).and(pcolKV2).and(pcolKV3).and(pcolKV4);
    PCollection<KV<String, String>> mergedKV = pCollList.apply(Flatten.<KV<String,String>>pCollections());

    // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> kvStringReducedPairs = mergedKV.apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = kvStringReducedPairs.apply(ParDo.of(new Job1Finalizer()));
    
    // END JOB 1

    // BEGIN ITERATIVE JOB 2
    
    PCollection<KV<String, RankedPage>> job2out = null; 

    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      // use job2in to calculate job2 out      
      job2out = runJob2Iteration(job2in);
      // update job2in so it equals the new job2out
      job2in = job2out;      
    }


    // END ITERATIVE JOB 2
    // ========================================

    // Map KVs to strings before outputting
    PCollection<String> output = job2out.apply(MapElements.into(
        TypeDescriptors.strings())
        .via(kv -> kv.toString()));

    // Write from Beam back out into the real world
    output.apply(TextIO.write().to("kandiOutput"));  
    p.run().waitUntilFinish();
        
  }

}
