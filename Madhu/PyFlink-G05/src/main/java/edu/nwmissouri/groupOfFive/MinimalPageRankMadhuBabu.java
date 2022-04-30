/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.groupOfFive;

import java.io.File;
import java.util.ArrayList;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalPageRankMadhuBabu}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalPageRankMadhuBabu {

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

  static class Job2Mapper extends DoFn<KV<String,RankedPage>, KV<String, RankedPage>>{
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
    OutputReceiver<KV<String,RankedPage>> receiver){
      Integer votes = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoters();
      if(voters instanceof Collection){
         votes = ((Collection<VotingPage>)voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(), new RankedPage(pageName,pageRank,arr)));
      }
    }
}

static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, Iterable<RankedPage>> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
  String page = element.getKey();
  Iterable<RankedPage> rankedPages = element.getValue();
  Double dampingFactor = 0.85;
  Double updatedRank = (1-dampingFactor);
  ArrayList<VotingPage> newVoters = new ArrayList<VotingPage>();
  for(RankedPage pg : rankedPages){
    if(pg != null){
      for(VotingPage vPage : pg.getVoters()){
        newVoters.add(vPage);
        updatedRank += (dampingFactor) * vPage.getRank() / (double)vPage.getVotes();
      }
    }
  }
  receiver.output(KV.of(page, new RankedPage(page, updatedRank, newVoters)));
  }
}


  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
   
   PCollection<KV<String,String>> ma1 = MadhuMapper01(p,"go.md",dataFolder);


   PCollection<KV<String,String>> ma2 = MadhuMapper01(p,"python.md",dataFolder);


   PCollection<KV<String,String>> ma3 = MadhuMapper01(p,"java.md",dataFolder);


   PCollection<KV<String,String>> ma4 = MadhuMapper01(p,"README.md",dataFolder);


   PCollection<KV<String,String>> ma5 = MadhuMapper01(p,"react.md",dataFolder);

   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(ma1).and(ma2).and(ma3).and(ma4).and(ma5);

    PCollection<KV<String, String>> PCMergeList = pCollectionList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> PCGrpList =PCMergeList.apply(GroupByKey.create());
    PCollection<KV<String, RankedPage>> job2in = PCGrpList.apply(ParDo.of(new Job1Finalizer()));


    PCollection<KV<String, RankedPage>> newUpdatedOutput = null;
    PCollection<KV<String, RankedPage>> mappedKVPairs = null;

    int iterations = 70;
    for(int i=0; i<iterations; i++){
      if(i==0){
        mappedKVPairs = job2in.apply(ParDo.of(new Job2Mapper()));
      }else{
        mappedKVPairs = newUpdatedOutput.apply(ParDo.of(new Job2Mapper()));
      }
      PCollection<KV<String, Iterable<RankedPage>>> reducedKVPairs = mappedKVPairs.apply(GroupByKey.<String, RankedPage>create());
      newUpdatedOutput = reducedKVPairs.apply(ParDo.of(new Job2Updater()));
    }
 
 
 PCollection<String> pLinksString = newUpdatedOutput.apply(
    MapElements.into(
        TypeDescriptors.strings())
        .via((myMergeLstout) -> myMergeLstout.toString()));
   
 
 pLinksString.apply(TextIO.write().to("MadhuBabuArlaPR"));
 p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> MadhuMapper01(Pipeline p, String filename, String dataFolder){
   
    String newdataPath = dataFolder + "/" + filename;



     PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
     
     PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));



     PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));



     PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }

public static  void deleteFiles(){
  final File file = new File("C:/Users/s542387/OneDrive - nwmissouri.edu/Documents/44517/PyFlink-G05/Madhu/PyFlink-G05");
  for (File f : file.listFiles()){
    if(f.getName().startsWith("Madhu")){
      f.delete();
    }
  }
}
}
