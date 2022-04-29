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

import java.util.ArrayList;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   default_example: true
//   context_line: 71
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms
//     - Quickstart

import java.util.Arrays;
import java.util.Collection;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalPageRankPariveshita {
  
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, ThotaRankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, ThotaRankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<ThotaVotingPage> voters = new ArrayList<ThotaVotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new ThotaVotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String,ThotaRankedPage>, KV<String,ThotaRankedPage>>{
    @ProcessElement
    public void processElement(@Element KV<String, ThotaRankedPage> element,
    OutputReceiver<KV<String,ThotaRankedPage>> receiver){
      Integer votes = 0;
      ArrayList<ThotaVotingPage> voters = element.getValue().getVoters();
      if(voters instanceof Collection){
         votes = ((Collection<ThotaVotingPage>)voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<ThotaVotingPage> arr = new ArrayList<ThotaVotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(), new ThotaRankedPage(pageName,pageRank,arr)));
      }
    }
}

static class Job2Updater extends DoFn<KV<String, Iterable<ThotaRankedPage>>, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, Iterable<ThotaRankedPage>> element,
      OutputReceiver<KV<String, ThotaRankedPage>> receiver) {
  String page = element.getKey();
  Iterable<ThotaRankedPage> rankedPages = element.getValue();
  Double dampingFactor = 0.85;
  Double updatedRank = (1-dampingFactor);
  ArrayList<ThotaVotingPage> newVoters = new ArrayList<ThotaVotingPage>();
  for(RankedPage pg : rankedPages){
    if(pg != null){
      for(ThotaVotingPage vPage : pg.getVoters()){
        newVoters.add(vPage);
        updatedRank += (dampingFactor) * vPage.getRank() / (double)vPage.getVotes();
      }
    }
  }
  receiver.output(KV.of(page, new RankedPage(page, updatedRank, newVoters)));
  }
}

  public static void main(String[] args) {
    
    deleteFiles();

    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    
    String dataFolder = "web04";
   
    PCollection<KV<String,String>> p1 = PariveshitaMapper01(p,"go.md",dataFolder);
   
    PCollection<KV<String,String>> p2 =  PariveshitaMapper01(p,"python.md",dataFolder);
   
    PCollection<KV<String,String>> p3 =  PariveshitaMapper01(p,"java.md",dataFolder);
   
    PCollection<KV<String,String>> p4 =  PariveshitaMapper01(p,"README.md",dataFolder);

    PCollection<KV<String,String>> p5 =  PariveshitaMapper01(p,"erlang.md",dataFolder);



   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(p1).and(p2).and(p3).and(p4).and(p5);
  
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());

    PCollection<KV<String, Iterable<String>>> groupedList =mergedList.apply(GroupByKey.<String, String>create());

    PCollection<KV<String, ThotaRankedPage>> job2Input = groupedList.apply(ParDo.of(new Job1Finalizer()));
     
    PCollection<KV<String, ThotaRankedPage>> job2Output = null;
    PCollection<KV<String, ThotaRankedPage>> mappedKVPairs = null;

   
   int iterations = 60;
   for(int i=0; i<iterations; i++){
     if(i==0){
       mappedKVPairs = job2in.apply(ParDo.of(new Job2Mapper()));
     }else{
       mappedKVPairs = newUpdatedOutput.apply(ParDo.of(new Job2Mapper()));
     }
     PCollection<KV<String, Iterable<ThotaRankedPage>>> reducedKVPairs = mappedKVPairs.apply(GroupByKey.<String,ThotaRankedPage>create());
     newUpdatedOutput = reducedKVPairs.apply(ParDo.of(new Job2Updater()));
   }

   PCollection<String> pLinksString = newUpdatedOutput.apply(
   MapElements.into(
       TypeDescriptors.strings())
       .via((myMergeLstout) -> myMergeLstout.toString()));
  
   // By default, it will write to a set of files with names like wordcounts-00001-of-00005
   //longLinkLines.apply(TextIO.write().to("pageRankThota"));
pLinksString.apply(TextIO.write().to("PariveshitaPR"));
p.run().waitUntilFinish();
}

  public static PCollection<KV<String,String>> PariveshitaMapper01(Pipeline p, String filename, String dataFolder){

   
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
    final File file = new File("./");
    for (File f : file.listFiles()){
      if(f.getName().startsWith("Pariveshita")){
        f.delete();
}
}
}
}
