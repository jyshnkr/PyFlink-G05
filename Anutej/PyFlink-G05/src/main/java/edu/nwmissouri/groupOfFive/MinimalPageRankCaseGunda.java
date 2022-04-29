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
import java.util.ArrayList;
import java.io.File;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
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
 *   2. Specifying 'inline' transformssx
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalPageRankCaseGunda {

  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankingPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankingPage>> receiver) {
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
      receiver.output(KV.of(element.getKey(), new RankingPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, RankingPage>, KV<String, RankingPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankingPage>> receiver) {
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
      receiver.output(KV.of(element.getKey(), new RankingPage(element.getKey(), voters)));
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankingPage>>, KV<String, RankingPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankingPage>> receiver) {
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
      receiver.output(KV.of(element.getKey(), new RankingPage(element.getKey(), voters)));
    }
  }

  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
   
    Pipeline p = Pipeline.create(options);
   
    String dataFolder = "web04";
      
   PCollection<KV<String,String>> at1 = AnuTejaMapper01(p,"go.md",dataFolder);
   
   PCollection<KV<String,String>> at2 = AnuTejaMapper01(p,"python.md",dataFolder);
   
   PCollection<KV<String,String>> at3 = AnuTejaMapper01(p,"java.md",dataFolder);
   
   PCollection<KV<String,String>> at4 = AnuTejaMapper01(p,"README.md",dataFolder);
   
   PCollection<KV<String,String>> at5 = AnuTejaMapper01(p,"cpp.md",dataFolder);

   PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(at1).and(at2).and(at3).and(at4).and(at5);
 
   PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());

   PCollection<KV<String, Iterable<String>>> gL =mergedList.apply(GroupByKey.create());
 
   PCollection<String> pLinksString = gL.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
   
   pLinksString.apply(TextIO.write().to("AnuTejaPR"));  
   
   p.run().waitUntilFinish();
 
  }

  public static PCollection<KV<String,String>> AnuTejaMapper01(Pipeline p, String filename, String dataFolder){
   
    String newdataPath = dataFolder + "/" + filename;
   
    PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
   
    PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     
    PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings())).via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     
    PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings())).via((String outLink) -> KV.of(filename,outLink)));
   
    return pColKVPairs;

  }

  private static PCollection<KV<String, RankingPage>> runJob2Iteration(
  PCollection<KV<String, RankingPage>> kvReducedPairs) {

//    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

// KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
// KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
// KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

// PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
//     .apply(GroupByKey.<String, RankedPage>create());

// KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
// KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
// 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

// PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

// KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
// python.md, 1.00000,1]}
// KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}

PCollection<KV<String, RankingPage>> updatedOutput = null;
return updatedOutput;
}

public static  void deleteFiles(){
  final File file = new File("./");
  for (File f : file.listFiles()){
    if(f.getName().startsWith("AnuTeja")){
      f.delete();
    }
  }

}
}