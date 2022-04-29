
package edu.nwmissouri.RawData;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

// import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
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
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;

public class MinimalPageRankAbhilash {

  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, AbhiRankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, AbhiRankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<AbhiVotingPage> voters = new ArrayList<AbhiVotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new AbhiVotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new AbhiRankedPage(element.getKey(), voters)));
    }
  }

  static class Job2Mapper extends DoFn<KV<String, AbhiRankedPage>, KV<String, AbhiRankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, AbhiRankedPage> element,
        OutputReceiver<KV<String, AbhiRankedPage>> receiver) {
      Integer votes = 0;
      ArrayList<AbhiVotingPage> voters = element.getValue().getVoters();
      if (voters instanceof Collection) {
        votes = ((Collection<AbhiVotingPage>) voters).size();
      }
      for (AbhiVotingPage vp : voters) {
        String pageName = vp.getName();
        Double pageRank = vp.getRank();
        String contributingPageName = element.getKey();
        Double contributingPageRank = element.getValue().getRank();
        AbhiVotingPage contributor = new AbhiVotingPage(contributingPageName, contributingPageRank, votes);
        ArrayList<AbhiVotingPage> arr = new ArrayList<AbhiVotingPage>();
        arr.add(contributor);
        receiver.output(KV.of(vp.getName(), new AbhiRankedPage(pageName, pageRank, arr)));
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<AbhiRankedPage>>, KV<String, AbhiRankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<AbhiRankedPage>> element,
        OutputReceiver<KV<String, AbhiRankedPage>> receiver) {
      String page = element.getKey();
      Iterable<AbhiRankedPage> rankedPages = element.getValue();
      Double dampingFactor = 0.85;
      Double updatedRank = (1 - dampingFactor);
      ArrayList<AbhiVotingPage> newVoters = new ArrayList<AbhiVotingPage>();
      for (AbhiRankedPage pg : rankedPages) {
        if (pg != null) {
          for (AbhiVotingPage vPage : pg.getVoters()) {
            newVoters.add(vPage);
            updatedRank += (dampingFactor) * vPage.getRank() / (double) vPage.getVotes();
          }
        }
      }
      receiver.output(KV.of(page, new AbhiRankedPage(page, updatedRank, newVoters)));
    }

  }

  public static void main(String[] args) {
    deleteFiles();
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";

    PCollection<KV<String, String>> p1 = AbhiMapper01(p, "go.md", dataFolder);
    PCollection<KV<String, String>> p2 = AbhiMapper01(p, "python.md", dataFolder);
    PCollection<KV<String, String>> p3 = AbhiMapper01(p, "java.md", dataFolder);
    PCollection<KV<String, String>> p4 = AbhiMapper01(p, "README.md", dataFolder);

    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(p1).and(p2).and(p3).and(p4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<KV<String, Iterable<String>>> gBK = mergedList.apply(GroupByKey.<String, String>create());
    PCollection<KV<String, AbhiRankedPage>> job2in = gBK.apply(ParDo.of(new Job1Finalizer()));
    // end of Job1

    // Start Job2

    PCollection<KV<String, AbhiRankedPage>> updatedOutput = null;
    PCollection<KV<String, AbhiRankedPage>> mappedKVs = null;

    int iterations = 50;
    for (int i = 0; i < iterations; i++) {
      if (i == 0) {
        mappedKVs = job2in
            .apply(ParDo.of(new Job2Mapper()));
      } else {
        mappedKVs = updatedOutput
            .apply(ParDo.of(new Job2Mapper()));
      }
      PCollection<KV<String, Iterable<AbhiRankedPage>>> reducedKVs = mappedKVs
          .apply(GroupByKey.<String, AbhiRankedPage>create());
      updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));
    }

    PCollection<String> pLinksString = updatedOutput
        .apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut) -> mergeOut.toString()));
    pLinksString.apply(TextIO.write().to("AbhiPR"));
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String, String>> AbhiMapper01(Pipeline p, String filename, String dataFolder) {

    String newdataPath = dataFolder + "/" + filename;
    PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
    PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
        .via((String linkLine) -> linkLine.substring(linkLine.indexOf("(") + 1, linkLine.length() - 1)));
    PCollection<KV<String, String>> pColKVPairs = pcolLinks
        .apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via((String outLink) -> KV.of(filename, outLink)));
    return pColKVPairs;
  }

  public static void deleteFiles() {
    final File file = new File("./");
    for (File f : file.listFiles()) {
      if (f.getName().startsWith("Abhi")) {
        f.delete();
      }
    }
  }

}
