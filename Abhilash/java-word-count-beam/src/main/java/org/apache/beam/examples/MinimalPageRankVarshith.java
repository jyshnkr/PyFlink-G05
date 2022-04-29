
package org.apache.beam.examples;


import java.io.File;
import java.util.Arrays;

// import org.apache.beam.model.pipeline.v1.RunnerApi.PCollection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;


public class MinimalPageRankVarshith {

  public static void main(String[] args) {
    deleteFiles();
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline p = Pipeline.create(options);
    String dataFolder = "web04";
   
    
   PCollection<KV<String,String>> p1 = VarshithMapper01(p,"go.md",dataFolder);
   PCollection<KV<String,String>> p2 = VarshithMapper01(p,"python.md",dataFolder);
   PCollection<KV<String,String>> p3 = VarshithMapper01(p,"java.md",dataFolder);
   PCollection<KV<String,String>> p4 = VarshithMapper01(p,"README.md",dataFolder);
   

   
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(p1).and(p2).and(p3).and(p4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<KV<String, Iterable<String>>> gBK = mergedList.apply(GroupByKey.<String, String>create());
    PCollection<String> pLinksString = gBK.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    pLinksString.apply(TextIO.write().to("VarshithPR"));   
    p.run().waitUntilFinish();
  }

  public static PCollection<KV<String,String>> VarshithMapper01(Pipeline p, String filename, String dataFolder){
    
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
      if(f.getName().startsWith("Varshith")){
        f.delete();
      }
    }
  }

}
