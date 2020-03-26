package edu.usfca.dataflow;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import com.google.protobuf.Message;

import edu.usfca.dataflow.transforms.ExtractData.ExtractAddicts;
import edu.usfca.dataflow.transforms.PurchaserProfiles.GetProfilesFromEvents;
import edu.usfca.dataflow.transforms.PurchaserProfiles.MergeProfiles;

/**
 * Follow the instructions on github: https://github.com/cs-rocks/cs686-lectures/blob/master/projects/project3-README.md
 *
 * Summary of Tasks:
 *
 * Task A: Complete the {@link edu.usfca.dataflow.utils.LogParser} class. Read the instructions carefully, and choose to
 * reuse your own code from Lab05 or instructor's code. You'll need to finish this before attempting other tasks. Make
 * sure your code passes all tests found in __TestLogParser.
 *
 * Task B: Define your purchaser_profile proto message in profile.proto. Follow the instructions in this file. Then,
 * complete the {@link edu.usfca.dataflow.transforms.PurchaserProfiles} class. You'll need to implement to PTransforms.
 * Note that whenever you make changes to your proto file (profile.proto), you'll need to re-build your gradle project
 * so that protoc library generates Java files based on your updated proto files (e.g., run "gradle test" under
 * "java/dataflow" directory).
 *
 * Task C: Complete the {@link edu.usfca.dataflow.transforms.ExtractData} class. This contains three PTransforms you
 * need to implement. I strongly recommend that, before you begin writing your code, try to draw Dataflow pipeline
 * diagrams to figure out what PTransforms you want to apply as intermediate steps between the input PCollection and the
 * output PCollection. If you do not "plan" things before writing code, you may end up rewriting a lot.
 *
 * ----------------------------------------------------------------
 *
 * NOTE: As you work on Task C, you may later wish that you had done things differently in Task B. That's normal, and
 * that's expected. Yet, you may find it easier to get "one thing done" at a time, so as to make minimal changes to your
 * code.
 *
 * Testing on BigQuery: You can upload the JSON files to BigQuery, and analyze the data there. That's not required for
 * this project, but it could be useful when you are debugging your code (because you can quickly write a SQL query to
 * aggregate values as opposed to debugging your Java program). If you need help parsing the httpURL string in BigQuery
 * (to extract URL parameters), check the queries from L17.
 * 
 * ----------------------------------------------------------------
 *
 * Remember: You can add more fields to existing proto messages (except for the ones explicitly marked as "NO-NO"), and
 * create new proto messages/java classes. Doing so may help you write concise code for your pipeline.
 */
public class Main {
  // TODO: Make sure you change USER_EMAIL below to your @dons email address.
  final private static String USER_EMAIL = "hmulabagula@dons.usfca.edu";

  static String getUserEmail() {
    return USER_EMAIL;
  }

  /*
   * Grading system will use the provided sample txt files and additional files (for hidden tests). Main method is
   * provided for your convenience, but it will not be used by the grading system.
   */
  public static void main(String[] args) {
    Pipeline p = Pipeline.create();

    p.apply(TextIO.read().from("C:\\Users\\mahal\\cs686\\projects\\cs686-proj3-hrishi-moola\\java\\dataflow\\resources\\sample-tiny.txt")) //
        .apply(new GetProfilesFromEvents()).apply(new MergeProfiles()).apply(new ExtractAddicts("id686486", 4))
        .apply(ParDo.of(new DoFn<Message, Void>() {
          @ProcessElement
          public void process(ProcessContext c) {
            System.out.format(c.element().toString() + "\n");
          }
        }));
    System.out.println("ta-da! All done!");

    p.run().waitUntilFinish();
  }
}
