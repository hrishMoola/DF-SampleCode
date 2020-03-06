package edu.usfca.dataflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

/**
 * Example command (this would execute Main.main() in java/dataflow):
 *
 * <your-repo>/java $ gradle run -Pargs="--pathToFile=/Users/haden/Downloads/data.gz"
 *
 */
public interface MyOptions extends DataflowPipelineOptions {
  @Description("Absolute path to an input file. e.g, /Users/haden/Downloads/data.gz")
  @Default.String("")
  String getPathToInputFile();

  void setPathToInputFile(String pathToInputFile);

  @Description("Absolute path to an output dir. e.g, /Users/haden/Downloads/output")
  @Validation.Required
  String getPathToOutputDir();

  void setPathToOutputDir(String pathToOutputDir);

  @Description("Debug flag you may find useful.")
  @Default.Boolean(false)
  Boolean getDebug();

  void setDebug(Boolean debug);
}
