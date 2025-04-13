package io.cdap.wrangler.directives;

import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.*;
import io.cdap.wrangler.api.parser.Type;
import io.cdap.wrangler.api.annotations.*;

import java.util.ArrayList;
import java.util.List;

@Plugin(type = Directive.TYPE)
@Name("delay-execution")
@Description("Delays the transformation by a specified time duration (e.g., 1s, 500ms)")
public class DelayExecution implements Directive {
  private long delayMs;

  @Override
  public UsageDefinition define() {
    TokenDefinition delayArg = TokenDefinition.builder("duration")
      .type(Type.TIME_DURATION)
      .usage("Time duration to delay (e.g., 1s, 500ms)")
      .build();

    return UsageDefinition.builder("delay-execution")
      .define(delayArg)
      .build();
  }

  @Override
  public void initialize(Arguments args) throws Exception {
    TimeDuration duration = args.value("duration");
    this.delayMs = duration.getMilliseconds();
  }

  @Override
  public List<Row> execute(List<Row> rows, ExecutorContext context) throws Exception {
    Thread.sleep(delayMs); // Sleep for the specified time
    return rows;
  }
}
