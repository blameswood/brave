package brave.grpc;

import brave.ServerHandler;
import brave.Span;
import brave.Tracer;
import brave.parser.Parser;
import brave.parser.TagsParser;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;

import static com.github.kristofa.brave.internal.Util.checkNotNull;

public class BraveTracingServerInterceptor implements ServerInterceptor {

  /** Creates a tracing interceptor with defaults. Use {@link #builder(Tracer)} to customize. */
  public static BraveTracingServerInterceptor create(Tracer tracer) {
    return builder(tracer).build();
  }

  public static BraveTracingServerInterceptor.Builder builder(Tracer tracer) {
    return new BraveTracingServerInterceptor.Builder(tracer);
  }

  public static final class Builder {
    final Tracer tracer;
    BraveTracingServerInterceptor.Config config = new BraveTracingServerInterceptor.Config();

    Builder(Tracer tracer) { // intentionally hidden
      this.tracer = checkNotNull(tracer, "tracer");
    }

    public BraveTracingServerInterceptor.Builder config(BraveTracingServerInterceptor.Config config) {
      this.config = checkNotNull(config, "config");
      return this;
    }

    public BraveTracingServerInterceptor build() {
      return new BraveTracingServerInterceptor(this);
    }
  }

  // Not final so it can be overridden to customize tags
  public static class Config extends ServerHandler.Config<ServerCall, Status> {

    @Override protected Parser<ServerCall, String> spanNameParser() {
      return c -> c.getMethodDescriptor().getFullMethodName();
    }

    @Override protected TagsParser<ServerCall> requestTagsParser() {
      return (req, span) -> {
      };
    }

    @Override protected TagsParser<Status> responseTagsParser() {
      return (status, span) -> {
        if (!status.getCode().equals(Status.Code.OK)) {
          span.tag("grpc.status_code", String.valueOf(status.getCode()));
        }
      };
    }
  }

  final Tracer tracer;
  final ServerHandler<ServerCall, Status> serverHandler;
  final TraceContext.Extractor<Metadata> contextExtractor;

  BraveTracingServerInterceptor(Builder builder) {
    tracer = builder.tracer;
    serverHandler = ServerHandler.create(builder.config);
    contextExtractor = Propagation.Factory.B3.create(AsciiMetadataKeyFactory.INSTANCE)
        .extractor(Metadata::get);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata requestHeaders, final ServerCallHandler<ReqT, RespT> next) {
    TraceContextOrSamplingFlags contextOrFlags = contextExtractor.extract(requestHeaders);
    Span span = contextOrFlags.context() != null
        ? tracer.joinSpan(contextOrFlags.context())
        : tracer.newTrace(contextOrFlags.samplingFlags());
    return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
      @Override
      public void request(int numMessages) {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
          serverHandler.handleReceive(call, span);
          super.request(numMessages);
        } catch (RuntimeException e) {
          throw serverHandler.handleError(e, span);
        }
      }

      @Override
      public void close(Status status, Metadata trailers) {
        try (Tracer.SpanInScope ws = tracer.withSpanInScope(span)) {
          serverHandler.handleSend(status, span);
          super.close(status, trailers);
        } catch (RuntimeException e) {
          throw serverHandler.handleError(e, span);
        }
      }
    }, requestHeaders);
  }
}