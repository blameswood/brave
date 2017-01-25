package brave.jaxrs2;

import brave.ServerHandler;
import brave.Span;
import brave.Tracer;
import brave.Tracer.SpanInScope;
import brave.parser.Parser;
import brave.parser.TagsParser;
import brave.propagation.Propagation;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import javax.annotation.Priority;
import javax.ws.rs.ConstrainedTo;
import javax.ws.rs.RuntimeType;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import zipkin.Constants;
import zipkin.TraceKeys;

import static com.github.kristofa.brave.internal.Util.checkNotNull;
import static javax.ws.rs.RuntimeType.SERVER;

@Provider
@PreMatching // no state is shared between different requests
@Priority(0) // to make the span in scope visible to other filters
@ConstrainedTo(SERVER)
public class BraveTracingContainerFilter implements ContainerRequestFilter,
    ContainerResponseFilter {

  /** Creates a tracing filter with defaults. Use {@link #builder(Tracer)} to customize. */
  public static BraveTracingContainerFilter create(Tracer tracer) {
    return builder(tracer).build();
  }

  public static Builder builder(Tracer tracer) {
    return new Builder(tracer);
  }

  public static final class Builder {
    final Tracer tracer;
    Config config = new Config();

    Builder(Tracer tracer) { // intentionally hidden
      this.tracer = checkNotNull(tracer, "tracer");
    }

    public Builder config(Config config) {
      this.config = checkNotNull(config, "config");
      return this;
    }

    public BraveTracingContainerFilter build() {
      return new BraveTracingContainerFilter(this);
    }
  }

  // Not final so it can be overridden to customize tags
  public static class Config
      extends ServerHandler.Config<ContainerRequestContext, ContainerResponseContext> {

    @Override protected Parser<ContainerRequestContext, String> spanNameParser() {
      return ContainerRequestContext::getMethod;
    }

    @Override protected Parser<ContainerRequestContext, zipkin.Endpoint> requestAddressParser() {
      return new ClientAddressParser("");
    }

    @Override protected TagsParser<ContainerRequestContext> requestTagsParser() {
      return (req, span) -> span.tag(TraceKeys.HTTP_URL,
          req.getUriInfo().getRequestUri().toString());
    }

    @Override protected TagsParser<ContainerResponseContext> responseTagsParser() {
      return (res, span) -> {
        int httpStatus = res.getStatus();
        if (httpStatus < 200 || httpStatus > 299) {
          span.tag(TraceKeys.HTTP_STATUS_CODE, String.valueOf(httpStatus));
        }
      };
    }
  }

  final Tracer tracer;
  final ServerHandler<ContainerRequestContext, ContainerResponseContext> serverHandler;
  final TraceContext.Extractor<ContainerRequestContext> contextExtractor;

  BraveTracingContainerFilter(Builder builder) {
    tracer = builder.tracer;
    serverHandler = ServerHandler.create(builder.config);
    contextExtractor = Propagation.B3_STRING.extractor(ContainerRequestContext::getHeaderString);
  }

  @Override public void filter(ContainerRequestContext context) {
    TraceContextOrSamplingFlags contextOrFlags = contextExtractor.extract(context);
    Span span = contextOrFlags.context() != null
        ? tracer.joinSpan(contextOrFlags.context())
        : tracer.newTrace(contextOrFlags.samplingFlags());
    serverHandler.handleReceive(context, span);
    context.setProperty(Span.class.getName(), span);
    context.setProperty(SpanInScope.class.getName(), tracer.withSpanInScope(span));
  }

  @Override
  public void filter(ContainerRequestContext request, ContainerResponseContext response) {
    Span span = (Span) request.getProperty(Span.class.getName());
    SpanInScope spanInScope = (SpanInScope) request.getProperty(SpanInScope.class.getName());
    Response.StatusType statusInfo = response.getStatusInfo();
    if (statusInfo.getFamily() == Response.Status.Family.SERVER_ERROR) {
      span.tag(Constants.ERROR, statusInfo.getReasonPhrase());
    }
    serverHandler.handleSend(response, span);
    spanInScope.close();
  }
}