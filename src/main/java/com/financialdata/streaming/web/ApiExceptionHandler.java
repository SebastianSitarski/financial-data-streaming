package com.financialdata.streaming.web;

import java.util.stream.Collectors;

import com.financialdata.streaming.market.CryptoSymbolNotFoundException;
import com.financialdata.streaming.market.InvalidRequestException;
import com.financialdata.streaming.market.LiveMarketDataNotAvailableException;
import com.financialdata.streaming.market.MarketDataRateLimitedException;
import com.financialdata.streaming.market.MarketDataUnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.TypeMismatchException;
import org.springframework.context.MessageSourceResolvable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.method.ParameterValidationResult;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.method.annotation.HandlerMethodValidationException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Single place that shapes every error into {@link ErrorResponse}. Extending
 * {@link ResponseEntityExceptionHandler} routes Spring MVC's own failures (unknown path, wrong
 * method, unsupported media type, parameter validation) through the same format instead of
 * Boot's default error body.
 */
@RestControllerAdvice
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(CryptoSymbolNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleSymbolNotFound(CryptoSymbolNotFoundException ex) {
        return new ErrorResponse("CRYPTO_SYMBOL_NOT_FOUND", ex.getMessage());
    }

    @ExceptionHandler(LiveMarketDataNotAvailableException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ErrorResponse handleLiveDataNotAvailable(LiveMarketDataNotAvailableException ex) {
        return new ErrorResponse("LIVE_DATA_NOT_AVAILABLE", ex.getMessage());
    }

    @ExceptionHandler(InvalidRequestException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ErrorResponse handleInvalidRequest(InvalidRequestException ex) {
        return new ErrorResponse("INVALID_REQUEST", ex.getMessage());
    }

    @ExceptionHandler(MarketDataRateLimitedException.class)
    public ResponseEntity<ErrorResponse> handleRateLimited(MarketDataRateLimitedException ex) {
        // expected under load; one line is enough, the stack trace adds nothing
        log.warn("Market data provider rate limit exceeded: {}", causeSummary(ex));
        ResponseEntity.BodyBuilder response = ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS);
        ex.getRetryAfter().ifPresent(retryAfter ->
                response.header(HttpHeaders.RETRY_AFTER, String.valueOf(retryAfter.toSeconds())));
        return response.body(new ErrorResponse("MARKET_DATA_RATE_LIMITED", ex.getMessage()));
    }

    @ExceptionHandler(MarketDataUnavailableException.class)
    @ResponseStatus(HttpStatus.BAD_GATEWAY)
    public ErrorResponse handleUnavailable(MarketDataUnavailableException ex) {
        // during an upstream outage this fires on every request, so keep the default output to one line
        log.warn("Market data provider error: {} ({})", ex.getMessage(), causeSummary(ex));
        log.debug("Market data provider error details", ex);
        return new ErrorResponse("MARKET_DATA_UNAVAILABLE", "Market data provider is currently unavailable");
    }

    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ErrorResponse handleUnexpected(Exception ex) {
        log.error("Unhandled error while serving request", ex);
        return new ErrorResponse("INTERNAL_ERROR", "Unexpected error");
    }

    @Override
    protected ResponseEntity<Object> handleHandlerMethodValidationException(HandlerMethodValidationException ex,
            HttpHeaders headers, HttpStatusCode status, WebRequest request) {
        String message = ex.getParameterValidationResults().stream()
                .map(ApiExceptionHandler::describe)
                .collect(Collectors.joining("; "));
        return ResponseEntity.status(status).headers(headers).body(new ErrorResponse("INVALID_REQUEST", message));
    }

    @Override
    protected ResponseEntity<Object> handleTypeMismatch(TypeMismatchException ex, HttpHeaders headers,
            HttpStatusCode status, WebRequest request) {
        String name = ex instanceof MethodArgumentTypeMismatchException mismatch ? mismatch.getName()
                : ex.getPropertyName();
        return ResponseEntity.status(status).headers(headers).body(new ErrorResponse("INVALID_REQUEST",
                "Parameter '" + name + "' has invalid value '" + ex.getValue() + "'"));
    }

    /**
     * Fallback for the remaining Spring MVC exceptions (404 for unknown paths, 405, 406, 415, ...).
     */
    @Override
    protected ResponseEntity<Object> handleExceptionInternal(Exception ex, Object body, HttpHeaders headers,
            HttpStatusCode statusCode, WebRequest request) {
        HttpStatus status = HttpStatus.resolve(statusCode.value());
        String code = status != null ? status.name() : "ERROR";
        String message = status != null ? status.getReasonPhrase() : ex.getMessage();
        return ResponseEntity.status(statusCode).headers(headers).body(new ErrorResponse(code, message));
    }

    private static String describe(ParameterValidationResult result) {
        String parameter = result.getMethodParameter().getParameterName();
        String errors = result.getResolvableErrors().stream()
                .map(MessageSourceResolvable::getDefaultMessage)
                .collect(Collectors.joining(", "));
        return "Parameter '" + parameter + "' " + errors;
    }

    private static String causeSummary(Throwable ex) {
        Throwable cause = ex.getCause();
        return cause == null ? "no cause" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}
