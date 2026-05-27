var __typeError = (msg) => {
  throw TypeError(msg);
};
var __accessCheck = (obj, member, msg) => member.has(obj) || __typeError("Cannot " + msg);
var __privateGet = (obj, member, getter) => (__accessCheck(obj, member, "read from private field"), getter ? getter.call(obj) : member.get(obj));
var __privateAdd = (obj, member, value) => member.has(obj) ? __typeError("Cannot add the same private member more than once") : member instanceof WeakSet ? member.add(obj) : member.set(obj, value);
var __privateSet = (obj, member, value, setter) => (__accessCheck(obj, member, "write to private field"), setter ? setter.call(obj, value) : member.set(obj, value), value);
var __privateMethod = (obj, member, method) => (__accessCheck(obj, member, "access private method"), method);
var _client, _currentQuery, _currentQueryInitialState, _currentResult, _currentResultState, _currentResultOptions, _currentThenable, _selectError, _selectFn, _selectResult, _lastQueryWithDefinedData, _staleTimeoutId, _refetchIntervalId, _currentRefetchInterval, _trackedProps, _QueryObserver_instances, executeFetch_fn, updateStaleTimeout_fn, computeRefetchInterval_fn, updateRefetchInterval_fn, updateTimers_fn, clearStaleTimeout_fn, clearRefetchInterval_fn, updateQuery_fn, notify_fn, _a;
import { P as ProtocolError, T as TimeoutWaitingForResponseErrorCode, u as utf8ToBytes, E as ExternalError, M as MissingRootKeyErrorCode, C as Certificate, l as lookupResultToBuffer, R as RequestStatusResponseStatus, U as UnknownError, a as RequestStatusDoneNoReplyErrorCode, b as RejectError, c as CertifiedRejectErrorCode, d as UNREACHABLE_ERROR, I as InputError, e as InvalidReadStateRequestErrorCode, f as ReadRequestType, g as Principal, h as IDL, i as MissingCanisterIdErrorCode, H as HttpAgent, j as encode, Q as QueryResponseStatus, k as UncertifiedRejectErrorCode, m as isV3ResponseBody, n as isV2ResponseBody, o as UncertifiedRejectUpdateErrorCode, p as UnexpectedErrorCode, q as decode, S as Subscribable, r as pendingThenable, s as resolveEnabled, t as shallowEqualObjects, v as resolveStaleTime, w as noop, x as environmentManager, y as isValidTimeout, z as timeUntilStale, A as timeoutManager, B as focusManager, D as fetchState, F as replaceData, G as notifyManager, J as infiniteQueryBehavior, K as hasPreviousPage, L as hasNextPage, N as reactExports, O as shouldThrowError, V as useQueryClient, W as useInternetIdentity, X as createActorWithConfig, Y as Variant, Z as Record, _ as Vec, $ as Service, a0 as Func, a1 as Null, a2 as Float64, a3 as Int, a4 as Text, a5 as Nat, a6 as Nat8, a7 as jsxRuntimeExports, a8 as React, a9 as reactDomExports, aa as ReactDOM, ab as cn$1, ac as Skeleton, ad as clsx } from "./index-DCauIVcJ.js";
const FIVE_MINUTES_IN_MSEC = 5 * 60 * 1e3;
function defaultStrategy() {
  return chain(conditionalDelay(once(), 1e3), backoff(1e3, 1.2), timeout(FIVE_MINUTES_IN_MSEC));
}
function once() {
  let first = true;
  return async () => {
    if (first) {
      first = false;
      return true;
    }
    return false;
  };
}
function conditionalDelay(condition, timeInMsec) {
  return async (canisterId, requestId, status) => {
    if (await condition(canisterId, requestId, status)) {
      return new Promise((resolve) => setTimeout(resolve, timeInMsec));
    }
  };
}
function timeout(timeInMsec) {
  const end = Date.now() + timeInMsec;
  return async (_canisterId, requestId, status) => {
    if (Date.now() > end) {
      throw ProtocolError.fromCode(new TimeoutWaitingForResponseErrorCode(`Request timed out after ${timeInMsec} msec`, requestId, status));
    }
  };
}
function backoff(startingThrottleInMsec, backoffFactor) {
  let currentThrottling = startingThrottleInMsec;
  return () => new Promise((resolve) => setTimeout(() => {
    currentThrottling *= backoffFactor;
    resolve();
  }, currentThrottling));
}
function chain(...strategies) {
  return async (canisterId, requestId, status) => {
    for (const a2 of strategies) {
      await a2(canisterId, requestId, status);
    }
  };
}
const DEFAULT_POLLING_OPTIONS = {
  preSignReadStateRequest: false
};
function hasProperty(value, property) {
  return Object.prototype.hasOwnProperty.call(value, property);
}
function isObjectWithProperty(value, property) {
  return value !== null && typeof value === "object" && hasProperty(value, property);
}
function hasFunction(value, property) {
  return hasProperty(value, property) && typeof value[property] === "function";
}
function isSignedReadStateRequestWithExpiry(value) {
  return isObjectWithProperty(value, "body") && isObjectWithProperty(value.body, "content") && value.body.content.request_type === ReadRequestType.ReadState && isObjectWithProperty(value.body.content, "ingress_expiry") && typeof value.body.content.ingress_expiry === "object" && value.body.content.ingress_expiry !== null && hasFunction(value.body.content.ingress_expiry, "toHash");
}
async function pollForResponse(agent, canisterId, requestId, options = {}) {
  const path = [utf8ToBytes("request_status"), requestId];
  let state;
  let currentRequest;
  const preSignReadStateRequest = options.preSignReadStateRequest ?? false;
  if (preSignReadStateRequest) {
    currentRequest = await constructRequest({
      paths: [path],
      agent,
      pollingOptions: options
    });
    state = await agent.readState(canisterId, { paths: [path] }, void 0, currentRequest);
  } else {
    state = await agent.readState(canisterId, { paths: [path] });
  }
  if (agent.rootKey == null) {
    throw ExternalError.fromCode(new MissingRootKeyErrorCode());
  }
  const cert = await Certificate.create({
    certificate: state.certificate,
    rootKey: agent.rootKey,
    canisterId,
    blsVerify: options.blsVerify,
    agent
  });
  const maybeBuf = lookupResultToBuffer(cert.lookup_path([...path, utf8ToBytes("status")]));
  let status;
  if (typeof maybeBuf === "undefined") {
    status = RequestStatusResponseStatus.Unknown;
  } else {
    status = new TextDecoder().decode(maybeBuf);
  }
  switch (status) {
    case RequestStatusResponseStatus.Replied: {
      return {
        reply: lookupResultToBuffer(cert.lookup_path([...path, "reply"])),
        certificate: cert
      };
    }
    case RequestStatusResponseStatus.Received:
    case RequestStatusResponseStatus.Unknown:
    case RequestStatusResponseStatus.Processing: {
      const strategy = options.strategy ?? defaultStrategy();
      await strategy(canisterId, requestId, status);
      return pollForResponse(agent, canisterId, requestId, {
        ...options,
        // Pass over either the strategy already provided or the new one created above
        strategy,
        request: currentRequest
      });
    }
    case RequestStatusResponseStatus.Rejected: {
      const rejectCode = new Uint8Array(lookupResultToBuffer(cert.lookup_path([...path, "reject_code"])))[0];
      const rejectMessage = new TextDecoder().decode(lookupResultToBuffer(cert.lookup_path([...path, "reject_message"])));
      const errorCodeBuf = lookupResultToBuffer(cert.lookup_path([...path, "error_code"]));
      const errorCode = errorCodeBuf ? new TextDecoder().decode(errorCodeBuf) : void 0;
      throw RejectError.fromCode(new CertifiedRejectErrorCode(requestId, rejectCode, rejectMessage, errorCode));
    }
    case RequestStatusResponseStatus.Done:
      throw UnknownError.fromCode(new RequestStatusDoneNoReplyErrorCode(requestId));
  }
  throw UNREACHABLE_ERROR;
}
async function constructRequest(options) {
  var _a2;
  const { paths, agent, pollingOptions } = options;
  if (pollingOptions.request && isSignedReadStateRequestWithExpiry(pollingOptions.request)) {
    return pollingOptions.request;
  }
  const request = await ((_a2 = agent.createReadStateRequest) == null ? void 0 : _a2.call(agent, {
    paths
  }, void 0));
  if (!isSignedReadStateRequestWithExpiry(request)) {
    throw InputError.fromCode(new InvalidReadStateRequestErrorCode(request));
  }
  return request;
}
const metadataSymbol = Symbol.for("ic-agent-metadata");
class Actor {
  /**
   * Get the Agent class this Actor would call, or undefined if the Actor would use
   * the default agent (global.ic.agent).
   * @param actor The actor to get the agent of.
   */
  static agentOf(actor) {
    return actor[metadataSymbol].config.agent;
  }
  /**
   * Get the interface of an actor, in the form of an instance of a Service.
   * @param actor The actor to get the interface of.
   */
  static interfaceOf(actor) {
    return actor[metadataSymbol].service;
  }
  static canisterIdOf(actor) {
    return Principal.from(actor[metadataSymbol].config.canisterId);
  }
  static createActorClass(interfaceFactory, options) {
    const service = interfaceFactory({ IDL });
    class CanisterActor extends Actor {
      constructor(config) {
        if (!config.canisterId) {
          throw InputError.fromCode(new MissingCanisterIdErrorCode(config.canisterId));
        }
        const canisterId = typeof config.canisterId === "string" ? Principal.fromText(config.canisterId) : config.canisterId;
        super({
          config: {
            ...DEFAULT_ACTOR_CONFIG,
            ...config,
            canisterId
          },
          service
        });
        for (const [methodName, func] of service._fields) {
          if (options == null ? void 0 : options.httpDetails) {
            func.annotations.push(ACTOR_METHOD_WITH_HTTP_DETAILS);
          }
          if (options == null ? void 0 : options.certificate) {
            func.annotations.push(ACTOR_METHOD_WITH_CERTIFICATE);
          }
          this[methodName] = _createActorMethod(this, methodName, func, config.blsVerify);
        }
      }
    }
    return CanisterActor;
  }
  /**
   * Creates an actor with the given interface factory and configuration.
   *
   * The [`@icp-sdk/bindgen`](https://js.icp.build/bindgen/) package can be used to generate the interface factory for your canister.
   * @param interfaceFactory - the interface factory for the actor, typically generated by the [`@icp-sdk/bindgen`](https://js.icp.build/bindgen/) package
   * @param configuration - the configuration for the actor
   * @returns an actor with the given interface factory and configuration
   * @example
   * Using the interface factory generated by the [`@icp-sdk/bindgen`](https://js.icp.build/bindgen/) package:
   * ```ts
   * import { Actor, HttpAgent } from '@icp-sdk/core/agent';
   * import { Principal } from '@icp-sdk/core/principal';
   * import { idlFactory } from './api/declarations/hello-world.did';
   *
   * const canisterId = Principal.fromText('rrkah-fqaaa-aaaaa-aaaaq-cai');
   *
   * const agent = await HttpAgent.create({
   *   host: 'https://icp-api.io',
   * });
   *
   * const actor = Actor.createActor(idlFactory, {
   *   agent,
   *   canisterId,
   * });
   *
   * const response = await actor.greet('world');
   * console.log(response);
   * ```
   * @example
   * Using the `createActor` wrapper function generated by the [`@icp-sdk/bindgen`](https://js.icp.build/bindgen/) package:
   * ```ts
   * import { HttpAgent } from '@icp-sdk/core/agent';
   * import { Principal } from '@icp-sdk/core/principal';
   * import { createActor } from './api/hello-world';
   *
   * const canisterId = Principal.fromText('rrkah-fqaaa-aaaaa-aaaaq-cai');
   *
   * const agent = await HttpAgent.create({
   *   host: 'https://icp-api.io',
   * });
   *
   * const actor = createActor(canisterId, {
   *   agent,
   * });
   *
   * const response = await actor.greet('world');
   * console.log(response);
   * ```
   */
  static createActor(interfaceFactory, configuration) {
    if (!configuration.canisterId) {
      throw InputError.fromCode(new MissingCanisterIdErrorCode(configuration.canisterId));
    }
    return new (this.createActorClass(interfaceFactory))(configuration);
  }
  /**
   * Returns an actor with methods that return the http response details along with the result
   * @param interfaceFactory - the interface factory for the actor
   * @param configuration - the configuration for the actor
   * @deprecated - use createActor with actorClassOptions instead
   */
  static createActorWithHttpDetails(interfaceFactory, configuration) {
    return new (this.createActorClass(interfaceFactory, { httpDetails: true }))(configuration);
  }
  /**
   * Returns an actor with methods that return the http response details along with the result
   * @param interfaceFactory - the interface factory for the actor
   * @param configuration - the configuration for the actor
   * @param actorClassOptions - options for the actor class extended details to return with the result
   */
  static createActorWithExtendedDetails(interfaceFactory, configuration, actorClassOptions = {
    httpDetails: true,
    certificate: true
  }) {
    return new (this.createActorClass(interfaceFactory, actorClassOptions))(configuration);
  }
  constructor(metadata) {
    this[metadataSymbol] = Object.freeze(metadata);
  }
}
function decodeReturnValue(types, msg) {
  const returnValues = decode(types, msg);
  switch (returnValues.length) {
    case 0:
      return void 0;
    case 1:
      return returnValues[0];
    default:
      return returnValues;
  }
}
const DEFAULT_ACTOR_CONFIG = {
  pollingOptions: DEFAULT_POLLING_OPTIONS
};
const ACTOR_METHOD_WITH_HTTP_DETAILS = "http-details";
const ACTOR_METHOD_WITH_CERTIFICATE = "certificate";
function _createActorMethod(actor, methodName, func, blsVerify) {
  let caller;
  if (func.annotations.includes("query") || func.annotations.includes("composite_query")) {
    caller = async (options, ...args) => {
      var _a2, _b;
      options = {
        ...options,
        ...(_b = (_a2 = actor[metadataSymbol].config).queryTransform) == null ? void 0 : _b.call(_a2, methodName, args, {
          ...actor[metadataSymbol].config,
          ...options
        })
      };
      const agent = options.agent || actor[metadataSymbol].config.agent || new HttpAgent();
      const cid = Principal.from(options.canisterId || actor[metadataSymbol].config.canisterId);
      const arg = encode(func.argTypes, args);
      const result = await agent.query(cid, {
        methodName,
        arg,
        effectiveCanisterId: options.effectiveCanisterId
      });
      const httpDetails = {
        ...result.httpDetails,
        requestDetails: result.requestDetails
      };
      switch (result.status) {
        case QueryResponseStatus.Rejected: {
          const uncertifiedRejectErrorCode = new UncertifiedRejectErrorCode(result.requestId, result.reject_code, result.reject_message, result.error_code, result.signatures);
          uncertifiedRejectErrorCode.callContext = {
            canisterId: cid,
            methodName,
            httpDetails
          };
          throw RejectError.fromCode(uncertifiedRejectErrorCode);
        }
        case QueryResponseStatus.Replied:
          return func.annotations.includes(ACTOR_METHOD_WITH_HTTP_DETAILS) ? {
            httpDetails,
            result: decodeReturnValue(func.retTypes, result.reply.arg)
          } : decodeReturnValue(func.retTypes, result.reply.arg);
      }
    };
  } else {
    caller = async (options, ...args) => {
      var _a2, _b;
      options = {
        ...options,
        ...(_b = (_a2 = actor[metadataSymbol].config).callTransform) == null ? void 0 : _b.call(_a2, methodName, args, {
          ...actor[metadataSymbol].config,
          ...options
        })
      };
      const agent = options.agent || actor[metadataSymbol].config.agent || HttpAgent.createSync();
      const { canisterId, effectiveCanisterId, pollingOptions } = {
        ...DEFAULT_ACTOR_CONFIG,
        ...actor[metadataSymbol].config,
        ...options
      };
      const cid = Principal.from(canisterId);
      const ecid = effectiveCanisterId !== void 0 ? Principal.from(effectiveCanisterId) : cid;
      const arg = encode(func.argTypes, args);
      const { requestId, response, requestDetails } = await agent.call(cid, {
        methodName,
        arg,
        effectiveCanisterId: ecid,
        nonce: options.nonce
      });
      let reply;
      let certificate;
      if (isV3ResponseBody(response.body)) {
        if (agent.rootKey == null) {
          throw ExternalError.fromCode(new MissingRootKeyErrorCode());
        }
        const cert = response.body.certificate;
        certificate = await Certificate.create({
          certificate: cert,
          rootKey: agent.rootKey,
          canisterId: ecid,
          blsVerify,
          agent
        });
        const path = [utf8ToBytes("request_status"), requestId];
        const status = new TextDecoder().decode(lookupResultToBuffer(certificate.lookup_path([...path, "status"])));
        switch (status) {
          case "replied":
            reply = lookupResultToBuffer(certificate.lookup_path([...path, "reply"]));
            break;
          case "rejected": {
            const rejectCode = new Uint8Array(lookupResultToBuffer(certificate.lookup_path([...path, "reject_code"])))[0];
            const rejectMessage = new TextDecoder().decode(lookupResultToBuffer(certificate.lookup_path([...path, "reject_message"])));
            const error_code_buf = lookupResultToBuffer(certificate.lookup_path([...path, "error_code"]));
            const error_code = error_code_buf ? new TextDecoder().decode(error_code_buf) : void 0;
            const certifiedRejectErrorCode = new CertifiedRejectErrorCode(requestId, rejectCode, rejectMessage, error_code);
            certifiedRejectErrorCode.callContext = {
              canisterId: cid,
              methodName,
              httpDetails: response
            };
            throw RejectError.fromCode(certifiedRejectErrorCode);
          }
        }
      } else if (isV2ResponseBody(response.body)) {
        const { reject_code, reject_message, error_code } = response.body;
        const errorCode = new UncertifiedRejectUpdateErrorCode(requestId, reject_code, reject_message, error_code);
        errorCode.callContext = {
          canisterId: cid,
          methodName,
          httpDetails: response
        };
        throw RejectError.fromCode(errorCode);
      }
      if (response.status === 202) {
        const pollOptions = {
          ...pollingOptions,
          blsVerify
        };
        const response2 = await pollForResponse(agent, ecid, requestId, pollOptions);
        certificate = response2.certificate;
        reply = response2.reply;
      }
      const shouldIncludeHttpDetails = func.annotations.includes(ACTOR_METHOD_WITH_HTTP_DETAILS);
      const shouldIncludeCertificate = func.annotations.includes(ACTOR_METHOD_WITH_CERTIFICATE);
      const httpDetails = { ...response, requestDetails };
      if (reply !== void 0) {
        if (shouldIncludeHttpDetails && shouldIncludeCertificate) {
          return {
            httpDetails,
            certificate,
            result: decodeReturnValue(func.retTypes, reply)
          };
        } else if (shouldIncludeCertificate) {
          return {
            certificate,
            result: decodeReturnValue(func.retTypes, reply)
          };
        } else if (shouldIncludeHttpDetails) {
          return {
            httpDetails,
            result: decodeReturnValue(func.retTypes, reply)
          };
        }
        return decodeReturnValue(func.retTypes, reply);
      } else {
        const errorCode = new UnexpectedErrorCode(`Call was returned undefined. We cannot determine if the call was successful or not. Return types: [${func.retTypes.map((t) => t.display()).join(",")}].`);
        errorCode.callContext = {
          canisterId: cid,
          methodName,
          httpDetails
        };
        throw UnknownError.fromCode(errorCode);
      }
    };
  }
  const handler = (...args) => caller({}, ...args);
  handler.withOptions = (options) => (...args) => caller(options, ...args);
  return handler;
}
var QueryObserver = (_a = class extends Subscribable {
  constructor(client, options) {
    super();
    __privateAdd(this, _QueryObserver_instances);
    __privateAdd(this, _client);
    __privateAdd(this, _currentQuery);
    __privateAdd(this, _currentQueryInitialState);
    __privateAdd(this, _currentResult);
    __privateAdd(this, _currentResultState);
    __privateAdd(this, _currentResultOptions);
    __privateAdd(this, _currentThenable);
    __privateAdd(this, _selectError);
    __privateAdd(this, _selectFn);
    __privateAdd(this, _selectResult);
    // This property keeps track of the last query with defined data.
    // It will be used to pass the previous data and query to the placeholder function between renders.
    __privateAdd(this, _lastQueryWithDefinedData);
    __privateAdd(this, _staleTimeoutId);
    __privateAdd(this, _refetchIntervalId);
    __privateAdd(this, _currentRefetchInterval);
    __privateAdd(this, _trackedProps, /* @__PURE__ */ new Set());
    this.options = options;
    __privateSet(this, _client, client);
    __privateSet(this, _selectError, null);
    __privateSet(this, _currentThenable, pendingThenable());
    this.bindMethods();
    this.setOptions(options);
  }
  bindMethods() {
    this.refetch = this.refetch.bind(this);
  }
  onSubscribe() {
    if (this.listeners.size === 1) {
      __privateGet(this, _currentQuery).addObserver(this);
      if (shouldFetchOnMount(__privateGet(this, _currentQuery), this.options)) {
        __privateMethod(this, _QueryObserver_instances, executeFetch_fn).call(this);
      } else {
        this.updateResult();
      }
      __privateMethod(this, _QueryObserver_instances, updateTimers_fn).call(this);
    }
  }
  onUnsubscribe() {
    if (!this.hasListeners()) {
      this.destroy();
    }
  }
  shouldFetchOnReconnect() {
    return shouldFetchOn(
      __privateGet(this, _currentQuery),
      this.options,
      this.options.refetchOnReconnect
    );
  }
  shouldFetchOnWindowFocus() {
    return shouldFetchOn(
      __privateGet(this, _currentQuery),
      this.options,
      this.options.refetchOnWindowFocus
    );
  }
  destroy() {
    this.listeners = /* @__PURE__ */ new Set();
    __privateMethod(this, _QueryObserver_instances, clearStaleTimeout_fn).call(this);
    __privateMethod(this, _QueryObserver_instances, clearRefetchInterval_fn).call(this);
    __privateGet(this, _currentQuery).removeObserver(this);
  }
  setOptions(options) {
    const prevOptions = this.options;
    const prevQuery = __privateGet(this, _currentQuery);
    this.options = __privateGet(this, _client).defaultQueryOptions(options);
    if (this.options.enabled !== void 0 && typeof this.options.enabled !== "boolean" && typeof this.options.enabled !== "function" && typeof resolveEnabled(this.options.enabled, __privateGet(this, _currentQuery)) !== "boolean") {
      throw new Error(
        "Expected enabled to be a boolean or a callback that returns a boolean"
      );
    }
    __privateMethod(this, _QueryObserver_instances, updateQuery_fn).call(this);
    __privateGet(this, _currentQuery).setOptions(this.options);
    if (prevOptions._defaulted && !shallowEqualObjects(this.options, prevOptions)) {
      __privateGet(this, _client).getQueryCache().notify({
        type: "observerOptionsUpdated",
        query: __privateGet(this, _currentQuery),
        observer: this
      });
    }
    const mounted = this.hasListeners();
    if (mounted && shouldFetchOptionally(
      __privateGet(this, _currentQuery),
      prevQuery,
      this.options,
      prevOptions
    )) {
      __privateMethod(this, _QueryObserver_instances, executeFetch_fn).call(this);
    }
    this.updateResult();
    if (mounted && (__privateGet(this, _currentQuery) !== prevQuery || resolveEnabled(this.options.enabled, __privateGet(this, _currentQuery)) !== resolveEnabled(prevOptions.enabled, __privateGet(this, _currentQuery)) || resolveStaleTime(this.options.staleTime, __privateGet(this, _currentQuery)) !== resolveStaleTime(prevOptions.staleTime, __privateGet(this, _currentQuery)))) {
      __privateMethod(this, _QueryObserver_instances, updateStaleTimeout_fn).call(this);
    }
    const nextRefetchInterval = __privateMethod(this, _QueryObserver_instances, computeRefetchInterval_fn).call(this);
    if (mounted && (__privateGet(this, _currentQuery) !== prevQuery || resolveEnabled(this.options.enabled, __privateGet(this, _currentQuery)) !== resolveEnabled(prevOptions.enabled, __privateGet(this, _currentQuery)) || nextRefetchInterval !== __privateGet(this, _currentRefetchInterval))) {
      __privateMethod(this, _QueryObserver_instances, updateRefetchInterval_fn).call(this, nextRefetchInterval);
    }
  }
  getOptimisticResult(options) {
    const query = __privateGet(this, _client).getQueryCache().build(__privateGet(this, _client), options);
    const result = this.createResult(query, options);
    if (shouldAssignObserverCurrentProperties(this, result)) {
      __privateSet(this, _currentResult, result);
      __privateSet(this, _currentResultOptions, this.options);
      __privateSet(this, _currentResultState, __privateGet(this, _currentQuery).state);
    }
    return result;
  }
  getCurrentResult() {
    return __privateGet(this, _currentResult);
  }
  trackResult(result, onPropTracked) {
    return new Proxy(result, {
      get: (target, key) => {
        this.trackProp(key);
        onPropTracked == null ? void 0 : onPropTracked(key);
        if (key === "promise") {
          this.trackProp("data");
          if (!this.options.experimental_prefetchInRender && __privateGet(this, _currentThenable).status === "pending") {
            __privateGet(this, _currentThenable).reject(
              new Error(
                "experimental_prefetchInRender feature flag is not enabled"
              )
            );
          }
        }
        return Reflect.get(target, key);
      }
    });
  }
  trackProp(key) {
    __privateGet(this, _trackedProps).add(key);
  }
  getCurrentQuery() {
    return __privateGet(this, _currentQuery);
  }
  refetch({ ...options } = {}) {
    return this.fetch({
      ...options
    });
  }
  fetchOptimistic(options) {
    const defaultedOptions = __privateGet(this, _client).defaultQueryOptions(options);
    const query = __privateGet(this, _client).getQueryCache().build(__privateGet(this, _client), defaultedOptions);
    return query.fetch().then(() => this.createResult(query, defaultedOptions));
  }
  fetch(fetchOptions) {
    return __privateMethod(this, _QueryObserver_instances, executeFetch_fn).call(this, {
      ...fetchOptions,
      cancelRefetch: fetchOptions.cancelRefetch ?? true
    }).then(() => {
      this.updateResult();
      return __privateGet(this, _currentResult);
    });
  }
  createResult(query, options) {
    var _a2;
    const prevQuery = __privateGet(this, _currentQuery);
    const prevOptions = this.options;
    const prevResult = __privateGet(this, _currentResult);
    const prevResultState = __privateGet(this, _currentResultState);
    const prevResultOptions = __privateGet(this, _currentResultOptions);
    const queryChange = query !== prevQuery;
    const queryInitialState = queryChange ? query.state : __privateGet(this, _currentQueryInitialState);
    const { state } = query;
    let newState = { ...state };
    let isPlaceholderData = false;
    let data;
    if (options._optimisticResults) {
      const mounted = this.hasListeners();
      const fetchOnMount = !mounted && shouldFetchOnMount(query, options);
      const fetchOptionally = mounted && shouldFetchOptionally(query, prevQuery, options, prevOptions);
      if (fetchOnMount || fetchOptionally) {
        newState = {
          ...newState,
          ...fetchState(state.data, query.options)
        };
      }
      if (options._optimisticResults === "isRestoring") {
        newState.fetchStatus = "idle";
      }
    }
    let { error, errorUpdatedAt, status } = newState;
    data = newState.data;
    let skipSelect = false;
    if (options.placeholderData !== void 0 && data === void 0 && status === "pending") {
      let placeholderData;
      if ((prevResult == null ? void 0 : prevResult.isPlaceholderData) && options.placeholderData === (prevResultOptions == null ? void 0 : prevResultOptions.placeholderData)) {
        placeholderData = prevResult.data;
        skipSelect = true;
      } else {
        placeholderData = typeof options.placeholderData === "function" ? options.placeholderData(
          (_a2 = __privateGet(this, _lastQueryWithDefinedData)) == null ? void 0 : _a2.state.data,
          __privateGet(this, _lastQueryWithDefinedData)
        ) : options.placeholderData;
      }
      if (placeholderData !== void 0) {
        status = "success";
        data = replaceData(
          prevResult == null ? void 0 : prevResult.data,
          placeholderData,
          options
        );
        isPlaceholderData = true;
      }
    }
    if (options.select && data !== void 0 && !skipSelect) {
      if (prevResult && data === (prevResultState == null ? void 0 : prevResultState.data) && options.select === __privateGet(this, _selectFn)) {
        data = __privateGet(this, _selectResult);
      } else {
        try {
          __privateSet(this, _selectFn, options.select);
          data = options.select(data);
          data = replaceData(prevResult == null ? void 0 : prevResult.data, data, options);
          __privateSet(this, _selectResult, data);
          __privateSet(this, _selectError, null);
        } catch (selectError) {
          __privateSet(this, _selectError, selectError);
        }
      }
    }
    if (__privateGet(this, _selectError)) {
      error = __privateGet(this, _selectError);
      data = __privateGet(this, _selectResult);
      errorUpdatedAt = Date.now();
      status = "error";
    }
    const isFetching = newState.fetchStatus === "fetching";
    const isPending = status === "pending";
    const isError = status === "error";
    const isLoading = isPending && isFetching;
    const hasData = data !== void 0;
    const result = {
      status,
      fetchStatus: newState.fetchStatus,
      isPending,
      isSuccess: status === "success",
      isError,
      isInitialLoading: isLoading,
      isLoading,
      data,
      dataUpdatedAt: newState.dataUpdatedAt,
      error,
      errorUpdatedAt,
      failureCount: newState.fetchFailureCount,
      failureReason: newState.fetchFailureReason,
      errorUpdateCount: newState.errorUpdateCount,
      isFetched: query.isFetched(),
      isFetchedAfterMount: newState.dataUpdateCount > queryInitialState.dataUpdateCount || newState.errorUpdateCount > queryInitialState.errorUpdateCount,
      isFetching,
      isRefetching: isFetching && !isPending,
      isLoadingError: isError && !hasData,
      isPaused: newState.fetchStatus === "paused",
      isPlaceholderData,
      isRefetchError: isError && hasData,
      isStale: isStale(query, options),
      refetch: this.refetch,
      promise: __privateGet(this, _currentThenable),
      isEnabled: resolveEnabled(options.enabled, query) !== false
    };
    const nextResult = result;
    if (this.options.experimental_prefetchInRender) {
      const hasResultData = nextResult.data !== void 0;
      const isErrorWithoutData = nextResult.status === "error" && !hasResultData;
      const finalizeThenableIfPossible = (thenable) => {
        if (isErrorWithoutData) {
          thenable.reject(nextResult.error);
        } else if (hasResultData) {
          thenable.resolve(nextResult.data);
        }
      };
      const recreateThenable = () => {
        const pending = __privateSet(this, _currentThenable, nextResult.promise = pendingThenable());
        finalizeThenableIfPossible(pending);
      };
      const prevThenable = __privateGet(this, _currentThenable);
      switch (prevThenable.status) {
        case "pending":
          if (query.queryHash === prevQuery.queryHash) {
            finalizeThenableIfPossible(prevThenable);
          }
          break;
        case "fulfilled":
          if (isErrorWithoutData || nextResult.data !== prevThenable.value) {
            recreateThenable();
          }
          break;
        case "rejected":
          if (!isErrorWithoutData || nextResult.error !== prevThenable.reason) {
            recreateThenable();
          }
          break;
      }
    }
    return nextResult;
  }
  updateResult() {
    const prevResult = __privateGet(this, _currentResult);
    const nextResult = this.createResult(__privateGet(this, _currentQuery), this.options);
    __privateSet(this, _currentResultState, __privateGet(this, _currentQuery).state);
    __privateSet(this, _currentResultOptions, this.options);
    if (__privateGet(this, _currentResultState).data !== void 0) {
      __privateSet(this, _lastQueryWithDefinedData, __privateGet(this, _currentQuery));
    }
    if (shallowEqualObjects(nextResult, prevResult)) {
      return;
    }
    __privateSet(this, _currentResult, nextResult);
    const shouldNotifyListeners = () => {
      if (!prevResult) {
        return true;
      }
      const { notifyOnChangeProps } = this.options;
      const notifyOnChangePropsValue = typeof notifyOnChangeProps === "function" ? notifyOnChangeProps() : notifyOnChangeProps;
      if (notifyOnChangePropsValue === "all" || !notifyOnChangePropsValue && !__privateGet(this, _trackedProps).size) {
        return true;
      }
      const includedProps = new Set(
        notifyOnChangePropsValue ?? __privateGet(this, _trackedProps)
      );
      if (this.options.throwOnError) {
        includedProps.add("error");
      }
      return Object.keys(__privateGet(this, _currentResult)).some((key) => {
        const typedKey = key;
        const changed = __privateGet(this, _currentResult)[typedKey] !== prevResult[typedKey];
        return changed && includedProps.has(typedKey);
      });
    };
    __privateMethod(this, _QueryObserver_instances, notify_fn).call(this, { listeners: shouldNotifyListeners() });
  }
  onQueryUpdate() {
    this.updateResult();
    if (this.hasListeners()) {
      __privateMethod(this, _QueryObserver_instances, updateTimers_fn).call(this);
    }
  }
}, _client = new WeakMap(), _currentQuery = new WeakMap(), _currentQueryInitialState = new WeakMap(), _currentResult = new WeakMap(), _currentResultState = new WeakMap(), _currentResultOptions = new WeakMap(), _currentThenable = new WeakMap(), _selectError = new WeakMap(), _selectFn = new WeakMap(), _selectResult = new WeakMap(), _lastQueryWithDefinedData = new WeakMap(), _staleTimeoutId = new WeakMap(), _refetchIntervalId = new WeakMap(), _currentRefetchInterval = new WeakMap(), _trackedProps = new WeakMap(), _QueryObserver_instances = new WeakSet(), executeFetch_fn = function(fetchOptions) {
  __privateMethod(this, _QueryObserver_instances, updateQuery_fn).call(this);
  let promise = __privateGet(this, _currentQuery).fetch(
    this.options,
    fetchOptions
  );
  if (!(fetchOptions == null ? void 0 : fetchOptions.throwOnError)) {
    promise = promise.catch(noop);
  }
  return promise;
}, updateStaleTimeout_fn = function() {
  __privateMethod(this, _QueryObserver_instances, clearStaleTimeout_fn).call(this);
  const staleTime = resolveStaleTime(
    this.options.staleTime,
    __privateGet(this, _currentQuery)
  );
  if (environmentManager.isServer() || __privateGet(this, _currentResult).isStale || !isValidTimeout(staleTime)) {
    return;
  }
  const time = timeUntilStale(__privateGet(this, _currentResult).dataUpdatedAt, staleTime);
  const timeout2 = time + 1;
  __privateSet(this, _staleTimeoutId, timeoutManager.setTimeout(() => {
    if (!__privateGet(this, _currentResult).isStale) {
      this.updateResult();
    }
  }, timeout2));
}, computeRefetchInterval_fn = function() {
  return (typeof this.options.refetchInterval === "function" ? this.options.refetchInterval(__privateGet(this, _currentQuery)) : this.options.refetchInterval) ?? false;
}, updateRefetchInterval_fn = function(nextInterval) {
  __privateMethod(this, _QueryObserver_instances, clearRefetchInterval_fn).call(this);
  __privateSet(this, _currentRefetchInterval, nextInterval);
  if (environmentManager.isServer() || resolveEnabled(this.options.enabled, __privateGet(this, _currentQuery)) === false || !isValidTimeout(__privateGet(this, _currentRefetchInterval)) || __privateGet(this, _currentRefetchInterval) === 0) {
    return;
  }
  __privateSet(this, _refetchIntervalId, timeoutManager.setInterval(() => {
    if (this.options.refetchIntervalInBackground || focusManager.isFocused()) {
      __privateMethod(this, _QueryObserver_instances, executeFetch_fn).call(this);
    }
  }, __privateGet(this, _currentRefetchInterval)));
}, updateTimers_fn = function() {
  __privateMethod(this, _QueryObserver_instances, updateStaleTimeout_fn).call(this);
  __privateMethod(this, _QueryObserver_instances, updateRefetchInterval_fn).call(this, __privateMethod(this, _QueryObserver_instances, computeRefetchInterval_fn).call(this));
}, clearStaleTimeout_fn = function() {
  if (__privateGet(this, _staleTimeoutId)) {
    timeoutManager.clearTimeout(__privateGet(this, _staleTimeoutId));
    __privateSet(this, _staleTimeoutId, void 0);
  }
}, clearRefetchInterval_fn = function() {
  if (__privateGet(this, _refetchIntervalId)) {
    timeoutManager.clearInterval(__privateGet(this, _refetchIntervalId));
    __privateSet(this, _refetchIntervalId, void 0);
  }
}, updateQuery_fn = function() {
  const query = __privateGet(this, _client).getQueryCache().build(__privateGet(this, _client), this.options);
  if (query === __privateGet(this, _currentQuery)) {
    return;
  }
  const prevQuery = __privateGet(this, _currentQuery);
  __privateSet(this, _currentQuery, query);
  __privateSet(this, _currentQueryInitialState, query.state);
  if (this.hasListeners()) {
    prevQuery == null ? void 0 : prevQuery.removeObserver(this);
    query.addObserver(this);
  }
}, notify_fn = function(notifyOptions) {
  notifyManager.batch(() => {
    if (notifyOptions.listeners) {
      this.listeners.forEach((listener) => {
        listener(__privateGet(this, _currentResult));
      });
    }
    __privateGet(this, _client).getQueryCache().notify({
      query: __privateGet(this, _currentQuery),
      type: "observerResultsUpdated"
    });
  });
}, _a);
function shouldLoadOnMount(query, options) {
  return resolveEnabled(options.enabled, query) !== false && query.state.data === void 0 && !(query.state.status === "error" && options.retryOnMount === false);
}
function shouldFetchOnMount(query, options) {
  return shouldLoadOnMount(query, options) || query.state.data !== void 0 && shouldFetchOn(query, options, options.refetchOnMount);
}
function shouldFetchOn(query, options, field) {
  if (resolveEnabled(options.enabled, query) !== false && resolveStaleTime(options.staleTime, query) !== "static") {
    const value = typeof field === "function" ? field(query) : field;
    return value === "always" || value !== false && isStale(query, options);
  }
  return false;
}
function shouldFetchOptionally(query, prevQuery, options, prevOptions) {
  return (query !== prevQuery || resolveEnabled(prevOptions.enabled, query) === false) && (!options.suspense || query.state.status !== "error") && isStale(query, options);
}
function isStale(query, options) {
  return resolveEnabled(options.enabled, query) !== false && query.isStaleByTime(resolveStaleTime(options.staleTime, query));
}
function shouldAssignObserverCurrentProperties(observer, optimisticResult) {
  if (!shallowEqualObjects(observer.getCurrentResult(), optimisticResult)) {
    return true;
  }
  return false;
}
var InfiniteQueryObserver = class extends QueryObserver {
  constructor(client, options) {
    super(client, options);
  }
  bindMethods() {
    super.bindMethods();
    this.fetchNextPage = this.fetchNextPage.bind(this);
    this.fetchPreviousPage = this.fetchPreviousPage.bind(this);
  }
  setOptions(options) {
    super.setOptions({
      ...options,
      behavior: infiniteQueryBehavior()
    });
  }
  getOptimisticResult(options) {
    options.behavior = infiniteQueryBehavior();
    return super.getOptimisticResult(options);
  }
  fetchNextPage(options) {
    return this.fetch({
      ...options,
      meta: {
        fetchMore: { direction: "forward" }
      }
    });
  }
  fetchPreviousPage(options) {
    return this.fetch({
      ...options,
      meta: {
        fetchMore: { direction: "backward" }
      }
    });
  }
  createResult(query, options) {
    var _a2, _b;
    const { state } = query;
    const parentResult = super.createResult(query, options);
    const { isFetching, isRefetching, isError, isRefetchError } = parentResult;
    const fetchDirection = (_b = (_a2 = state.fetchMeta) == null ? void 0 : _a2.fetchMore) == null ? void 0 : _b.direction;
    const isFetchNextPageError = isError && fetchDirection === "forward";
    const isFetchingNextPage = isFetching && fetchDirection === "forward";
    const isFetchPreviousPageError = isError && fetchDirection === "backward";
    const isFetchingPreviousPage = isFetching && fetchDirection === "backward";
    const result = {
      ...parentResult,
      fetchNextPage: this.fetchNextPage,
      fetchPreviousPage: this.fetchPreviousPage,
      hasNextPage: hasNextPage(options, state.data),
      hasPreviousPage: hasPreviousPage(options, state.data),
      isFetchNextPageError,
      isFetchingNextPage,
      isFetchPreviousPageError,
      isFetchingPreviousPage,
      isRefetchError: isRefetchError && !isFetchNextPageError && !isFetchPreviousPageError,
      isRefetching: isRefetching && !isFetchingNextPage && !isFetchingPreviousPage
    };
    return result;
  }
};
var IsRestoringContext = reactExports.createContext(false);
var useIsRestoring = () => reactExports.useContext(IsRestoringContext);
IsRestoringContext.Provider;
function createValue() {
  let isReset = false;
  return {
    clearReset: () => {
      isReset = false;
    },
    reset: () => {
      isReset = true;
    },
    isReset: () => {
      return isReset;
    }
  };
}
var QueryErrorResetBoundaryContext = reactExports.createContext(createValue());
var useQueryErrorResetBoundary = () => reactExports.useContext(QueryErrorResetBoundaryContext);
var ensurePreventErrorBoundaryRetry = (options, errorResetBoundary, query) => {
  const throwOnError = (query == null ? void 0 : query.state.error) && typeof options.throwOnError === "function" ? shouldThrowError(options.throwOnError, [query.state.error, query]) : options.throwOnError;
  if (options.suspense || options.experimental_prefetchInRender || throwOnError) {
    if (!errorResetBoundary.isReset()) {
      options.retryOnMount = false;
    }
  }
};
var useClearResetErrorBoundary = (errorResetBoundary) => {
  reactExports.useEffect(() => {
    errorResetBoundary.clearReset();
  }, [errorResetBoundary]);
};
var getHasError = ({
  result,
  errorResetBoundary,
  throwOnError,
  query,
  suspense
}) => {
  return result.isError && !errorResetBoundary.isReset() && !result.isFetching && query && (suspense && result.data === void 0 || shouldThrowError(throwOnError, [result.error, query]));
};
var ensureSuspenseTimers = (defaultedOptions) => {
  if (defaultedOptions.suspense) {
    const MIN_SUSPENSE_TIME_MS = 1e3;
    const clamp = (value) => value === "static" ? value : Math.max(value ?? MIN_SUSPENSE_TIME_MS, MIN_SUSPENSE_TIME_MS);
    const originalStaleTime = defaultedOptions.staleTime;
    defaultedOptions.staleTime = typeof originalStaleTime === "function" ? (...args) => clamp(originalStaleTime(...args)) : clamp(originalStaleTime);
    if (typeof defaultedOptions.gcTime === "number") {
      defaultedOptions.gcTime = Math.max(
        defaultedOptions.gcTime,
        MIN_SUSPENSE_TIME_MS
      );
    }
  }
};
var willFetch = (result, isRestoring) => result.isLoading && result.isFetching && !isRestoring;
var shouldSuspend = (defaultedOptions, result) => (defaultedOptions == null ? void 0 : defaultedOptions.suspense) && result.isPending;
var fetchOptimistic = (defaultedOptions, observer, errorResetBoundary) => observer.fetchOptimistic(defaultedOptions).catch(() => {
  errorResetBoundary.clearReset();
});
function useBaseQuery(options, Observer, queryClient) {
  var _a2, _b, _c, _d;
  const isRestoring = useIsRestoring();
  const errorResetBoundary = useQueryErrorResetBoundary();
  const client = useQueryClient();
  const defaultedOptions = client.defaultQueryOptions(options);
  (_b = (_a2 = client.getDefaultOptions().queries) == null ? void 0 : _a2._experimental_beforeQuery) == null ? void 0 : _b.call(
    _a2,
    defaultedOptions
  );
  const query = client.getQueryCache().get(defaultedOptions.queryHash);
  defaultedOptions._optimisticResults = isRestoring ? "isRestoring" : "optimistic";
  ensureSuspenseTimers(defaultedOptions);
  ensurePreventErrorBoundaryRetry(defaultedOptions, errorResetBoundary, query);
  useClearResetErrorBoundary(errorResetBoundary);
  const isNewCacheEntry = !client.getQueryCache().get(defaultedOptions.queryHash);
  const [observer] = reactExports.useState(
    () => new Observer(
      client,
      defaultedOptions
    )
  );
  const result = observer.getOptimisticResult(defaultedOptions);
  const shouldSubscribe = !isRestoring && options.subscribed !== false;
  reactExports.useSyncExternalStore(
    reactExports.useCallback(
      (onStoreChange) => {
        const unsubscribe = shouldSubscribe ? observer.subscribe(notifyManager.batchCalls(onStoreChange)) : noop;
        observer.updateResult();
        return unsubscribe;
      },
      [observer, shouldSubscribe]
    ),
    () => observer.getCurrentResult(),
    () => observer.getCurrentResult()
  );
  reactExports.useEffect(() => {
    observer.setOptions(defaultedOptions);
  }, [defaultedOptions, observer]);
  if (shouldSuspend(defaultedOptions, result)) {
    throw fetchOptimistic(defaultedOptions, observer, errorResetBoundary);
  }
  if (getHasError({
    result,
    errorResetBoundary,
    throwOnError: defaultedOptions.throwOnError,
    query,
    suspense: defaultedOptions.suspense
  })) {
    throw result.error;
  }
  (_d = (_c = client.getDefaultOptions().queries) == null ? void 0 : _c._experimental_afterQuery) == null ? void 0 : _d.call(
    _c,
    defaultedOptions,
    result
  );
  if (defaultedOptions.experimental_prefetchInRender && !environmentManager.isServer() && willFetch(result, isRestoring)) {
    const promise = isNewCacheEntry ? (
      // Fetch immediately on render in order to ensure `.promise` is resolved even if the component is unmounted
      fetchOptimistic(defaultedOptions, observer, errorResetBoundary)
    ) : (
      // subscribe to the "cache promise" so that we can finalize the currentThenable once data comes in
      query == null ? void 0 : query.promise
    );
    promise == null ? void 0 : promise.catch(noop).finally(() => {
      observer.updateResult();
    });
  }
  return !defaultedOptions.notifyOnChangeProps ? observer.trackResult(result) : result;
}
function useQuery(options, queryClient) {
  return useBaseQuery(options, QueryObserver);
}
function useInfiniteQuery(options, queryClient) {
  return useBaseQuery(
    options,
    InfiniteQueryObserver
  );
}
function hasAccessControl(actor) {
  return typeof actor === "object" && actor !== null && "_initializeAccessControl" in actor;
}
const ACTOR_QUERY_KEY = "actor";
function useActor(createActor2) {
  const { identity, isAuthenticated } = useInternetIdentity();
  const queryClient = useQueryClient();
  const actorQuery = useQuery({
    queryKey: [ACTOR_QUERY_KEY, identity == null ? void 0 : identity.getPrincipal().toString()],
    queryFn: async () => {
      if (!isAuthenticated) {
        return await createActorWithConfig(createActor2);
      }
      const actorOptions = {
        agentOptions: {
          identity
        }
      };
      const actor = await createActorWithConfig(createActor2, actorOptions);
      if (hasAccessControl(actor)) {
        await actor._initializeAccessControl();
      }
      return actor;
    },
    // Only refetch when identity changes
    staleTime: Number.POSITIVE_INFINITY,
    // This will cause the actor to be recreated when the identity changes
    enabled: true
  });
  reactExports.useEffect(() => {
    if (actorQuery.data) {
      queryClient.invalidateQueries({
        predicate: (query) => {
          return !query.queryKey.includes(ACTOR_QUERY_KEY);
        }
      });
      queryClient.refetchQueries({
        predicate: (query) => {
          return !query.queryKey.includes(ACTOR_QUERY_KEY);
        }
      });
    }
  }, [actorQuery.data, queryClient]);
  return {
    actor: actorQuery.data || null,
    isFetching: actorQuery.isFetching
  };
}
const ChartKind = Variant({
  "line": Null,
  "candle": Null
});
const LinePoint = Record({
  "timestamp": Int,
  "price": Float64
});
const Candle = Record({
  "low": Float64,
  "high": Float64,
  "close": Float64,
  "open": Float64,
  "timestamp": Int
});
const ChartData = Record({
  "days": Nat,
  "kind": ChartKind,
  "line": Vec(LinePoint),
  "candles": Vec(Candle),
  "updatedAt": Int,
  "coinId": Text
});
const ApiResult_2 = Variant({ "ok": ChartData, "err": Text });
const GlobalStats = Record({
  "btcDominance": Float64,
  "totalMarketCap": Float64,
  "activeCryptocurrencies": Nat,
  "marketCapChangePercentage24h": Float64,
  "totalVolume24h": Float64,
  "markets": Nat,
  "ethDominance": Float64
});
const GlobalResponse = Record({
  "updatedAt": Int,
  "stats": GlobalStats
});
const ApiResult_1 = Variant({
  "ok": GlobalResponse,
  "err": Text
});
const SparklineData = Vec(Float64);
const Coin = Record({
  "id": Text,
  "ath": Float64,
  "athChangePercentage": Float64,
  "currentPrice": Float64,
  "totalVolume": Float64,
  "circulatingSupply": Float64,
  "marketCap": Float64,
  "name": Text,
  "priceChangePercentage24h": Float64,
  "priceChangePercentage1h": Float64,
  "priceChangePercentage7d": Float64,
  "low24h": Float64,
  "totalSupply": Float64,
  "high24h": Float64,
  "sparkline7d": SparklineData,
  "image": Text,
  "marketCapRank": Nat,
  "symbol": Text
});
const MarketResponse = Record({
  "page": Nat,
  "coins": Vec(Coin),
  "perPage": Nat,
  "updatedAt": Int
});
const ApiResult = Variant({
  "ok": MarketResponse,
  "err": Text
});
const http_header = Record({
  "value": Text,
  "name": Text
});
const http_request_result = Record({
  "status": Nat,
  "body": Vec(Nat8),
  "headers": Vec(http_header)
});
const TransformationInput = Record({
  "context": Vec(Nat8),
  "response": http_request_result
});
const TransformationOutput = Record({
  "status": Nat,
  "body": Vec(Nat8),
  "headers": Vec(http_header)
});
Service({
  "getCoinChart": Func([Text, Nat, ChartKind], [ApiResult_2], []),
  "getGlobalStats": Func([], [ApiResult_1], []),
  "getMarketData": Func([], [ApiResult], []),
  "getMarketDataPage": Func([Nat, Nat], [ApiResult], []),
  "transform": Func(
    [TransformationInput],
    [TransformationOutput],
    ["query"]
  )
});
const idlFactory = ({ IDL: IDL2 }) => {
  const ChartKind2 = IDL2.Variant({ "line": IDL2.Null, "candle": IDL2.Null });
  const LinePoint2 = IDL2.Record({
    "timestamp": IDL2.Int,
    "price": IDL2.Float64
  });
  const Candle2 = IDL2.Record({
    "low": IDL2.Float64,
    "high": IDL2.Float64,
    "close": IDL2.Float64,
    "open": IDL2.Float64,
    "timestamp": IDL2.Int
  });
  const ChartData2 = IDL2.Record({
    "days": IDL2.Nat,
    "kind": ChartKind2,
    "line": IDL2.Vec(LinePoint2),
    "candles": IDL2.Vec(Candle2),
    "updatedAt": IDL2.Int,
    "coinId": IDL2.Text
  });
  const ApiResult_22 = IDL2.Variant({ "ok": ChartData2, "err": IDL2.Text });
  const GlobalStats2 = IDL2.Record({
    "btcDominance": IDL2.Float64,
    "totalMarketCap": IDL2.Float64,
    "activeCryptocurrencies": IDL2.Nat,
    "marketCapChangePercentage24h": IDL2.Float64,
    "totalVolume24h": IDL2.Float64,
    "markets": IDL2.Nat,
    "ethDominance": IDL2.Float64
  });
  const GlobalResponse2 = IDL2.Record({
    "updatedAt": IDL2.Int,
    "stats": GlobalStats2
  });
  const ApiResult_12 = IDL2.Variant({ "ok": GlobalResponse2, "err": IDL2.Text });
  const SparklineData2 = IDL2.Vec(IDL2.Float64);
  const Coin2 = IDL2.Record({
    "id": IDL2.Text,
    "ath": IDL2.Float64,
    "athChangePercentage": IDL2.Float64,
    "currentPrice": IDL2.Float64,
    "totalVolume": IDL2.Float64,
    "circulatingSupply": IDL2.Float64,
    "marketCap": IDL2.Float64,
    "name": IDL2.Text,
    "priceChangePercentage24h": IDL2.Float64,
    "priceChangePercentage1h": IDL2.Float64,
    "priceChangePercentage7d": IDL2.Float64,
    "low24h": IDL2.Float64,
    "totalSupply": IDL2.Float64,
    "high24h": IDL2.Float64,
    "sparkline7d": SparklineData2,
    "image": IDL2.Text,
    "marketCapRank": IDL2.Nat,
    "symbol": IDL2.Text
  });
  const MarketResponse2 = IDL2.Record({
    "page": IDL2.Nat,
    "coins": IDL2.Vec(Coin2),
    "perPage": IDL2.Nat,
    "updatedAt": IDL2.Int
  });
  const ApiResult2 = IDL2.Variant({ "ok": MarketResponse2, "err": IDL2.Text });
  const http_header2 = IDL2.Record({ "value": IDL2.Text, "name": IDL2.Text });
  const http_request_result2 = IDL2.Record({
    "status": IDL2.Nat,
    "body": IDL2.Vec(IDL2.Nat8),
    "headers": IDL2.Vec(http_header2)
  });
  const TransformationInput2 = IDL2.Record({
    "context": IDL2.Vec(IDL2.Nat8),
    "response": http_request_result2
  });
  const TransformationOutput2 = IDL2.Record({
    "status": IDL2.Nat,
    "body": IDL2.Vec(IDL2.Nat8),
    "headers": IDL2.Vec(http_header2)
  });
  return IDL2.Service({
    "getCoinChart": IDL2.Func(
      [IDL2.Text, IDL2.Nat, ChartKind2],
      [ApiResult_22],
      []
    ),
    "getGlobalStats": IDL2.Func([], [ApiResult_12], []),
    "getMarketData": IDL2.Func([], [ApiResult2], []),
    "getMarketDataPage": IDL2.Func([IDL2.Nat, IDL2.Nat], [ApiResult2], []),
    "transform": IDL2.Func(
      [TransformationInput2],
      [TransformationOutput2],
      ["query"]
    )
  });
};
class Backend {
  constructor(actor, _uploadFile, _downloadFile, processError) {
    this.actor = actor;
    this._uploadFile = _uploadFile;
    this._downloadFile = _downloadFile;
    this.processError = processError;
  }
  async getCoinChart(arg0, arg1, arg2) {
    if (this.processError) {
      try {
        const result = await this.actor.getCoinChart(arg0, arg1, to_candid_ChartKind_n1(this._uploadFile, this._downloadFile, arg2));
        return from_candid_ApiResult_2_n3(this._uploadFile, this._downloadFile, result);
      } catch (e2) {
        this.processError(e2);
        throw new Error("unreachable");
      }
    } else {
      const result = await this.actor.getCoinChart(arg0, arg1, to_candid_ChartKind_n1(this._uploadFile, this._downloadFile, arg2));
      return from_candid_ApiResult_2_n3(this._uploadFile, this._downloadFile, result);
    }
  }
  async getGlobalStats() {
    if (this.processError) {
      try {
        const result = await this.actor.getGlobalStats();
        return from_candid_ApiResult_1_n9(this._uploadFile, this._downloadFile, result);
      } catch (e2) {
        this.processError(e2);
        throw new Error("unreachable");
      }
    } else {
      const result = await this.actor.getGlobalStats();
      return from_candid_ApiResult_1_n9(this._uploadFile, this._downloadFile, result);
    }
  }
  async getMarketData() {
    if (this.processError) {
      try {
        const result = await this.actor.getMarketData();
        return from_candid_ApiResult_n11(this._uploadFile, this._downloadFile, result);
      } catch (e2) {
        this.processError(e2);
        throw new Error("unreachable");
      }
    } else {
      const result = await this.actor.getMarketData();
      return from_candid_ApiResult_n11(this._uploadFile, this._downloadFile, result);
    }
  }
  async getMarketDataPage(arg0, arg1) {
    if (this.processError) {
      try {
        const result = await this.actor.getMarketDataPage(arg0, arg1);
        return from_candid_ApiResult_n11(this._uploadFile, this._downloadFile, result);
      } catch (e2) {
        this.processError(e2);
        throw new Error("unreachable");
      }
    } else {
      const result = await this.actor.getMarketDataPage(arg0, arg1);
      return from_candid_ApiResult_n11(this._uploadFile, this._downloadFile, result);
    }
  }
  async transform(arg0) {
    if (this.processError) {
      try {
        const result = await this.actor.transform(arg0);
        return result;
      } catch (e2) {
        this.processError(e2);
        throw new Error("unreachable");
      }
    } else {
      const result = await this.actor.transform(arg0);
      return result;
    }
  }
}
function from_candid_ApiResult_1_n9(_uploadFile, _downloadFile, value) {
  return from_candid_variant_n10(_uploadFile, _downloadFile, value);
}
function from_candid_ApiResult_2_n3(_uploadFile, _downloadFile, value) {
  return from_candid_variant_n4(_uploadFile, _downloadFile, value);
}
function from_candid_ApiResult_n11(_uploadFile, _downloadFile, value) {
  return from_candid_variant_n12(_uploadFile, _downloadFile, value);
}
function from_candid_ChartData_n5(_uploadFile, _downloadFile, value) {
  return from_candid_record_n6(_uploadFile, _downloadFile, value);
}
function from_candid_ChartKind_n7(_uploadFile, _downloadFile, value) {
  return from_candid_variant_n8(_uploadFile, _downloadFile, value);
}
function from_candid_record_n6(_uploadFile, _downloadFile, value) {
  return {
    days: value.days,
    kind: from_candid_ChartKind_n7(_uploadFile, _downloadFile, value.kind),
    line: value.line,
    candles: value.candles,
    updatedAt: value.updatedAt,
    coinId: value.coinId
  };
}
function from_candid_variant_n10(_uploadFile, _downloadFile, value) {
  return "ok" in value ? {
    __kind__: "ok",
    ok: value.ok
  } : "err" in value ? {
    __kind__: "err",
    err: value.err
  } : value;
}
function from_candid_variant_n12(_uploadFile, _downloadFile, value) {
  return "ok" in value ? {
    __kind__: "ok",
    ok: value.ok
  } : "err" in value ? {
    __kind__: "err",
    err: value.err
  } : value;
}
function from_candid_variant_n4(_uploadFile, _downloadFile, value) {
  return "ok" in value ? {
    __kind__: "ok",
    ok: from_candid_ChartData_n5(_uploadFile, _downloadFile, value.ok)
  } : "err" in value ? {
    __kind__: "err",
    err: value.err
  } : value;
}
function from_candid_variant_n8(_uploadFile, _downloadFile, value) {
  return "line" in value ? "line" : "candle" in value ? "candle" : value;
}
function to_candid_ChartKind_n1(_uploadFile, _downloadFile, value) {
  return to_candid_variant_n2(_uploadFile, _downloadFile, value);
}
function to_candid_variant_n2(_uploadFile, _downloadFile, value) {
  return value == "line" ? {
    line: null
  } : value == "candle" ? {
    candle: null
  } : value;
}
function createActor(canisterId, _uploadFile, _downloadFile, options = {}) {
  const agent = options.agent || HttpAgent.createSync({
    ...options.agentOptions
  });
  if (options.agent && options.agentOptions) {
    console.warn("Detected both agent and agentOptions passed to createActor. Ignoring agentOptions and proceeding with the provided agent.");
  }
  const actor = Actor.createActor(idlFactory, {
    agent,
    canisterId,
    ...options.actorOptions
  });
  return new Backend(actor, _uploadFile, _downloadFile, options.processError);
}
const ChartKindLine = { line: null };
const ChartKindCandle = { candle: null };
function isLineKind(k2) {
  if (k2 === null || typeof k2 !== "object") return false;
  return k2.line === null;
}
const COINGECKO_API = "https://api.coingecko.com/api/v3";
function mapCoinGeckoCoin(c2) {
  var _a2, _b;
  return {
    id: c2.id,
    symbol: c2.symbol.toUpperCase(),
    name: c2.name,
    image: c2.image,
    currentPrice: c2.current_price,
    marketCap: c2.market_cap,
    marketCapRank: c2.market_cap_rank,
    priceChangePercentage1h: c2.price_change_percentage_1h_in_currency ?? 0,
    priceChangePercentage24h: c2.price_change_percentage_24h ?? 0,
    priceChangePercentage7d: c2.price_change_percentage_7d_in_currency ?? 0,
    totalVolume: c2.total_volume ?? 0,
    high24h: c2.high_24h ?? 0,
    low24h: c2.low_24h ?? 0,
    circulatingSupply: c2.circulating_supply ?? 0,
    totalSupply: c2.total_supply ?? 0,
    ath: c2.ath ?? 0,
    athChangePercentage: c2.ath_change_percentage ?? 0,
    sparkline7d: ((_b = (_a2 = c2.sparkline_in_7d) == null ? void 0 : _a2.price) == null ? void 0 : _b.slice(-48)) ?? []
  };
}
async function fetchCoinGeckoMarket(page = 1, perPage = 100) {
  const url = new URL(`${COINGECKO_API}/coins/markets`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("order", "market_cap_desc");
  url.searchParams.set("per_page", String(perPage));
  url.searchParams.set("page", String(page));
  url.searchParams.set("sparkline", "true");
  url.searchParams.set("price_change_percentage", "1h,24h,7d");
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko error: ${res.status}`);
  const data = await res.json();
  return data.map(mapCoinGeckoCoin);
}
async function fetchCoinGeckoGlobal() {
  const res = await fetch(`${COINGECKO_API}/global`, { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko global error: ${res.status}`);
  const json = await res.json();
  const d2 = json.data;
  return {
    totalMarketCap: d2.total_market_cap.eur ?? 0,
    totalVolume24h: d2.total_volume.eur ?? 0,
    marketCapChangePercentage24h: d2.market_cap_change_percentage_24h_usd ?? 0,
    btcDominance: d2.market_cap_percentage.btc ?? 0,
    ethDominance: d2.market_cap_percentage.eth ?? 0,
    activeCryptocurrencies: d2.active_cryptocurrencies ?? 0,
    markets: d2.markets ?? 0
  };
}
async function fetchCoinGeckoChartLine(coinId, days) {
  const url = new URL(`${COINGECKO_API}/coins/${coinId}/market_chart`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("days", days === 0 ? "max" : String(days));
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko chart error: ${res.status}`);
  const json = await res.json();
  return json.prices.map(([t, p2]) => ({ timestamp: t, price: p2 }));
}
async function fetchCoinGeckoChartCandles(coinId, days) {
  const url = new URL(`${COINGECKO_API}/coins/${coinId}/ohlc`);
  url.searchParams.set("vs_currency", "eur");
  url.searchParams.set("days", days === 0 ? "max" : String(days));
  const res = await fetch(url.toString(), { headers: { Accept: "application/json" } });
  if (!res.ok) throw new Error(`CoinGecko OHLC error: ${res.status}`);
  const json = await res.json();
  return json.map(([t, o2, h2, l2, c2]) => ({ timestamp: t, open: o2, high: h2, low: l2, close: c2 }));
}
const PER_PAGE = 100;
const MAX_PAGES = 10;
const TOTAL_COINS_TARGET = MAX_PAGES * PER_PAGE;
function mapBackendCoin(c2) {
  return {
    id: c2.id,
    symbol: c2.symbol.toUpperCase(),
    name: c2.name,
    image: c2.image,
    currentPrice: c2.currentPrice,
    marketCap: c2.marketCap,
    marketCapRank: Number(c2.marketCapRank),
    priceChangePercentage1h: c2.priceChangePercentage1h,
    priceChangePercentage24h: c2.priceChangePercentage24h,
    priceChangePercentage7d: c2.priceChangePercentage7d,
    totalVolume: c2.totalVolume,
    high24h: c2.high24h,
    low24h: c2.low24h,
    circulatingSupply: c2.circulatingSupply,
    totalSupply: c2.totalSupply,
    ath: c2.ath,
    athChangePercentage: c2.athChangePercentage,
    sparkline7d: c2.sparkline7d
  };
}
function useMarketDataInfinite() {
  const { actor, isFetching } = useActor(createActor);
  return useInfiniteQuery({
    queryKey: ["marketData", "infinite"],
    initialPageParam: 1,
    getNextPageParam: (last, allPages) => {
      if (last.coins.length < PER_PAGE) return void 0;
      if (allPages.length >= MAX_PAGES) return void 0;
      return last.page + 1;
    },
    queryFn: async ({ pageParam }) => {
      const page = pageParam;
      if (actor && !isFetching) {
        try {
          const result = await actor.getMarketDataPage(BigInt(page), BigInt(PER_PAGE));
          if (result.__kind__ === "ok") {
            return { coins: result.ok.coins.map(mapBackendCoin), page };
          }
        } catch (_2) {
        }
      }
      return { coins: await fetchCoinGeckoMarket(page, PER_PAGE), page };
    },
    staleTime: 6e4,
    refetchInterval: 6e4
  });
}
function useGlobalStats() {
  const { actor, isFetching } = useActor(createActor);
  return useQuery({
    queryKey: ["globalStats"],
    queryFn: async () => {
      if (actor && !isFetching) {
        try {
          const result = await actor.getGlobalStats();
          if (result.__kind__ === "ok") {
            const { stats } = result.ok;
            return {
              totalMarketCap: stats.totalMarketCap,
              totalVolume24h: stats.totalVolume24h,
              marketCapChangePercentage24h: stats.marketCapChangePercentage24h,
              btcDominance: stats.btcDominance,
              ethDominance: stats.ethDominance,
              activeCryptocurrencies: Number(stats.activeCryptocurrencies),
              markets: Number(stats.markets)
            };
          }
        } catch (_2) {
        }
      }
      return fetchCoinGeckoGlobal();
    },
    staleTime: 6e4,
    refetchInterval: 6e4
  });
}
function useCoinChart(coinId, days, kind, enabled) {
  const { actor, isFetching } = useActor(createActor);
  return useQuery({
    queryKey: ["coinChart", coinId, days, kind],
    enabled: enabled && !!coinId,
    queryFn: async () => {
      if (!coinId) throw new Error("no coin");
      const backendKind = kind === "line" ? ChartKindLine : ChartKindCandle;
      if (actor && !isFetching) {
        try {
          const result = await actor.getCoinChart(coinId, BigInt(days), backendKind);
          if (result.__kind__ === "ok") {
            return {
              coinId: result.ok.coinId,
              days: Number(result.ok.days),
              kind: isLineKind(result.ok.kind) ? "line" : "candle",
              line: result.ok.line.map((p2) => ({ timestamp: Number(p2.timestamp), price: p2.price })),
              candles: result.ok.candles.map((c2) => ({
                timestamp: Number(c2.timestamp),
                open: c2.open,
                high: c2.high,
                low: c2.low,
                close: c2.close
              })),
              updatedAt: Number(result.ok.updatedAt)
            };
          }
        } catch (_2) {
        }
      }
      if (kind === "line") {
        const line = await fetchCoinGeckoChartLine(coinId, days);
        return { coinId, days, kind: "line", line, candles: [], updatedAt: Date.now() };
      }
      const candles = await fetchCoinGeckoChartCandles(coinId, days);
      return { coinId, days, kind: "candle", line: [], candles, updatedAt: Date.now() };
    },
    staleTime: 5 * 6e4
  });
}
const DE_LOCALE = "de-DE";
function formatPrice(value) {
  if (value === 0) return "€0,00";
  if (value < 0.01) {
    return new Intl.NumberFormat(DE_LOCALE, {
      style: "currency",
      currency: "EUR",
      minimumFractionDigits: 6,
      maximumFractionDigits: 6
    }).format(value);
  }
  if (value < 1) {
    return new Intl.NumberFormat(DE_LOCALE, {
      style: "currency",
      currency: "EUR",
      minimumFractionDigits: 4,
      maximumFractionDigits: 4
    }).format(value);
  }
  return new Intl.NumberFormat(DE_LOCALE, {
    style: "currency",
    currency: "EUR",
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}
function formatCompactNumber(value) {
  if (!Number.isFinite(value) || value === 0) return "—";
  if (value >= 1e12) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value / 1e12)} Bio`;
  }
  if (value >= 1e9) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value / 1e9)} Mrd`;
  }
  if (value >= 1e6) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    }).format(value / 1e6)} Mio`;
  }
  if (value >= 1e3) {
    return `${new Intl.NumberFormat(DE_LOCALE, {
      minimumFractionDigits: 1,
      maximumFractionDigits: 1
    }).format(value / 1e3)} Tsd`;
  }
  return new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0
  }).format(value);
}
function formatPercent(value) {
  if (!Number.isFinite(value)) return "—";
  const formatted = new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 2
  }).format(Math.abs(value));
  if (value > 0) return `▲ ${formatted} %`;
  if (value < 0) return `▼ ${formatted} %`;
  return `${formatted} %`;
}
function formatPercentPlain(value) {
  if (!Number.isFinite(value)) return "—";
  return `${new Intl.NumberFormat(DE_LOCALE, {
    minimumFractionDigits: 1,
    maximumFractionDigits: 2
  }).format(value)} %`;
}
function formatSupply(value, symbol) {
  if (!value || !Number.isFinite(value) || value <= 0) return "—";
  return `${formatCompactNumber(value)} ${symbol}`;
}
const CHART_TIMEFRAMES = ["1h", "24h", "7d", "30d", "90d", "1y", "all"];
function timeframeToDays(tf) {
  switch (tf) {
    case "1h":
      return 1;
    case "24h":
      return 1;
    case "7d":
      return 7;
    case "30d":
      return 30;
    case "90d":
      return 90;
    case "1y":
      return 365;
    case "all":
      return 0;
  }
}
function timeframeLabel(tf) {
  switch (tf) {
    case "1h":
      return "1 Std";
    case "24h":
      return "24 Std";
    case "7d":
      return "7 T";
    case "30d":
      return "30 T";
    case "90d":
      return "90 T";
    case "1y":
      return "1 J";
    case "all":
      return "Alle";
  }
}
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const toKebabCase = (string) => string.replace(/([a-z0-9])([A-Z])/g, "$1-$2").toLowerCase();
const toCamelCase = (string) => string.replace(
  /^([A-Z])|[\s-_]+(\w)/g,
  (match, p1, p2) => p2 ? p2.toUpperCase() : p1.toLowerCase()
);
const toPascalCase = (string) => {
  const camelCase = toCamelCase(string);
  return camelCase.charAt(0).toUpperCase() + camelCase.slice(1);
};
const mergeClasses = (...classes) => classes.filter((className, index, array) => {
  return Boolean(className) && className.trim() !== "" && array.indexOf(className) === index;
}).join(" ").trim();
const hasA11yProp = (props) => {
  for (const prop in props) {
    if (prop.startsWith("aria-") || prop === "role" || prop === "title") {
      return true;
    }
  }
};
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
var defaultAttributes = {
  xmlns: "http://www.w3.org/2000/svg",
  width: 24,
  height: 24,
  viewBox: "0 0 24 24",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 2,
  strokeLinecap: "round",
  strokeLinejoin: "round"
};
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const Icon = reactExports.forwardRef(
  ({
    color = "currentColor",
    size: size2 = 24,
    strokeWidth = 2,
    absoluteStrokeWidth,
    className = "",
    children,
    iconNode,
    ...rest
  }, ref) => reactExports.createElement(
    "svg",
    {
      ref,
      ...defaultAttributes,
      width: size2,
      height: size2,
      stroke: color,
      strokeWidth: absoluteStrokeWidth ? Number(strokeWidth) * 24 / Number(size2) : strokeWidth,
      className: mergeClasses("lucide", className),
      ...!children && !hasA11yProp(rest) && { "aria-hidden": "true" },
      ...rest
    },
    [
      ...iconNode.map(([tag, attrs]) => reactExports.createElement(tag, attrs)),
      ...Array.isArray(children) ? children : [children]
    ]
  )
);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const createLucideIcon = (iconName, iconNode) => {
  const Component = reactExports.forwardRef(
    ({ className, ...props }, ref) => reactExports.createElement(Icon, {
      ref,
      iconNode,
      className: mergeClasses(
        `lucide-${toKebabCase(toPascalCase(iconName))}`,
        `lucide-${iconName}`,
        className
      ),
      ...props
    })
  );
  Component.displayName = toPascalCase(iconName);
  return Component;
};
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$9 = [
  [
    "path",
    {
      d: "M22 12h-2.48a2 2 0 0 0-1.93 1.46l-2.35 8.36a.25.25 0 0 1-.48 0L9.24 2.18a.25.25 0 0 0-.48 0l-2.35 8.36A2 2 0 0 1 4.49 12H2",
      key: "169zse"
    }
  ]
];
const Activity = createLucideIcon("activity", __iconNode$9);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$8 = [
  ["path", { d: "m21 16-4 4-4-4", key: "f6ql7i" }],
  ["path", { d: "M17 20V4", key: "1ejh1v" }],
  ["path", { d: "m3 8 4-4 4 4", key: "11wl7u" }],
  ["path", { d: "M7 4v16", key: "1glfcx" }]
];
const ArrowUpDown = createLucideIcon("arrow-up-down", __iconNode$8);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$7 = [
  ["path", { d: "M3 3v16a2 2 0 0 0 2 2h16", key: "c24i48" }],
  ["path", { d: "M18 17V9", key: "2bz60n" }],
  ["path", { d: "M13 17V5", key: "1frdt8" }],
  ["path", { d: "M8 17v-3", key: "17ska0" }]
];
const ChartColumn = createLucideIcon("chart-column", __iconNode$7);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$6 = [
  ["path", { d: "M3 3v16a2 2 0 0 0 2 2h16", key: "c24i48" }],
  ["path", { d: "m19 9-5 5-4-4-3 3", key: "2osh9i" }]
];
const ChartLine = createLucideIcon("chart-line", __iconNode$6);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$5 = [
  ["path", { d: "M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8", key: "v9h5vc" }],
  ["path", { d: "M21 3v5h-5", key: "1q7to0" }],
  ["path", { d: "M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16", key: "3uifl3" }],
  ["path", { d: "M8 16H3v5", key: "1cv678" }]
];
const RefreshCw = createLucideIcon("refresh-cw", __iconNode$5);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$4 = [
  ["path", { d: "m21 21-4.34-4.34", key: "14j7rj" }],
  ["circle", { cx: "11", cy: "11", r: "8", key: "4ej97u" }]
];
const Search = createLucideIcon("search", __iconNode$4);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$3 = [
  [
    "path",
    {
      d: "M11.525 2.295a.53.53 0 0 1 .95 0l2.31 4.679a2.123 2.123 0 0 0 1.595 1.16l5.166.756a.53.53 0 0 1 .294.904l-3.736 3.638a2.123 2.123 0 0 0-.611 1.878l.882 5.14a.53.53 0 0 1-.771.56l-4.618-2.428a2.122 2.122 0 0 0-1.973 0L6.396 21.01a.53.53 0 0 1-.77-.56l.881-5.139a2.122 2.122 0 0 0-.611-1.879L2.16 9.795a.53.53 0 0 1 .294-.906l5.165-.755a2.122 2.122 0 0 0 1.597-1.16z",
      key: "r04s7s"
    }
  ]
];
const Star = createLucideIcon("star", __iconNode$3);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$2 = [
  ["path", { d: "M16 17h6v-6", key: "t6n2it" }],
  ["path", { d: "m22 17-8.5-8.5-5 5L2 7", key: "x473p" }]
];
const TrendingDown = createLucideIcon("trending-down", __iconNode$2);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode$1 = [
  ["path", { d: "M16 7h6v6", key: "box55l" }],
  ["path", { d: "m22 7-8.5 8.5-5-5L2 17", key: "1t1m79" }]
];
const TrendingUp = createLucideIcon("trending-up", __iconNode$1);
/**
 * @license lucide-react v0.511.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */
const __iconNode = [
  ["path", { d: "M18 6 6 18", key: "1bl5f8" }],
  ["path", { d: "m6 6 12 12", key: "d8bk6v" }]
];
const X$1 = createLucideIcon("x", __iconNode);
function size(_a2) {
  var width = _a2.width, height = _a2.height;
  if (width < 0) {
    throw new Error("Negative width is not allowed for Size");
  }
  if (height < 0) {
    throw new Error("Negative height is not allowed for Size");
  }
  return {
    width,
    height
  };
}
function equalSizes(first, second) {
  return first.width === second.width && first.height === second.height;
}
var Observable = (
  /** @class */
  function() {
    function Observable2(win) {
      var _this = this;
      this._resolutionListener = function() {
        return _this._onResolutionChanged();
      };
      this._resolutionMediaQueryList = null;
      this._observers = [];
      this._window = win;
      this._installResolutionListener();
    }
    Observable2.prototype.dispose = function() {
      this._uninstallResolutionListener();
      this._window = null;
    };
    Object.defineProperty(Observable2.prototype, "value", {
      get: function() {
        return this._window.devicePixelRatio;
      },
      enumerable: false,
      configurable: true
    });
    Observable2.prototype.subscribe = function(next) {
      var _this = this;
      var observer = { next };
      this._observers.push(observer);
      return {
        unsubscribe: function() {
          _this._observers = _this._observers.filter(function(o2) {
            return o2 !== observer;
          });
        }
      };
    };
    Observable2.prototype._installResolutionListener = function() {
      if (this._resolutionMediaQueryList !== null) {
        throw new Error("Resolution listener is already installed");
      }
      var dppx = this._window.devicePixelRatio;
      this._resolutionMediaQueryList = this._window.matchMedia("all and (resolution: ".concat(dppx, "dppx)"));
      this._resolutionMediaQueryList.addListener(this._resolutionListener);
    };
    Observable2.prototype._uninstallResolutionListener = function() {
      if (this._resolutionMediaQueryList !== null) {
        this._resolutionMediaQueryList.removeListener(this._resolutionListener);
        this._resolutionMediaQueryList = null;
      }
    };
    Observable2.prototype._reinstallResolutionListener = function() {
      this._uninstallResolutionListener();
      this._installResolutionListener();
    };
    Observable2.prototype._onResolutionChanged = function() {
      var _this = this;
      this._observers.forEach(function(observer) {
        return observer.next(_this._window.devicePixelRatio);
      });
      this._reinstallResolutionListener();
    };
    return Observable2;
  }()
);
function createObservable(win) {
  return new Observable(win);
}
var DevicePixelContentBoxBinding = (
  /** @class */
  function() {
    function DevicePixelContentBoxBinding2(canvasElement, transformBitmapSize, options) {
      var _a2;
      this._canvasElement = null;
      this._bitmapSizeChangedListeners = [];
      this._suggestedBitmapSize = null;
      this._suggestedBitmapSizeChangedListeners = [];
      this._devicePixelRatioObservable = null;
      this._canvasElementResizeObserver = null;
      this._canvasElement = canvasElement;
      this._canvasElementClientSize = size({
        width: this._canvasElement.clientWidth,
        height: this._canvasElement.clientHeight
      });
      this._transformBitmapSize = transformBitmapSize !== null && transformBitmapSize !== void 0 ? transformBitmapSize : function(size2) {
        return size2;
      };
      this._allowResizeObserver = (_a2 = options === null || options === void 0 ? void 0 : options.allowResizeObserver) !== null && _a2 !== void 0 ? _a2 : true;
      this._chooseAndInitObserver();
    }
    DevicePixelContentBoxBinding2.prototype.dispose = function() {
      var _a2, _b;
      if (this._canvasElement === null) {
        throw new Error("Object is disposed");
      }
      (_a2 = this._canvasElementResizeObserver) === null || _a2 === void 0 ? void 0 : _a2.disconnect();
      this._canvasElementResizeObserver = null;
      (_b = this._devicePixelRatioObservable) === null || _b === void 0 ? void 0 : _b.dispose();
      this._devicePixelRatioObservable = null;
      this._suggestedBitmapSizeChangedListeners.length = 0;
      this._bitmapSizeChangedListeners.length = 0;
      this._canvasElement = null;
    };
    Object.defineProperty(DevicePixelContentBoxBinding2.prototype, "canvasElement", {
      get: function() {
        if (this._canvasElement === null) {
          throw new Error("Object is disposed");
        }
        return this._canvasElement;
      },
      enumerable: false,
      configurable: true
    });
    Object.defineProperty(DevicePixelContentBoxBinding2.prototype, "canvasElementClientSize", {
      get: function() {
        return this._canvasElementClientSize;
      },
      enumerable: false,
      configurable: true
    });
    Object.defineProperty(DevicePixelContentBoxBinding2.prototype, "bitmapSize", {
      get: function() {
        return size({
          width: this.canvasElement.width,
          height: this.canvasElement.height
        });
      },
      enumerable: false,
      configurable: true
    });
    DevicePixelContentBoxBinding2.prototype.resizeCanvasElement = function(clientSize) {
      this._canvasElementClientSize = size(clientSize);
      this.canvasElement.style.width = "".concat(this._canvasElementClientSize.width, "px");
      this.canvasElement.style.height = "".concat(this._canvasElementClientSize.height, "px");
      this._invalidateBitmapSize();
    };
    DevicePixelContentBoxBinding2.prototype.subscribeBitmapSizeChanged = function(listener) {
      this._bitmapSizeChangedListeners.push(listener);
    };
    DevicePixelContentBoxBinding2.prototype.unsubscribeBitmapSizeChanged = function(listener) {
      this._bitmapSizeChangedListeners = this._bitmapSizeChangedListeners.filter(function(l2) {
        return l2 !== listener;
      });
    };
    Object.defineProperty(DevicePixelContentBoxBinding2.prototype, "suggestedBitmapSize", {
      get: function() {
        return this._suggestedBitmapSize;
      },
      enumerable: false,
      configurable: true
    });
    DevicePixelContentBoxBinding2.prototype.subscribeSuggestedBitmapSizeChanged = function(listener) {
      this._suggestedBitmapSizeChangedListeners.push(listener);
    };
    DevicePixelContentBoxBinding2.prototype.unsubscribeSuggestedBitmapSizeChanged = function(listener) {
      this._suggestedBitmapSizeChangedListeners = this._suggestedBitmapSizeChangedListeners.filter(function(l2) {
        return l2 !== listener;
      });
    };
    DevicePixelContentBoxBinding2.prototype.applySuggestedBitmapSize = function() {
      if (this._suggestedBitmapSize === null) {
        return;
      }
      var oldSuggestedSize = this._suggestedBitmapSize;
      this._suggestedBitmapSize = null;
      this._resizeBitmap(oldSuggestedSize);
      this._emitSuggestedBitmapSizeChanged(oldSuggestedSize, this._suggestedBitmapSize);
    };
    DevicePixelContentBoxBinding2.prototype._resizeBitmap = function(newSize) {
      var oldSize = this.bitmapSize;
      if (equalSizes(oldSize, newSize)) {
        return;
      }
      this.canvasElement.width = newSize.width;
      this.canvasElement.height = newSize.height;
      this._emitBitmapSizeChanged(oldSize, newSize);
    };
    DevicePixelContentBoxBinding2.prototype._emitBitmapSizeChanged = function(oldSize, newSize) {
      var _this = this;
      this._bitmapSizeChangedListeners.forEach(function(listener) {
        return listener.call(_this, oldSize, newSize);
      });
    };
    DevicePixelContentBoxBinding2.prototype._suggestNewBitmapSize = function(newSize) {
      var oldSuggestedSize = this._suggestedBitmapSize;
      var finalNewSize = size(this._transformBitmapSize(newSize, this._canvasElementClientSize));
      var newSuggestedSize = equalSizes(this.bitmapSize, finalNewSize) ? null : finalNewSize;
      if (oldSuggestedSize === null && newSuggestedSize === null) {
        return;
      }
      if (oldSuggestedSize !== null && newSuggestedSize !== null && equalSizes(oldSuggestedSize, newSuggestedSize)) {
        return;
      }
      this._suggestedBitmapSize = newSuggestedSize;
      this._emitSuggestedBitmapSizeChanged(oldSuggestedSize, newSuggestedSize);
    };
    DevicePixelContentBoxBinding2.prototype._emitSuggestedBitmapSizeChanged = function(oldSize, newSize) {
      var _this = this;
      this._suggestedBitmapSizeChangedListeners.forEach(function(listener) {
        return listener.call(_this, oldSize, newSize);
      });
    };
    DevicePixelContentBoxBinding2.prototype._chooseAndInitObserver = function() {
      var _this = this;
      if (!this._allowResizeObserver) {
        this._initDevicePixelRatioObservable();
        return;
      }
      isDevicePixelContentBoxSupported().then(function(isSupported) {
        return isSupported ? _this._initResizeObserver() : _this._initDevicePixelRatioObservable();
      });
    };
    DevicePixelContentBoxBinding2.prototype._initDevicePixelRatioObservable = function() {
      var _this = this;
      if (this._canvasElement === null) {
        return;
      }
      var win = canvasElementWindow(this._canvasElement);
      if (win === null) {
        throw new Error("No window is associated with the canvas");
      }
      this._devicePixelRatioObservable = createObservable(win);
      this._devicePixelRatioObservable.subscribe(function() {
        return _this._invalidateBitmapSize();
      });
      this._invalidateBitmapSize();
    };
    DevicePixelContentBoxBinding2.prototype._invalidateBitmapSize = function() {
      var _a2, _b;
      if (this._canvasElement === null) {
        return;
      }
      var win = canvasElementWindow(this._canvasElement);
      if (win === null) {
        return;
      }
      var ratio = (_b = (_a2 = this._devicePixelRatioObservable) === null || _a2 === void 0 ? void 0 : _a2.value) !== null && _b !== void 0 ? _b : win.devicePixelRatio;
      var canvasRects = this._canvasElement.getClientRects();
      var newSize = (
        // eslint-disable-next-line no-negated-condition
        canvasRects[0] !== void 0 ? predictedBitmapSize(canvasRects[0], ratio) : size({
          width: this._canvasElementClientSize.width * ratio,
          height: this._canvasElementClientSize.height * ratio
        })
      );
      this._suggestNewBitmapSize(newSize);
    };
    DevicePixelContentBoxBinding2.prototype._initResizeObserver = function() {
      var _this = this;
      if (this._canvasElement === null) {
        return;
      }
      this._canvasElementResizeObserver = new ResizeObserver(function(entries) {
        var entry = entries.find(function(entry2) {
          return entry2.target === _this._canvasElement;
        });
        if (!entry || !entry.devicePixelContentBoxSize || !entry.devicePixelContentBoxSize[0]) {
          return;
        }
        var entrySize = entry.devicePixelContentBoxSize[0];
        var newSize = size({
          width: entrySize.inlineSize,
          height: entrySize.blockSize
        });
        _this._suggestNewBitmapSize(newSize);
      });
      this._canvasElementResizeObserver.observe(this._canvasElement, { box: "device-pixel-content-box" });
    };
    return DevicePixelContentBoxBinding2;
  }()
);
function bindTo(canvasElement, target) {
  {
    return new DevicePixelContentBoxBinding(canvasElement, target.transform, target.options);
  }
}
function canvasElementWindow(canvasElement) {
  return canvasElement.ownerDocument.defaultView;
}
function isDevicePixelContentBoxSupported() {
  return new Promise(function(resolve) {
    var ro = new ResizeObserver(function(entries) {
      resolve(entries.every(function(entry) {
        return "devicePixelContentBoxSize" in entry;
      }));
      ro.disconnect();
    });
    ro.observe(document.body, { box: "device-pixel-content-box" });
  }).catch(function() {
    return false;
  });
}
function predictedBitmapSize(canvasRect, ratio) {
  return size({
    width: Math.round(canvasRect.left * ratio + canvasRect.width * ratio) - Math.round(canvasRect.left * ratio),
    height: Math.round(canvasRect.top * ratio + canvasRect.height * ratio) - Math.round(canvasRect.top * ratio)
  });
}
var CanvasRenderingTarget2D = (
  /** @class */
  function() {
    function CanvasRenderingTarget2D2(context, mediaSize, bitmapSize) {
      if (mediaSize.width === 0 || mediaSize.height === 0) {
        throw new TypeError("Rendering target could only be created on a media with positive width and height");
      }
      this._mediaSize = mediaSize;
      if (bitmapSize.width === 0 || bitmapSize.height === 0) {
        throw new TypeError("Rendering target could only be created using a bitmap with positive integer width and height");
      }
      this._bitmapSize = bitmapSize;
      this._context = context;
    }
    CanvasRenderingTarget2D2.prototype.useMediaCoordinateSpace = function(f2) {
      try {
        this._context.save();
        this._context.setTransform(1, 0, 0, 1, 0, 0);
        this._context.scale(this._horizontalPixelRatio, this._verticalPixelRatio);
        return f2({
          context: this._context,
          mediaSize: this._mediaSize
        });
      } finally {
        this._context.restore();
      }
    };
    CanvasRenderingTarget2D2.prototype.useBitmapCoordinateSpace = function(f2) {
      try {
        this._context.save();
        this._context.setTransform(1, 0, 0, 1, 0, 0);
        return f2({
          context: this._context,
          mediaSize: this._mediaSize,
          bitmapSize: this._bitmapSize,
          horizontalPixelRatio: this._horizontalPixelRatio,
          verticalPixelRatio: this._verticalPixelRatio
        });
      } finally {
        this._context.restore();
      }
    };
    Object.defineProperty(CanvasRenderingTarget2D2.prototype, "_horizontalPixelRatio", {
      get: function() {
        return this._bitmapSize.width / this._mediaSize.width;
      },
      enumerable: false,
      configurable: true
    });
    Object.defineProperty(CanvasRenderingTarget2D2.prototype, "_verticalPixelRatio", {
      get: function() {
        return this._bitmapSize.height / this._mediaSize.height;
      },
      enumerable: false,
      configurable: true
    });
    return CanvasRenderingTarget2D2;
  }()
);
function tryCreateCanvasRenderingTarget2D(binding, contextOptions) {
  var mediaSize = binding.canvasElementClientSize;
  if (mediaSize.width === 0 || mediaSize.height === 0) {
    return null;
  }
  var bitmapSize = binding.bitmapSize;
  if (bitmapSize.width === 0 || bitmapSize.height === 0) {
    return null;
  }
  var context = binding.canvasElement.getContext("2d", contextOptions);
  if (context === null) {
    return null;
  }
  return new CanvasRenderingTarget2D(context, mediaSize, bitmapSize);
}
/*!
 * @license
 * TradingView Lightweight Charts™ v4.2.3
 * Copyright (c) 2025 TradingView, Inc.
 * Licensed under Apache License 2.0 https://www.apache.org/licenses/LICENSE-2.0
 */
const e = { upColor: "#26a69a", downColor: "#ef5350", wickVisible: true, borderVisible: true, borderColor: "#378658", borderUpColor: "#26a69a", borderDownColor: "#ef5350", wickColor: "#737375", wickUpColor: "#26a69a", wickDownColor: "#ef5350" }, r = { upColor: "#26a69a", downColor: "#ef5350", openVisible: true, thinBars: true }, h = { color: "#2196f3", lineStyle: 0, lineWidth: 3, lineType: 0, lineVisible: true, crosshairMarkerVisible: true, crosshairMarkerRadius: 4, crosshairMarkerBorderColor: "", crosshairMarkerBorderWidth: 2, crosshairMarkerBackgroundColor: "", lastPriceAnimation: 0, pointMarkersVisible: false }, l = { topColor: "rgba( 46, 220, 135, 0.4)", bottomColor: "rgba( 40, 221, 100, 0)", invertFilledArea: false, lineColor: "#33D778", lineStyle: 0, lineWidth: 3, lineType: 0, lineVisible: true, crosshairMarkerVisible: true, crosshairMarkerRadius: 4, crosshairMarkerBorderColor: "", crosshairMarkerBorderWidth: 2, crosshairMarkerBackgroundColor: "", lastPriceAnimation: 0, pointMarkersVisible: false }, a = { baseValue: { type: "price", price: 0 }, topFillColor1: "rgba(38, 166, 154, 0.28)", topFillColor2: "rgba(38, 166, 154, 0.05)", topLineColor: "rgba(38, 166, 154, 1)", bottomFillColor1: "rgba(239, 83, 80, 0.05)", bottomFillColor2: "rgba(239, 83, 80, 0.28)", bottomLineColor: "rgba(239, 83, 80, 1)", lineWidth: 3, lineStyle: 0, lineType: 0, lineVisible: true, crosshairMarkerVisible: true, crosshairMarkerRadius: 4, crosshairMarkerBorderColor: "", crosshairMarkerBorderWidth: 2, crosshairMarkerBackgroundColor: "", lastPriceAnimation: 0, pointMarkersVisible: false }, o = { color: "#26a69a", base: 0 }, _ = { color: "#2196f3" }, u = { title: "", visible: true, lastValueVisible: true, priceLineVisible: true, priceLineSource: 0, priceLineWidth: 1, priceLineColor: "", priceLineStyle: 2, baseLineVisible: true, baseLineWidth: 1, baseLineColor: "#B2B5BE", baseLineStyle: 0, priceFormat: { type: "price", precision: 2, minMove: 0.01 } };
var c, d;
function f(t, i) {
  const n = { 0: [], 1: [t.lineWidth, t.lineWidth], 2: [2 * t.lineWidth, 2 * t.lineWidth], 3: [6 * t.lineWidth, 6 * t.lineWidth], 4: [t.lineWidth, 4 * t.lineWidth] }[i];
  t.setLineDash(n);
}
function v(t, i, n, s) {
  t.beginPath();
  const e2 = t.lineWidth % 2 ? 0.5 : 0;
  t.moveTo(n, i + e2), t.lineTo(s, i + e2), t.stroke();
}
function p(t, i) {
  if (!t) throw new Error("Assertion failed" + (i ? ": " + i : ""));
}
function m(t) {
  if (void 0 === t) throw new Error("Value is undefined");
  return t;
}
function b(t) {
  if (null === t) throw new Error("Value is null");
  return t;
}
function w(t) {
  return b(m(t));
}
!function(t) {
  t[t.Simple = 0] = "Simple", t[t.WithSteps = 1] = "WithSteps", t[t.Curved = 2] = "Curved";
}(c || (c = {})), function(t) {
  t[t.Solid = 0] = "Solid", t[t.Dotted = 1] = "Dotted", t[t.Dashed = 2] = "Dashed", t[t.LargeDashed = 3] = "LargeDashed", t[t.SparseDotted = 4] = "SparseDotted";
}(d || (d = {}));
const g = { khaki: "#f0e68c", azure: "#f0ffff", aliceblue: "#f0f8ff", ghostwhite: "#f8f8ff", gold: "#ffd700", goldenrod: "#daa520", gainsboro: "#dcdcdc", gray: "#808080", green: "#008000", honeydew: "#f0fff0", floralwhite: "#fffaf0", lightblue: "#add8e6", lightcoral: "#f08080", lemonchiffon: "#fffacd", hotpink: "#ff69b4", lightyellow: "#ffffe0", greenyellow: "#adff2f", lightgoldenrodyellow: "#fafad2", limegreen: "#32cd32", linen: "#faf0e6", lightcyan: "#e0ffff", magenta: "#f0f", maroon: "#800000", olive: "#808000", orange: "#ffa500", oldlace: "#fdf5e6", mediumblue: "#0000cd", transparent: "#0000", lime: "#0f0", lightpink: "#ffb6c1", mistyrose: "#ffe4e1", moccasin: "#ffe4b5", midnightblue: "#191970", orchid: "#da70d6", mediumorchid: "#ba55d3", mediumturquoise: "#48d1cc", orangered: "#ff4500", royalblue: "#4169e1", powderblue: "#b0e0e6", red: "#f00", coral: "#ff7f50", turquoise: "#40e0d0", white: "#fff", whitesmoke: "#f5f5f5", wheat: "#f5deb3", teal: "#008080", steelblue: "#4682b4", bisque: "#ffe4c4", aquamarine: "#7fffd4", aqua: "#0ff", sienna: "#a0522d", silver: "#c0c0c0", springgreen: "#00ff7f", antiquewhite: "#faebd7", burlywood: "#deb887", brown: "#a52a2a", beige: "#f5f5dc", chocolate: "#d2691e", chartreuse: "#7fff00", cornflowerblue: "#6495ed", cornsilk: "#fff8dc", crimson: "#dc143c", cadetblue: "#5f9ea0", tomato: "#ff6347", fuchsia: "#f0f", blue: "#00f", salmon: "#fa8072", blanchedalmond: "#ffebcd", slateblue: "#6a5acd", slategray: "#708090", thistle: "#d8bfd8", tan: "#d2b48c", cyan: "#0ff", darkblue: "#00008b", darkcyan: "#008b8b", darkgoldenrod: "#b8860b", darkgray: "#a9a9a9", blueviolet: "#8a2be2", black: "#000", darkmagenta: "#8b008b", darkslateblue: "#483d8b", darkkhaki: "#bdb76b", darkorchid: "#9932cc", darkorange: "#ff8c00", darkgreen: "#006400", darkred: "#8b0000", dodgerblue: "#1e90ff", darkslategray: "#2f4f4f", dimgray: "#696969", deepskyblue: "#00bfff", firebrick: "#b22222", forestgreen: "#228b22", indigo: "#4b0082", ivory: "#fffff0", lavenderblush: "#fff0f5", feldspar: "#d19275", indianred: "#cd5c5c", lightgreen: "#90ee90", lightgrey: "#d3d3d3", lightskyblue: "#87cefa", lightslategray: "#789", lightslateblue: "#8470ff", snow: "#fffafa", lightseagreen: "#20b2aa", lightsalmon: "#ffa07a", darksalmon: "#e9967a", darkviolet: "#9400d3", mediumpurple: "#9370d8", mediumaquamarine: "#66cdaa", skyblue: "#87ceeb", lavender: "#e6e6fa", lightsteelblue: "#b0c4de", mediumvioletred: "#c71585", mintcream: "#f5fffa", navajowhite: "#ffdead", navy: "#000080", olivedrab: "#6b8e23", palevioletred: "#d87093", violetred: "#d02090", yellow: "#ff0", yellowgreen: "#9acd32", lawngreen: "#7cfc00", pink: "#ffc0cb", paleturquoise: "#afeeee", palegoldenrod: "#eee8aa", darkolivegreen: "#556b2f", darkseagreen: "#8fbc8f", darkturquoise: "#00ced1", peachpuff: "#ffdab9", deeppink: "#ff1493", violet: "#ee82ee", palegreen: "#98fb98", mediumseagreen: "#3cb371", peru: "#cd853f", saddlebrown: "#8b4513", sandybrown: "#f4a460", rosybrown: "#bc8f8f", purple: "#800080", seagreen: "#2e8b57", seashell: "#fff5ee", papayawhip: "#ffefd5", mediumslateblue: "#7b68ee", plum: "#dda0dd", mediumspringgreen: "#00fa9a" };
function M(t) {
  return t < 0 ? 0 : t > 255 ? 255 : Math.round(t) || 0;
}
function x(t) {
  return t <= 0 || t > 1 ? Math.min(Math.max(t, 0), 1) : Math.round(1e4 * t) / 1e4;
}
const S = /^#([0-9a-f])([0-9a-f])([0-9a-f])([0-9a-f])?$/i, k = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})?$/i, y = /^rgb\(\s*(-?\d{1,10})\s*,\s*(-?\d{1,10})\s*,\s*(-?\d{1,10})\s*\)$/, C = /^rgba\(\s*(-?\d{1,10})\s*,\s*(-?\d{1,10})\s*,\s*(-?\d{1,10})\s*,\s*(-?\d*\.?\d+)\s*\)$/;
function T(t) {
  (t = t.toLowerCase()) in g && (t = g[t]);
  {
    const i = C.exec(t) || y.exec(t);
    if (i) return [M(parseInt(i[1], 10)), M(parseInt(i[2], 10)), M(parseInt(i[3], 10)), x(i.length < 5 ? 1 : parseFloat(i[4]))];
  }
  {
    const i = k.exec(t);
    if (i) return [M(parseInt(i[1], 16)), M(parseInt(i[2], 16)), M(parseInt(i[3], 16)), 1];
  }
  {
    const i = S.exec(t);
    if (i) return [M(17 * parseInt(i[1], 16)), M(17 * parseInt(i[2], 16)), M(17 * parseInt(i[3], 16)), 1];
  }
  throw new Error(`Cannot parse color: ${t}`);
}
function P(t) {
  return 0.199 * t[0] + 0.687 * t[1] + 0.114 * t[2];
}
function R(t) {
  const i = T(t);
  return { t: `rgb(${i[0]}, ${i[1]}, ${i[2]})`, i: P(i) > 160 ? "black" : "white" };
}
class D {
  constructor() {
    this.h = [];
  }
  l(t, i, n) {
    const s = { o: t, _: i, u: true === n };
    this.h.push(s);
  }
  v(t) {
    const i = this.h.findIndex((i2) => t === i2.o);
    i > -1 && this.h.splice(i, 1);
  }
  p(t) {
    this.h = this.h.filter((i) => i._ !== t);
  }
  m(t, i, n) {
    const s = [...this.h];
    this.h = this.h.filter((t2) => !t2.u), s.forEach((s2) => s2.o(t, i, n));
  }
  M() {
    return this.h.length > 0;
  }
  S() {
    this.h = [];
  }
}
function V(t, ...i) {
  for (const n of i) for (const i2 in n) void 0 !== n[i2] && Object.prototype.hasOwnProperty.call(n, i2) && !["__proto__", "constructor", "prototype"].includes(i2) && ("object" != typeof n[i2] || void 0 === t[i2] || Array.isArray(n[i2]) ? t[i2] = n[i2] : V(t[i2], n[i2]));
  return t;
}
function O(t) {
  return "number" == typeof t && isFinite(t);
}
function B(t) {
  return "number" == typeof t && t % 1 == 0;
}
function A(t) {
  return "string" == typeof t;
}
function I(t) {
  return "boolean" == typeof t;
}
function z(t) {
  const i = t;
  if (!i || "object" != typeof i) return i;
  let n, s, e2;
  for (s in n = Array.isArray(i) ? [] : {}, i) i.hasOwnProperty(s) && (e2 = i[s], n[s] = e2 && "object" == typeof e2 ? z(e2) : e2);
  return n;
}
function L(t) {
  return null !== t;
}
function E(t) {
  return null === t ? void 0 : t;
}
const N = "-apple-system, BlinkMacSystemFont, 'Trebuchet MS', Roboto, Ubuntu, sans-serif";
function F(t, i, n) {
  return void 0 === i && (i = N), `${n = void 0 !== n ? `${n} ` : ""}${t}px ${i}`;
}
class W {
  constructor(t) {
    this.k = { C: 1, T: 5, P: NaN, R: "", D: "", V: "", O: "", B: 0, A: 0, I: 0, L: 0, N: 0 }, this.F = t;
  }
  W() {
    const t = this.k, i = this.j(), n = this.H();
    return t.P === i && t.D === n || (t.P = i, t.D = n, t.R = F(i, n), t.L = 2.5 / 12 * i, t.B = t.L, t.A = i / 12 * t.T, t.I = i / 12 * t.T, t.N = 0), t.V = this.$(), t.O = this.U(), this.k;
  }
  $() {
    return this.F.W().layout.textColor;
  }
  U() {
    return this.F.q();
  }
  j() {
    return this.F.W().layout.fontSize;
  }
  H() {
    return this.F.W().layout.fontFamily;
  }
}
class j {
  constructor() {
    this.Y = [];
  }
  Z(t) {
    this.Y = t;
  }
  X(t, i, n) {
    this.Y.forEach((s) => {
      s.X(t, i, n);
    });
  }
}
class H {
  X(t, i, n) {
    t.useBitmapCoordinateSpace((t2) => this.K(t2, i, n));
  }
}
class $ extends H {
  constructor() {
    super(...arguments), this.G = null;
  }
  J(t) {
    this.G = t;
  }
  K({ context: t, horizontalPixelRatio: i, verticalPixelRatio: n }) {
    if (null === this.G || null === this.G.tt) return;
    const s = this.G.tt, e2 = this.G, r2 = Math.max(1, Math.floor(i)) % 2 / 2, h2 = (h3) => {
      t.beginPath();
      for (let l2 = s.to - 1; l2 >= s.from; --l2) {
        const s2 = e2.it[l2], a2 = Math.round(s2.nt * i) + r2, o2 = s2.st * n, _2 = h3 * n + r2;
        t.moveTo(a2, o2), t.arc(a2, o2, _2, 0, 2 * Math.PI);
      }
      t.fill();
    };
    e2.et > 0 && (t.fillStyle = e2.rt, h2(e2.ht + e2.et)), t.fillStyle = e2.lt, h2(e2.ht);
  }
}
function U() {
  return { it: [{ nt: 0, st: 0, ot: 0, _t: 0 }], lt: "", rt: "", ht: 0, et: 0, tt: null };
}
const q = { from: 0, to: 1 };
class Y {
  constructor(t, i) {
    this.ut = new j(), this.ct = [], this.dt = [], this.ft = true, this.F = t, this.vt = i, this.ut.Z(this.ct);
  }
  bt(t) {
    const i = this.F.wt();
    i.length !== this.ct.length && (this.dt = i.map(U), this.ct = this.dt.map((t2) => {
      const i2 = new $();
      return i2.J(t2), i2;
    }), this.ut.Z(this.ct)), this.ft = true;
  }
  gt() {
    return this.ft && (this.Mt(), this.ft = false), this.ut;
  }
  Mt() {
    const t = 2 === this.vt.W().mode, i = this.F.wt(), n = this.vt.xt(), s = this.F.St();
    i.forEach((i2, e2) => {
      var r2;
      const h2 = this.dt[e2], l2 = i2.kt(n);
      if (t || null === l2 || !i2.yt()) return void (h2.tt = null);
      const a2 = b(i2.Ct());
      h2.lt = l2.Tt, h2.ht = l2.ht, h2.et = l2.Pt, h2.it[0]._t = l2._t, h2.it[0].st = i2.Dt().Rt(l2._t, a2.Vt), h2.rt = null !== (r2 = l2.Ot) && void 0 !== r2 ? r2 : this.F.Bt(h2.it[0].st / i2.Dt().At()), h2.it[0].ot = n, h2.it[0].nt = s.It(n), h2.tt = q;
    });
  }
}
class Z extends H {
  constructor(t) {
    super(), this.zt = t;
  }
  K({ context: t, bitmapSize: i, horizontalPixelRatio: n, verticalPixelRatio: s }) {
    if (null === this.zt) return;
    const e2 = this.zt.Lt.yt, r2 = this.zt.Et.yt;
    if (!e2 && !r2) return;
    const h2 = Math.round(this.zt.nt * n), l2 = Math.round(this.zt.st * s);
    t.lineCap = "butt", e2 && h2 >= 0 && (t.lineWidth = Math.floor(this.zt.Lt.et * n), t.strokeStyle = this.zt.Lt.V, t.fillStyle = this.zt.Lt.V, f(t, this.zt.Lt.Nt), function(t2, i2, n2, s2) {
      t2.beginPath();
      const e3 = t2.lineWidth % 2 ? 0.5 : 0;
      t2.moveTo(i2 + e3, n2), t2.lineTo(i2 + e3, s2), t2.stroke();
    }(t, h2, 0, i.height)), r2 && l2 >= 0 && (t.lineWidth = Math.floor(this.zt.Et.et * s), t.strokeStyle = this.zt.Et.V, t.fillStyle = this.zt.Et.V, f(t, this.zt.Et.Nt), v(t, l2, 0, i.width));
  }
}
class X {
  constructor(t) {
    this.ft = true, this.Ft = { Lt: { et: 1, Nt: 0, V: "", yt: false }, Et: { et: 1, Nt: 0, V: "", yt: false }, nt: 0, st: 0 }, this.Wt = new Z(this.Ft), this.jt = t;
  }
  bt() {
    this.ft = true;
  }
  gt() {
    return this.ft && (this.Mt(), this.ft = false), this.Wt;
  }
  Mt() {
    const t = this.jt.yt(), i = b(this.jt.Ht()), n = i.$t().W().crosshair, s = this.Ft;
    if (2 === n.mode) return s.Et.yt = false, void (s.Lt.yt = false);
    s.Et.yt = t && this.jt.Ut(i), s.Lt.yt = t && this.jt.qt(), s.Et.et = n.horzLine.width, s.Et.Nt = n.horzLine.style, s.Et.V = n.horzLine.color, s.Lt.et = n.vertLine.width, s.Lt.Nt = n.vertLine.style, s.Lt.V = n.vertLine.color, s.nt = this.jt.Yt(), s.st = this.jt.Zt();
  }
}
function K(t, i, n, s, e2, r2) {
  t.fillRect(i + r2, n, s - 2 * r2, r2), t.fillRect(i + r2, n + e2 - r2, s - 2 * r2, r2), t.fillRect(i, n, r2, e2), t.fillRect(i + s - r2, n, r2, e2);
}
function G(t, i, n, s, e2, r2) {
  t.save(), t.globalCompositeOperation = "copy", t.fillStyle = r2, t.fillRect(i, n, s, e2), t.restore();
}
function J(t, i, n, s, e2, r2) {
  t.beginPath(), t.roundRect ? t.roundRect(i, n, s, e2, r2) : (t.lineTo(i + s - r2[1], n), 0 !== r2[1] && t.arcTo(i + s, n, i + s, n + r2[1], r2[1]), t.lineTo(i + s, n + e2 - r2[2]), 0 !== r2[2] && t.arcTo(i + s, n + e2, i + s - r2[2], n + e2, r2[2]), t.lineTo(i + r2[3], n + e2), 0 !== r2[3] && t.arcTo(i, n + e2, i, n + e2 - r2[3], r2[3]), t.lineTo(i, n + r2[0]), 0 !== r2[0] && t.arcTo(i, n, i + r2[0], n, r2[0]));
}
function Q(t, i, n, s, e2, r2, h2 = 0, l2 = [0, 0, 0, 0], a2 = "") {
  if (t.save(), !h2 || !a2 || a2 === r2) return J(t, i, n, s, e2, l2), t.fillStyle = r2, t.fill(), void t.restore();
  const o2 = h2 / 2;
  var _2;
  J(t, i + o2, n + o2, s - h2, e2 - h2, (_2 = -o2, l2.map((t2) => 0 === t2 ? t2 : t2 + _2))), "transparent" !== r2 && (t.fillStyle = r2, t.fill()), "transparent" !== a2 && (t.lineWidth = h2, t.strokeStyle = a2, t.closePath(), t.stroke()), t.restore();
}
function tt(t, i, n, s, e2, r2, h2) {
  t.save(), t.globalCompositeOperation = "copy";
  const l2 = t.createLinearGradient(0, 0, 0, e2);
  l2.addColorStop(0, r2), l2.addColorStop(1, h2), t.fillStyle = l2, t.fillRect(i, n, s, e2), t.restore();
}
class it {
  constructor(t, i) {
    this.J(t, i);
  }
  J(t, i) {
    this.zt = t, this.Xt = i;
  }
  At(t, i) {
    return this.zt.yt ? t.P + t.L + t.B : 0;
  }
  X(t, i, n, s) {
    if (!this.zt.yt || 0 === this.zt.Kt.length) return;
    const e2 = this.zt.V, r2 = this.Xt.t, h2 = t.useBitmapCoordinateSpace((t2) => {
      const h3 = t2.context;
      h3.font = i.R;
      const l2 = this.Gt(t2, i, n, s), a2 = l2.Jt;
      return l2.Qt ? Q(h3, a2.ti, a2.ii, a2.ni, a2.si, r2, a2.ei, [a2.ht, 0, 0, a2.ht], r2) : Q(h3, a2.ri, a2.ii, a2.ni, a2.si, r2, a2.ei, [0, a2.ht, a2.ht, 0], r2), this.zt.hi && (h3.fillStyle = e2, h3.fillRect(a2.ri, a2.li, a2.ai - a2.ri, a2.oi)), this.zt._i && (h3.fillStyle = i.O, h3.fillRect(l2.Qt ? a2.ui - a2.ei : 0, a2.ii, a2.ei, a2.ci - a2.ii)), l2;
    });
    t.useMediaCoordinateSpace(({ context: t2 }) => {
      const n2 = h2.di;
      t2.font = i.R, t2.textAlign = h2.Qt ? "right" : "left", t2.textBaseline = "middle", t2.fillStyle = e2, t2.fillText(this.zt.Kt, n2.fi, (n2.ii + n2.ci) / 2 + n2.pi);
    });
  }
  Gt(t, i, n, s) {
    var e2;
    const { context: r2, bitmapSize: h2, mediaSize: l2, horizontalPixelRatio: a2, verticalPixelRatio: o2 } = t, _2 = this.zt.hi || !this.zt.mi ? i.T : 0, u2 = this.zt.bi ? i.C : 0, c2 = i.L + this.Xt.wi, d2 = i.B + this.Xt.gi, f2 = i.A, v2 = i.I, p2 = this.zt.Kt, m2 = i.P, b2 = n.Mi(r2, p2), w2 = Math.ceil(n.xi(r2, p2)), g2 = m2 + c2 + d2, M2 = i.C + f2 + v2 + w2 + _2, x2 = Math.max(1, Math.floor(o2));
    let S2 = Math.round(g2 * o2);
    S2 % 2 != x2 % 2 && (S2 += 1);
    const k2 = u2 > 0 ? Math.max(1, Math.floor(u2 * a2)) : 0, y2 = Math.round(M2 * a2), C2 = Math.round(_2 * a2), T2 = null !== (e2 = this.Xt.Si) && void 0 !== e2 ? e2 : this.Xt.ki, P2 = Math.round(T2 * o2) - Math.floor(0.5 * o2), R2 = Math.floor(P2 + x2 / 2 - S2 / 2), D2 = R2 + S2, V2 = "right" === s, O2 = V2 ? l2.width - u2 : u2, B2 = V2 ? h2.width - k2 : k2;
    let A2, I2, z2;
    return V2 ? (A2 = B2 - y2, I2 = B2 - C2, z2 = O2 - _2 - f2 - u2) : (A2 = B2 + y2, I2 = B2 + C2, z2 = O2 + _2 + f2), { Qt: V2, Jt: { ii: R2, li: P2, ci: D2, ni: y2, si: S2, ht: 2 * a2, ei: k2, ti: A2, ri: B2, ai: I2, oi: x2, ui: h2.width }, di: { ii: R2 / o2, ci: D2 / o2, fi: z2, pi: b2 } };
  }
}
class nt {
  constructor(t) {
    this.yi = { ki: 0, t: "#000", gi: 0, wi: 0 }, this.Ci = { Kt: "", yt: false, hi: true, mi: false, Ot: "", V: "#FFF", _i: false, bi: false }, this.Ti = { Kt: "", yt: false, hi: false, mi: true, Ot: "", V: "#FFF", _i: true, bi: true }, this.ft = true, this.Pi = new (t || it)(this.Ci, this.yi), this.Ri = new (t || it)(this.Ti, this.yi);
  }
  Kt() {
    return this.Di(), this.Ci.Kt;
  }
  ki() {
    return this.Di(), this.yi.ki;
  }
  bt() {
    this.ft = true;
  }
  At(t, i = false) {
    return Math.max(this.Pi.At(t, i), this.Ri.At(t, i));
  }
  Vi() {
    return this.yi.Si || 0;
  }
  Oi(t) {
    this.yi.Si = t;
  }
  Bi() {
    return this.Di(), this.Ci.yt || this.Ti.yt;
  }
  Ai() {
    return this.Di(), this.Ci.yt;
  }
  gt(t) {
    return this.Di(), this.Ci.hi = this.Ci.hi && t.W().ticksVisible, this.Ti.hi = this.Ti.hi && t.W().ticksVisible, this.Pi.J(this.Ci, this.yi), this.Ri.J(this.Ti, this.yi), this.Pi;
  }
  Ii() {
    return this.Di(), this.Pi.J(this.Ci, this.yi), this.Ri.J(this.Ti, this.yi), this.Ri;
  }
  Di() {
    this.ft && (this.Ci.hi = true, this.Ti.hi = false, this.zi(this.Ci, this.Ti, this.yi));
  }
}
class st extends nt {
  constructor(t, i, n) {
    super(), this.jt = t, this.Li = i, this.Ei = n;
  }
  zi(t, i, n) {
    if (t.yt = false, 2 === this.jt.W().mode) return;
    const s = this.jt.W().horzLine;
    if (!s.labelVisible) return;
    const e2 = this.Li.Ct();
    if (!this.jt.yt() || this.Li.Ni() || null === e2) return;
    const r2 = R(s.labelBackgroundColor);
    n.t = r2.t, t.V = r2.i;
    const h2 = 2 / 12 * this.Li.P();
    n.wi = h2, n.gi = h2;
    const l2 = this.Ei(this.Li);
    n.ki = l2.ki, t.Kt = this.Li.Fi(l2._t, e2), t.yt = true;
  }
}
const et = /[1-9]/g;
class rt {
  constructor() {
    this.zt = null;
  }
  J(t) {
    this.zt = t;
  }
  X(t, i) {
    if (null === this.zt || false === this.zt.yt || 0 === this.zt.Kt.length) return;
    const n = t.useMediaCoordinateSpace(({ context: t2 }) => (t2.font = i.R, Math.round(i.Wi.xi(t2, b(this.zt).Kt, et))));
    if (n <= 0) return;
    const s = i.ji, e2 = n + 2 * s, r2 = e2 / 2, h2 = this.zt.Hi;
    let l2 = this.zt.ki, a2 = Math.floor(l2 - r2) + 0.5;
    a2 < 0 ? (l2 += Math.abs(0 - a2), a2 = Math.floor(l2 - r2) + 0.5) : a2 + e2 > h2 && (l2 -= Math.abs(h2 - (a2 + e2)), a2 = Math.floor(l2 - r2) + 0.5);
    const o2 = a2 + e2, _2 = Math.ceil(0 + i.C + i.T + i.L + i.P + i.B);
    t.useBitmapCoordinateSpace(({ context: t2, horizontalPixelRatio: n2, verticalPixelRatio: s2 }) => {
      const e3 = b(this.zt);
      t2.fillStyle = e3.t;
      const r3 = Math.round(a2 * n2), h3 = Math.round(0 * s2), l3 = Math.round(o2 * n2), u2 = Math.round(_2 * s2), c2 = Math.round(2 * n2);
      if (t2.beginPath(), t2.moveTo(r3, h3), t2.lineTo(r3, u2 - c2), t2.arcTo(r3, u2, r3 + c2, u2, c2), t2.lineTo(l3 - c2, u2), t2.arcTo(l3, u2, l3, u2 - c2, c2), t2.lineTo(l3, h3), t2.fill(), e3.hi) {
        const r4 = Math.round(e3.ki * n2), l4 = h3, a3 = Math.round((l4 + i.T) * s2);
        t2.fillStyle = e3.V;
        const o3 = Math.max(1, Math.floor(n2)), _3 = Math.floor(0.5 * n2);
        t2.fillRect(r4 - _3, l4, o3, a3 - l4);
      }
    }), t.useMediaCoordinateSpace(({ context: t2 }) => {
      const n2 = b(this.zt), e3 = 0 + i.C + i.T + i.L + i.P / 2;
      t2.font = i.R, t2.textAlign = "left", t2.textBaseline = "middle", t2.fillStyle = n2.V;
      const r3 = i.Wi.Mi(t2, "Apr0");
      t2.translate(a2 + s, e3 + r3), t2.fillText(n2.Kt, 0, 0);
    });
  }
}
class ht {
  constructor(t, i, n) {
    this.ft = true, this.Wt = new rt(), this.Ft = { yt: false, t: "#4c525e", V: "white", Kt: "", Hi: 0, ki: NaN, hi: true }, this.vt = t, this.$i = i, this.Ei = n;
  }
  bt() {
    this.ft = true;
  }
  gt() {
    return this.ft && (this.Mt(), this.ft = false), this.Wt.J(this.Ft), this.Wt;
  }
  Mt() {
    const t = this.Ft;
    if (t.yt = false, 2 === this.vt.W().mode) return;
    const i = this.vt.W().vertLine;
    if (!i.labelVisible) return;
    const n = this.$i.St();
    if (n.Ni()) return;
    t.Hi = n.Hi();
    const s = this.Ei();
    if (null === s) return;
    t.ki = s.ki;
    const e2 = n.Ui(this.vt.xt());
    t.Kt = n.qi(b(e2)), t.yt = true;
    const r2 = R(i.labelBackgroundColor);
    t.t = r2.t, t.V = r2.i, t.hi = n.W().ticksVisible;
  }
}
class lt {
  constructor() {
    this.Yi = null, this.Zi = 0;
  }
  Xi() {
    return this.Zi;
  }
  Ki(t) {
    this.Zi = t;
  }
  Dt() {
    return this.Yi;
  }
  Gi(t) {
    this.Yi = t;
  }
  Ji(t) {
    return [];
  }
  Qi() {
    return [];
  }
  yt() {
    return true;
  }
}
var at;
!function(t) {
  t[t.Normal = 0] = "Normal", t[t.Magnet = 1] = "Magnet", t[t.Hidden = 2] = "Hidden";
}(at || (at = {}));
class ot extends lt {
  constructor(t, i) {
    super(), this.tn = null, this.nn = NaN, this.sn = 0, this.en = true, this.rn = /* @__PURE__ */ new Map(), this.hn = false, this.ln = NaN, this.an = NaN, this._n = NaN, this.un = NaN, this.$i = t, this.cn = i, this.dn = new Y(t, this);
    this.fn = /* @__PURE__ */ ((t2, i2) => (n2) => {
      const s = i2(), e2 = t2();
      if (n2 === b(this.tn).vn()) return { _t: e2, ki: s };
      {
        const t3 = b(n2.Ct());
        return { _t: n2.pn(s, t3), ki: s };
      }
    })(() => this.nn, () => this.an);
    const n = /* @__PURE__ */ ((t2, i2) => () => {
      const n2 = this.$i.St().mn(t2()), s = i2();
      return n2 && Number.isFinite(s) ? { ot: n2, ki: s } : null;
    })(() => this.sn, () => this.Yt());
    this.bn = new ht(this, t, n), this.wn = new X(this);
  }
  W() {
    return this.cn;
  }
  gn(t, i) {
    this._n = t, this.un = i;
  }
  Mn() {
    this._n = NaN, this.un = NaN;
  }
  xn() {
    return this._n;
  }
  Sn() {
    return this.un;
  }
  kn(t, i, n) {
    this.hn || (this.hn = true), this.en = true, this.yn(t, i, n);
  }
  xt() {
    return this.sn;
  }
  Yt() {
    return this.ln;
  }
  Zt() {
    return this.an;
  }
  yt() {
    return this.en;
  }
  Cn() {
    this.en = false, this.Tn(), this.nn = NaN, this.ln = NaN, this.an = NaN, this.tn = null, this.Mn();
  }
  Pn(t) {
    return null !== this.tn ? [this.wn, this.dn] : [];
  }
  Ut(t) {
    return t === this.tn && this.cn.horzLine.visible;
  }
  qt() {
    return this.cn.vertLine.visible;
  }
  Rn(t, i) {
    this.en && this.tn === t || this.rn.clear();
    const n = [];
    return this.tn === t && n.push(this.Dn(this.rn, i, this.fn)), n;
  }
  Qi() {
    return this.en ? [this.bn] : [];
  }
  Ht() {
    return this.tn;
  }
  Vn() {
    this.wn.bt(), this.rn.forEach((t) => t.bt()), this.bn.bt(), this.dn.bt();
  }
  On(t) {
    return t && !t.vn().Ni() ? t.vn() : null;
  }
  yn(t, i, n) {
    this.Bn(t, i, n) && this.Vn();
  }
  Bn(t, i, n) {
    const s = this.ln, e2 = this.an, r2 = this.nn, h2 = this.sn, l2 = this.tn, a2 = this.On(n);
    this.sn = t, this.ln = isNaN(t) ? NaN : this.$i.St().It(t), this.tn = n;
    const o2 = null !== a2 ? a2.Ct() : null;
    return null !== a2 && null !== o2 ? (this.nn = i, this.an = a2.Rt(i, o2)) : (this.nn = NaN, this.an = NaN), s !== this.ln || e2 !== this.an || h2 !== this.sn || r2 !== this.nn || l2 !== this.tn;
  }
  Tn() {
    const t = this.$i.wt().map((t2) => t2.In().An()).filter(L), i = 0 === t.length ? null : Math.max(...t);
    this.sn = null !== i ? i : NaN;
  }
  Dn(t, i, n) {
    let s = t.get(i);
    return void 0 === s && (s = new st(this, i, n), t.set(i, s)), s;
  }
}
function _t(t) {
  return "left" === t || "right" === t;
}
class ut {
  constructor(t) {
    this.zn = /* @__PURE__ */ new Map(), this.Ln = [], this.En = t;
  }
  Nn(t, i) {
    const n = function(t2, i2) {
      return void 0 === t2 ? i2 : { Fn: Math.max(t2.Fn, i2.Fn), Wn: t2.Wn || i2.Wn };
    }(this.zn.get(t), i);
    this.zn.set(t, n);
  }
  jn() {
    return this.En;
  }
  Hn(t) {
    const i = this.zn.get(t);
    return void 0 === i ? { Fn: this.En } : { Fn: Math.max(this.En, i.Fn), Wn: i.Wn };
  }
  $n() {
    this.Un(), this.Ln = [{ qn: 0 }];
  }
  Yn(t) {
    this.Un(), this.Ln = [{ qn: 1, Vt: t }];
  }
  Zn(t) {
    this.Xn(), this.Ln.push({ qn: 5, Vt: t });
  }
  Un() {
    this.Xn(), this.Ln.push({ qn: 6 });
  }
  Kn() {
    this.Un(), this.Ln = [{ qn: 4 }];
  }
  Gn(t) {
    this.Un(), this.Ln.push({ qn: 2, Vt: t });
  }
  Jn(t) {
    this.Un(), this.Ln.push({ qn: 3, Vt: t });
  }
  Qn() {
    return this.Ln;
  }
  ts(t) {
    for (const i of t.Ln) this.ns(i);
    this.En = Math.max(this.En, t.En), t.zn.forEach((t2, i) => {
      this.Nn(i, t2);
    });
  }
  static ss() {
    return new ut(2);
  }
  static es() {
    return new ut(3);
  }
  ns(t) {
    switch (t.qn) {
      case 0:
        this.$n();
        break;
      case 1:
        this.Yn(t.Vt);
        break;
      case 2:
        this.Gn(t.Vt);
        break;
      case 3:
        this.Jn(t.Vt);
        break;
      case 4:
        this.Kn();
        break;
      case 5:
        this.Zn(t.Vt);
        break;
      case 6:
        this.Xn();
    }
  }
  Xn() {
    const t = this.Ln.findIndex((t2) => 5 === t2.qn);
    -1 !== t && this.Ln.splice(t, 1);
  }
}
const ct = ".";
function dt(t, i) {
  if (!O(t)) return "n/a";
  if (!B(i)) throw new TypeError("invalid length");
  if (i < 0 || i > 16) throw new TypeError("invalid length");
  if (0 === i) return t.toString();
  return ("0000000000000000" + t.toString()).slice(-i);
}
class ft {
  constructor(t, i) {
    if (i || (i = 1), O(t) && B(t) || (t = 100), t < 0) throw new TypeError("invalid base");
    this.Li = t, this.rs = i, this.hs();
  }
  format(t) {
    const i = t < 0 ? "−" : "";
    return t = Math.abs(t), i + this.ls(t);
  }
  hs() {
    if (this._s = 0, this.Li > 0 && this.rs > 0) {
      let t = this.Li;
      for (; t > 1; ) t /= 10, this._s++;
    }
  }
  ls(t) {
    const i = this.Li / this.rs;
    let n = Math.floor(t), s = "";
    const e2 = void 0 !== this._s ? this._s : NaN;
    if (i > 1) {
      let r2 = +(Math.round(t * i) - n * i).toFixed(this._s);
      r2 >= i && (r2 -= i, n += 1), s = ct + dt(+r2.toFixed(this._s) * this.rs, e2);
    } else n = Math.round(n * i) / i, e2 > 0 && (s = ct + dt(0, e2));
    return n.toFixed(0) + s;
  }
}
class vt extends ft {
  constructor(t = 100) {
    super(t);
  }
  format(t) {
    return `${super.format(t)}%`;
  }
}
class pt {
  constructor(t) {
    this.us = t;
  }
  format(t) {
    let i = "";
    return t < 0 && (i = "-", t = -t), t < 995 ? i + this.cs(t) : t < 999995 ? i + this.cs(t / 1e3) + "K" : t < 999999995 ? (t = 1e3 * Math.round(t / 1e3), i + this.cs(t / 1e6) + "M") : (t = 1e6 * Math.round(t / 1e6), i + this.cs(t / 1e9) + "B");
  }
  cs(t) {
    let i;
    const n = Math.pow(10, this.us);
    return i = (t = Math.round(t * n) / n) >= 1e-15 && t < 1 ? t.toFixed(this.us).replace(/\.?0+$/, "") : String(t), i.replace(/(\.[1-9]*)0+$/, (t2, i2) => i2);
  }
}
function mt(t, i, n, s, e2, r2, h2) {
  if (0 === i.length || s.from >= i.length || s.to <= 0) return;
  const { context: l2, horizontalPixelRatio: a2, verticalPixelRatio: o2 } = t, _2 = i[s.from];
  let u2 = r2(t, _2), c2 = _2;
  if (s.to - s.from < 2) {
    const i2 = e2 / 2;
    l2.beginPath();
    const n2 = { nt: _2.nt - i2, st: _2.st }, s2 = { nt: _2.nt + i2, st: _2.st };
    l2.moveTo(n2.nt * a2, n2.st * o2), l2.lineTo(s2.nt * a2, s2.st * o2), h2(t, u2, n2, s2);
  } else {
    const e3 = (i2, n2) => {
      h2(t, u2, c2, n2), l2.beginPath(), u2 = i2, c2 = n2;
    };
    let d2 = c2;
    l2.beginPath(), l2.moveTo(_2.nt * a2, _2.st * o2);
    for (let h3 = s.from + 1; h3 < s.to; ++h3) {
      d2 = i[h3];
      const s2 = r2(t, d2);
      switch (n) {
        case 0:
          l2.lineTo(d2.nt * a2, d2.st * o2);
          break;
        case 1:
          l2.lineTo(d2.nt * a2, i[h3 - 1].st * o2), s2 !== u2 && (e3(s2, d2), l2.lineTo(d2.nt * a2, i[h3 - 1].st * o2)), l2.lineTo(d2.nt * a2, d2.st * o2);
          break;
        case 2: {
          const [t2, n2] = Mt(i, h3 - 1, h3);
          l2.bezierCurveTo(t2.nt * a2, t2.st * o2, n2.nt * a2, n2.st * o2, d2.nt * a2, d2.st * o2);
          break;
        }
      }
      1 !== n && s2 !== u2 && (e3(s2, d2), l2.moveTo(d2.nt * a2, d2.st * o2));
    }
    (c2 !== d2 || c2 === d2 && 1 === n) && h2(t, u2, c2, d2);
  }
}
const bt = 6;
function wt(t, i) {
  return { nt: t.nt - i.nt, st: t.st - i.st };
}
function gt(t, i) {
  return { nt: t.nt / i, st: t.st / i };
}
function Mt(t, i, n) {
  const s = Math.max(0, i - 1), e2 = Math.min(t.length - 1, n + 1);
  var r2, h2;
  return [(r2 = t[i], h2 = gt(wt(t[n], t[s]), bt), { nt: r2.nt + h2.nt, st: r2.st + h2.st }), wt(t[n], gt(wt(t[e2], t[i]), bt))];
}
function xt(t, i, n, s, e2) {
  const { context: r2, horizontalPixelRatio: h2, verticalPixelRatio: l2 } = i;
  r2.lineTo(e2.nt * h2, t * l2), r2.lineTo(s.nt * h2, t * l2), r2.closePath(), r2.fillStyle = n, r2.fill();
}
class St extends H {
  constructor() {
    super(...arguments), this.G = null;
  }
  J(t) {
    this.G = t;
  }
  K(t) {
    var i;
    if (null === this.G) return;
    const { it: n, tt: s, ds: e2, et: r2, Nt: h2, fs: l2 } = this.G, a2 = null !== (i = this.G.vs) && void 0 !== i ? i : this.G.ps ? 0 : t.mediaSize.height;
    if (null === s) return;
    const o2 = t.context;
    o2.lineCap = "butt", o2.lineJoin = "round", o2.lineWidth = r2, f(o2, h2), o2.lineWidth = 1, mt(t, n, l2, s, e2, this.bs.bind(this), xt.bind(null, a2));
  }
}
function kt(t, i, n) {
  return Math.min(Math.max(t, i), n);
}
function yt(t, i, n) {
  return i - t <= n;
}
function Ct(t) {
  const i = Math.ceil(t);
  return i % 2 == 0 ? i - 1 : i;
}
class Tt {
  ws(t, i) {
    const n = this.gs, { Ms: s, xs: e2, Ss: r2, ks: h2, ys: l2, vs: a2 } = i;
    if (void 0 === this.Cs || void 0 === n || n.Ms !== s || n.xs !== e2 || n.Ss !== r2 || n.ks !== h2 || n.vs !== a2 || n.ys !== l2) {
      const n2 = t.context.createLinearGradient(0, 0, 0, l2);
      if (n2.addColorStop(0, s), null != a2) {
        const i2 = kt(a2 * t.verticalPixelRatio / l2, 0, 1);
        n2.addColorStop(i2, e2), n2.addColorStop(i2, r2);
      }
      n2.addColorStop(1, h2), this.Cs = n2, this.gs = i;
    }
    return this.Cs;
  }
}
class Pt extends St {
  constructor() {
    super(...arguments), this.Ts = new Tt();
  }
  bs(t, i) {
    return this.Ts.ws(t, { Ms: i.Ps, xs: "", Ss: "", ks: i.Rs, ys: t.bitmapSize.height });
  }
}
function Rt(t, i) {
  const n = t.context;
  n.strokeStyle = i, n.stroke();
}
class Dt extends H {
  constructor() {
    super(...arguments), this.G = null;
  }
  J(t) {
    this.G = t;
  }
  K(t) {
    if (null === this.G) return;
    const { it: i, tt: n, ds: s, fs: e2, et: r2, Nt: h2, Ds: l2 } = this.G;
    if (null === n) return;
    const a2 = t.context;
    a2.lineCap = "butt", a2.lineWidth = r2 * t.verticalPixelRatio, f(a2, h2), a2.lineJoin = "round";
    const o2 = this.Vs.bind(this);
    void 0 !== e2 && mt(t, i, e2, n, s, o2, Rt), l2 && function(t2, i2, n2, s2, e3) {
      const { horizontalPixelRatio: r3, verticalPixelRatio: h3, context: l3 } = t2;
      let a3 = null;
      const o3 = Math.max(1, Math.floor(r3)) % 2 / 2, _2 = n2 * h3 + o3;
      for (let n3 = s2.to - 1; n3 >= s2.from; --n3) {
        const s3 = i2[n3];
        if (s3) {
          const i3 = e3(t2, s3);
          i3 !== a3 && (l3.beginPath(), null !== a3 && l3.fill(), l3.fillStyle = i3, a3 = i3);
          const n4 = Math.round(s3.nt * r3) + o3, u2 = s3.st * h3;
          l3.moveTo(n4, u2), l3.arc(n4, u2, _2, 0, 2 * Math.PI);
        }
      }
      l3.fill();
    }(t, i, l2, n, o2);
  }
}
class Vt extends Dt {
  Vs(t, i) {
    return i.lt;
  }
}
function Ot(t, i, n, s, e2 = 0, r2 = i.length) {
  let h2 = r2 - e2;
  for (; 0 < h2; ) {
    const r3 = h2 >> 1, l2 = e2 + r3;
    s(i[l2], n) === t ? (e2 = l2 + 1, h2 -= r3 + 1) : h2 = r3;
  }
  return e2;
}
const Bt = Ot.bind(null, true), At = Ot.bind(null, false);
function It(t, i) {
  return t.ot < i;
}
function zt(t, i) {
  return i < t.ot;
}
function Lt(t, i, n) {
  const s = i.Os(), e2 = i.ui(), r2 = Bt(t, s, It), h2 = At(t, e2, zt);
  if (!n) return { from: r2, to: h2 };
  let l2 = r2, a2 = h2;
  return r2 > 0 && r2 < t.length && t[r2].ot >= s && (l2 = r2 - 1), h2 > 0 && h2 < t.length && t[h2 - 1].ot <= e2 && (a2 = h2 + 1), { from: l2, to: a2 };
}
class Et {
  constructor(t, i, n) {
    this.Bs = true, this.As = true, this.Is = true, this.zs = [], this.Ls = null, this.Es = t, this.Ns = i, this.Fs = n;
  }
  bt(t) {
    this.Bs = true, "data" === t && (this.As = true), "options" === t && (this.Is = true);
  }
  gt() {
    return this.Es.yt() ? (this.Ws(), null === this.Ls ? null : this.js) : null;
  }
  Hs() {
    this.zs = this.zs.map((t) => Object.assign(Object.assign({}, t), this.Es.Us().$s(t.ot)));
  }
  qs() {
    this.Ls = null;
  }
  Ws() {
    this.As && (this.Ys(), this.As = false), this.Is && (this.Hs(), this.Is = false), this.Bs && (this.Zs(), this.Bs = false);
  }
  Zs() {
    const t = this.Es.Dt(), i = this.Ns.St();
    if (this.qs(), i.Ni() || t.Ni()) return;
    const n = i.Xs();
    if (null === n) return;
    if (0 === this.Es.In().Ks()) return;
    const s = this.Es.Ct();
    null !== s && (this.Ls = Lt(this.zs, n, this.Fs), this.Gs(t, i, s.Vt), this.Js());
  }
}
class Nt extends Et {
  constructor(t, i) {
    super(t, i, true);
  }
  Gs(t, i, n) {
    i.Qs(this.zs, E(this.Ls)), t.te(this.zs, n, E(this.Ls));
  }
  ie(t, i) {
    return { ot: t, _t: i, nt: NaN, st: NaN };
  }
  Ys() {
    const t = this.Es.Us();
    this.zs = this.Es.In().ne().map((i) => {
      const n = i.Vt[3];
      return this.se(i.ee, n, t);
    });
  }
}
class Ft extends Nt {
  constructor(t, i) {
    super(t, i), this.js = new j(), this.re = new Pt(), this.he = new Vt(), this.js.Z([this.re, this.he]);
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.ie(t, i)), n.$s(t));
  }
  Js() {
    const t = this.Es.W();
    this.re.J({ fs: t.lineType, it: this.zs, Nt: t.lineStyle, et: t.lineWidth, vs: null, ps: t.invertFilledArea, tt: this.Ls, ds: this.Ns.St().le() }), this.he.J({ fs: t.lineVisible ? t.lineType : void 0, it: this.zs, Nt: t.lineStyle, et: t.lineWidth, tt: this.Ls, ds: this.Ns.St().le(), Ds: t.pointMarkersVisible ? t.pointMarkersRadius || t.lineWidth / 2 + 2 : void 0 });
  }
}
class Wt extends H {
  constructor() {
    super(...arguments), this.zt = null, this.ae = 0, this.oe = 0;
  }
  J(t) {
    this.zt = t;
  }
  K({ context: t, horizontalPixelRatio: i, verticalPixelRatio: n }) {
    if (null === this.zt || 0 === this.zt.In.length || null === this.zt.tt) return;
    if (this.ae = this._e(i), this.ae >= 2) {
      Math.max(1, Math.floor(i)) % 2 != this.ae % 2 && this.ae--;
    }
    this.oe = this.zt.ue ? Math.min(this.ae, Math.floor(i)) : this.ae;
    let s = null;
    const e2 = this.oe <= this.ae && this.zt.le >= Math.floor(1.5 * i);
    for (let r2 = this.zt.tt.from; r2 < this.zt.tt.to; ++r2) {
      const h2 = this.zt.In[r2];
      s !== h2.ce && (t.fillStyle = h2.ce, s = h2.ce);
      const l2 = Math.floor(0.5 * this.oe), a2 = Math.round(h2.nt * i), o2 = a2 - l2, _2 = this.oe, u2 = o2 + _2 - 1, c2 = Math.min(h2.de, h2.fe), d2 = Math.max(h2.de, h2.fe), f2 = Math.round(c2 * n) - l2, v2 = Math.round(d2 * n) + l2, p2 = Math.max(v2 - f2, this.oe);
      t.fillRect(o2, f2, _2, p2);
      const m2 = Math.ceil(1.5 * this.ae);
      if (e2) {
        if (this.zt.ve) {
          const i3 = a2 - m2;
          let s3 = Math.max(f2, Math.round(h2.pe * n) - l2), e4 = s3 + _2 - 1;
          e4 > f2 + p2 - 1 && (e4 = f2 + p2 - 1, s3 = e4 - _2 + 1), t.fillRect(i3, s3, o2 - i3, e4 - s3 + 1);
        }
        const i2 = a2 + m2;
        let s2 = Math.max(f2, Math.round(h2.me * n) - l2), e3 = s2 + _2 - 1;
        e3 > f2 + p2 - 1 && (e3 = f2 + p2 - 1, s2 = e3 - _2 + 1), t.fillRect(u2 + 1, s2, i2 - u2, e3 - s2 + 1);
      }
    }
  }
  _e(t) {
    const i = Math.floor(t);
    return Math.max(i, Math.floor(function(t2, i2) {
      return Math.floor(0.3 * t2 * i2);
    }(b(this.zt).le, t)));
  }
}
class jt extends Et {
  constructor(t, i) {
    super(t, i, false);
  }
  Gs(t, i, n) {
    i.Qs(this.zs, E(this.Ls)), t.be(this.zs, n, E(this.Ls));
  }
  we(t, i, n) {
    return { ot: t, ge: i.Vt[0], Me: i.Vt[1], xe: i.Vt[2], Se: i.Vt[3], nt: NaN, pe: NaN, de: NaN, fe: NaN, me: NaN };
  }
  Ys() {
    const t = this.Es.Us();
    this.zs = this.Es.In().ne().map((i) => this.se(i.ee, i, t));
  }
}
class Ht extends jt {
  constructor() {
    super(...arguments), this.js = new Wt();
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.we(t, i, n)), n.$s(t));
  }
  Js() {
    const t = this.Es.W();
    this.js.J({ In: this.zs, le: this.Ns.St().le(), ve: t.openVisible, ue: t.thinBars, tt: this.Ls });
  }
}
class $t extends St {
  constructor() {
    super(...arguments), this.Ts = new Tt();
  }
  bs(t, i) {
    const n = this.G;
    return this.Ts.ws(t, { Ms: i.ke, xs: i.ye, Ss: i.Ce, ks: i.Te, ys: t.bitmapSize.height, vs: n.vs });
  }
}
class Ut extends Dt {
  constructor() {
    super(...arguments), this.Pe = new Tt();
  }
  Vs(t, i) {
    const n = this.G;
    return this.Pe.ws(t, { Ms: i.Re, xs: i.Re, Ss: i.De, ks: i.De, ys: t.bitmapSize.height, vs: n.vs });
  }
}
class qt extends Nt {
  constructor(t, i) {
    super(t, i), this.js = new j(), this.Ve = new $t(), this.Oe = new Ut(), this.js.Z([this.Ve, this.Oe]);
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.ie(t, i)), n.$s(t));
  }
  Js() {
    const t = this.Es.Ct();
    if (null === t) return;
    const i = this.Es.W(), n = this.Es.Dt().Rt(i.baseValue.price, t.Vt), s = this.Ns.St().le();
    this.Ve.J({ it: this.zs, et: i.lineWidth, Nt: i.lineStyle, fs: i.lineType, vs: n, ps: false, tt: this.Ls, ds: s }), this.Oe.J({ it: this.zs, et: i.lineWidth, Nt: i.lineStyle, fs: i.lineVisible ? i.lineType : void 0, Ds: i.pointMarkersVisible ? i.pointMarkersRadius || i.lineWidth / 2 + 2 : void 0, vs: n, tt: this.Ls, ds: s });
  }
}
class Yt extends H {
  constructor() {
    super(...arguments), this.zt = null, this.ae = 0;
  }
  J(t) {
    this.zt = t;
  }
  K(t) {
    if (null === this.zt || 0 === this.zt.In.length || null === this.zt.tt) return;
    const { horizontalPixelRatio: i } = t;
    if (this.ae = function(t2, i2) {
      if (t2 >= 2.5 && t2 <= 4) return Math.floor(3 * i2);
      const n2 = 1 - 0.2 * Math.atan(Math.max(4, t2) - 4) / (0.5 * Math.PI), s2 = Math.floor(t2 * n2 * i2), e2 = Math.floor(t2 * i2), r2 = Math.min(s2, e2);
      return Math.max(Math.floor(i2), r2);
    }(this.zt.le, i), this.ae >= 2) {
      Math.floor(i) % 2 != this.ae % 2 && this.ae--;
    }
    const n = this.zt.In;
    this.zt.Be && this.Ae(t, n, this.zt.tt), this.zt._i && this.Ie(t, n, this.zt.tt);
    const s = this.ze(i);
    (!this.zt._i || this.ae > 2 * s) && this.Le(t, n, this.zt.tt);
  }
  Ae(t, i, n) {
    if (null === this.zt) return;
    const { context: s, horizontalPixelRatio: e2, verticalPixelRatio: r2 } = t;
    let h2 = "", l2 = Math.min(Math.floor(e2), Math.floor(this.zt.le * e2));
    l2 = Math.max(Math.floor(e2), Math.min(l2, this.ae));
    const a2 = Math.floor(0.5 * l2);
    let o2 = null;
    for (let t2 = n.from; t2 < n.to; t2++) {
      const n2 = i[t2];
      n2.Ee !== h2 && (s.fillStyle = n2.Ee, h2 = n2.Ee);
      const _2 = Math.round(Math.min(n2.pe, n2.me) * r2), u2 = Math.round(Math.max(n2.pe, n2.me) * r2), c2 = Math.round(n2.de * r2), d2 = Math.round(n2.fe * r2);
      let f2 = Math.round(e2 * n2.nt) - a2;
      const v2 = f2 + l2 - 1;
      null !== o2 && (f2 = Math.max(o2 + 1, f2), f2 = Math.min(f2, v2));
      const p2 = v2 - f2 + 1;
      s.fillRect(f2, c2, p2, _2 - c2), s.fillRect(f2, u2 + 1, p2, d2 - u2), o2 = v2;
    }
  }
  ze(t) {
    let i = Math.floor(1 * t);
    this.ae <= 2 * i && (i = Math.floor(0.5 * (this.ae - 1)));
    const n = Math.max(Math.floor(t), i);
    return this.ae <= 2 * n ? Math.max(Math.floor(t), Math.floor(1 * t)) : n;
  }
  Ie(t, i, n) {
    if (null === this.zt) return;
    const { context: s, horizontalPixelRatio: e2, verticalPixelRatio: r2 } = t;
    let h2 = "";
    const l2 = this.ze(e2);
    let a2 = null;
    for (let t2 = n.from; t2 < n.to; t2++) {
      const n2 = i[t2];
      n2.Ne !== h2 && (s.fillStyle = n2.Ne, h2 = n2.Ne);
      let o2 = Math.round(n2.nt * e2) - Math.floor(0.5 * this.ae);
      const _2 = o2 + this.ae - 1, u2 = Math.round(Math.min(n2.pe, n2.me) * r2), c2 = Math.round(Math.max(n2.pe, n2.me) * r2);
      if (null !== a2 && (o2 = Math.max(a2 + 1, o2), o2 = Math.min(o2, _2)), this.zt.le * e2 > 2 * l2) K(s, o2, u2, _2 - o2 + 1, c2 - u2 + 1, l2);
      else {
        const t3 = _2 - o2 + 1;
        s.fillRect(o2, u2, t3, c2 - u2 + 1);
      }
      a2 = _2;
    }
  }
  Le(t, i, n) {
    if (null === this.zt) return;
    const { context: s, horizontalPixelRatio: e2, verticalPixelRatio: r2 } = t;
    let h2 = "";
    const l2 = this.ze(e2);
    for (let t2 = n.from; t2 < n.to; t2++) {
      const n2 = i[t2];
      let a2 = Math.round(Math.min(n2.pe, n2.me) * r2), o2 = Math.round(Math.max(n2.pe, n2.me) * r2), _2 = Math.round(n2.nt * e2) - Math.floor(0.5 * this.ae), u2 = _2 + this.ae - 1;
      if (n2.ce !== h2) {
        const t3 = n2.ce;
        s.fillStyle = t3, h2 = t3;
      }
      this.zt._i && (_2 += l2, a2 += l2, u2 -= l2, o2 -= l2), a2 > o2 || s.fillRect(_2, a2, u2 - _2 + 1, o2 - a2 + 1);
    }
  }
}
class Zt extends jt {
  constructor() {
    super(...arguments), this.js = new Yt();
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.we(t, i, n)), n.$s(t));
  }
  Js() {
    const t = this.Es.W();
    this.js.J({ In: this.zs, le: this.Ns.St().le(), Be: t.wickVisible, _i: t.borderVisible, tt: this.Ls });
  }
}
class Xt {
  constructor(t, i) {
    this.Fe = t, this.Li = i;
  }
  X(t, i, n) {
    this.Fe.draw(t, this.Li, i, n);
  }
}
class Kt extends Et {
  constructor(t, i, n) {
    super(t, i, false), this.wn = n, this.js = new Xt(this.wn.renderer(), (i2) => {
      const n2 = t.Ct();
      return null === n2 ? null : t.Dt().Rt(i2, n2.Vt);
    });
  }
  We(t) {
    return this.wn.priceValueBuilder(t);
  }
  je(t) {
    return this.wn.isWhitespace(t);
  }
  Ys() {
    const t = this.Es.Us();
    this.zs = this.Es.In().ne().map((i) => Object.assign(Object.assign({ ot: i.ee, nt: NaN }, t.$s(i.ee)), { He: i.$e }));
  }
  Gs(t, i) {
    i.Qs(this.zs, E(this.Ls));
  }
  Js() {
    this.wn.update({ bars: this.zs.map(Gt), barSpacing: this.Ns.St().le(), visibleRange: this.Ls }, this.Es.W());
  }
}
function Gt(t) {
  return { x: t.nt, time: t.ot, originalData: t.He, barColor: t.ce };
}
class Jt extends H {
  constructor() {
    super(...arguments), this.zt = null, this.Ue = [];
  }
  J(t) {
    this.zt = t, this.Ue = [];
  }
  K({ context: t, horizontalPixelRatio: i, verticalPixelRatio: n }) {
    if (null === this.zt || 0 === this.zt.it.length || null === this.zt.tt) return;
    this.Ue.length || this.qe(i);
    const s = Math.max(1, Math.floor(n)), e2 = Math.round(this.zt.Ye * n) - Math.floor(s / 2), r2 = e2 + s;
    for (let i2 = this.zt.tt.from; i2 < this.zt.tt.to; i2++) {
      const h2 = this.zt.it[i2], l2 = this.Ue[i2 - this.zt.tt.from], a2 = Math.round(h2.st * n);
      let o2, _2;
      t.fillStyle = h2.ce, a2 <= e2 ? (o2 = a2, _2 = r2) : (o2 = e2, _2 = a2 - Math.floor(s / 2) + s), t.fillRect(l2.Os, o2, l2.ui - l2.Os + 1, _2 - o2);
    }
  }
  qe(t) {
    if (null === this.zt || 0 === this.zt.it.length || null === this.zt.tt) return void (this.Ue = []);
    const i = Math.ceil(this.zt.le * t) <= 1 ? 0 : Math.max(1, Math.floor(t)), n = Math.round(this.zt.le * t) - i;
    this.Ue = new Array(this.zt.tt.to - this.zt.tt.from);
    for (let i2 = this.zt.tt.from; i2 < this.zt.tt.to; i2++) {
      const s2 = this.zt.it[i2], e2 = Math.round(s2.nt * t);
      let r2, h2;
      if (n % 2) {
        const t2 = (n - 1) / 2;
        r2 = e2 - t2, h2 = e2 + t2;
      } else {
        const t2 = n / 2;
        r2 = e2 - t2, h2 = e2 + t2 - 1;
      }
      this.Ue[i2 - this.zt.tt.from] = { Os: r2, ui: h2, Ze: e2, Xe: s2.nt * t, ot: s2.ot };
    }
    for (let t2 = this.zt.tt.from + 1; t2 < this.zt.tt.to; t2++) {
      const n2 = this.Ue[t2 - this.zt.tt.from], s2 = this.Ue[t2 - this.zt.tt.from - 1];
      n2.ot === s2.ot + 1 && (n2.Os - s2.ui !== i + 1 && (s2.Ze > s2.Xe ? s2.ui = n2.Os - i - 1 : n2.Os = s2.ui + i + 1));
    }
    let s = Math.ceil(this.zt.le * t);
    for (let t2 = this.zt.tt.from; t2 < this.zt.tt.to; t2++) {
      const i2 = this.Ue[t2 - this.zt.tt.from];
      i2.ui < i2.Os && (i2.ui = i2.Os);
      const n2 = i2.ui - i2.Os + 1;
      s = Math.min(n2, s);
    }
    if (i > 0 && s < 4) for (let t2 = this.zt.tt.from; t2 < this.zt.tt.to; t2++) {
      const i2 = this.Ue[t2 - this.zt.tt.from];
      i2.ui - i2.Os + 1 > s && (i2.Ze > i2.Xe ? i2.ui -= 1 : i2.Os += 1);
    }
  }
}
class Qt extends Nt {
  constructor() {
    super(...arguments), this.js = new Jt();
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.ie(t, i)), n.$s(t));
  }
  Js() {
    const t = { it: this.zs, le: this.Ns.St().le(), tt: this.Ls, Ye: this.Es.Dt().Rt(this.Es.W().base, b(this.Es.Ct()).Vt) };
    this.js.J(t);
  }
}
class ti extends Nt {
  constructor() {
    super(...arguments), this.js = new Vt();
  }
  se(t, i, n) {
    return Object.assign(Object.assign({}, this.ie(t, i)), n.$s(t));
  }
  Js() {
    const t = this.Es.W(), i = { it: this.zs, Nt: t.lineStyle, fs: t.lineVisible ? t.lineType : void 0, et: t.lineWidth, Ds: t.pointMarkersVisible ? t.pointMarkersRadius || t.lineWidth / 2 + 2 : void 0, tt: this.Ls, ds: this.Ns.St().le() };
    this.js.J(i);
  }
}
const ii = /[2-9]/g;
class ni {
  constructor(t = 50) {
    this.Ke = 0, this.Ge = 1, this.Je = 1, this.Qe = {}, this.tr = /* @__PURE__ */ new Map(), this.ir = t;
  }
  nr() {
    this.Ke = 0, this.tr.clear(), this.Ge = 1, this.Je = 1, this.Qe = {};
  }
  xi(t, i, n) {
    return this.sr(t, i, n).width;
  }
  Mi(t, i, n) {
    const s = this.sr(t, i, n);
    return ((s.actualBoundingBoxAscent || 0) - (s.actualBoundingBoxDescent || 0)) / 2;
  }
  sr(t, i, n) {
    const s = n || ii, e2 = String(i).replace(s, "0");
    if (this.tr.has(e2)) return m(this.tr.get(e2)).er;
    if (this.Ke === this.ir) {
      const t2 = this.Qe[this.Je];
      delete this.Qe[this.Je], this.tr.delete(t2), this.Je++, this.Ke--;
    }
    t.save(), t.textBaseline = "middle";
    const r2 = t.measureText(e2);
    return t.restore(), 0 === r2.width && i.length || (this.tr.set(e2, { er: r2, rr: this.Ge }), this.Qe[this.Ge] = e2, this.Ke++, this.Ge++), r2;
  }
}
class si {
  constructor(t) {
    this.hr = null, this.k = null, this.lr = "right", this.ar = t;
  }
  _r(t, i, n) {
    this.hr = t, this.k = i, this.lr = n;
  }
  X(t) {
    null !== this.k && null !== this.hr && this.hr.X(t, this.k, this.ar, this.lr);
  }
}
class ei {
  constructor(t, i, n) {
    this.ur = t, this.ar = new ni(50), this.cr = i, this.F = n, this.j = -1, this.Wt = new si(this.ar);
  }
  gt() {
    const t = this.F.dr(this.cr);
    if (null === t) return null;
    const i = t.vr(this.cr) ? t.pr() : this.cr.Dt();
    if (null === i) return null;
    const n = t.mr(i);
    if ("overlay" === n) return null;
    const s = this.F.br();
    return s.P !== this.j && (this.j = s.P, this.ar.nr()), this.Wt._r(this.ur.Ii(), s, n), this.Wt;
  }
}
class ri extends H {
  constructor() {
    super(...arguments), this.zt = null;
  }
  J(t) {
    this.zt = t;
  }
  wr(t, i) {
    var n;
    if (!(null === (n = this.zt) || void 0 === n ? void 0 : n.yt)) return null;
    const { st: s, et: e2, gr: r2 } = this.zt;
    return i >= s - e2 - 7 && i <= s + e2 + 7 ? { Mr: this.zt, gr: r2 } : null;
  }
  K({ context: t, bitmapSize: i, horizontalPixelRatio: n, verticalPixelRatio: s }) {
    if (null === this.zt) return;
    if (false === this.zt.yt) return;
    const e2 = Math.round(this.zt.st * s);
    e2 < 0 || e2 > i.height || (t.lineCap = "butt", t.strokeStyle = this.zt.V, t.lineWidth = Math.floor(this.zt.et * n), f(t, this.zt.Nt), v(t, e2, 0, i.width));
  }
}
class hi {
  constructor(t) {
    this.Sr = { st: 0, V: "rgba(0, 0, 0, 0)", et: 1, Nt: 0, yt: false }, this.kr = new ri(), this.ft = true, this.Es = t, this.Ns = t.$t(), this.kr.J(this.Sr);
  }
  bt() {
    this.ft = true;
  }
  gt() {
    return this.Es.yt() ? (this.ft && (this.yr(), this.ft = false), this.kr) : null;
  }
}
class li extends hi {
  constructor(t) {
    super(t);
  }
  yr() {
    this.Sr.yt = false;
    const t = this.Es.Dt(), i = t.Cr().Cr;
    if (2 !== i && 3 !== i) return;
    const n = this.Es.W();
    if (!n.baseLineVisible || !this.Es.yt()) return;
    const s = this.Es.Ct();
    null !== s && (this.Sr.yt = true, this.Sr.st = t.Rt(s.Vt, s.Vt), this.Sr.V = n.baseLineColor, this.Sr.et = n.baseLineWidth, this.Sr.Nt = n.baseLineStyle);
  }
}
class ai extends H {
  constructor() {
    super(...arguments), this.zt = null;
  }
  J(t) {
    this.zt = t;
  }
  $e() {
    return this.zt;
  }
  K({ context: t, horizontalPixelRatio: i, verticalPixelRatio: n }) {
    const s = this.zt;
    if (null === s) return;
    const e2 = Math.max(1, Math.floor(i)), r2 = e2 % 2 / 2, h2 = Math.round(s.Xe.x * i) + r2, l2 = s.Xe.y * n;
    t.fillStyle = s.Tr, t.beginPath();
    const a2 = Math.max(2, 1.5 * s.Pr) * i;
    t.arc(h2, l2, a2, 0, 2 * Math.PI, false), t.fill(), t.fillStyle = s.Rr, t.beginPath(), t.arc(h2, l2, s.ht * i, 0, 2 * Math.PI, false), t.fill(), t.lineWidth = e2, t.strokeStyle = s.Dr, t.beginPath(), t.arc(h2, l2, s.ht * i + e2 / 2, 0, 2 * Math.PI, false), t.stroke();
  }
}
const oi = [{ Vr: 0, Or: 0.25, Br: 4, Ar: 10, Ir: 0.25, zr: 0, Lr: 0.4, Er: 0.8 }, { Vr: 0.25, Or: 0.525, Br: 10, Ar: 14, Ir: 0, zr: 0, Lr: 0.8, Er: 0 }, { Vr: 0.525, Or: 1, Br: 14, Ar: 14, Ir: 0, zr: 0, Lr: 0, Er: 0 }];
function _i(t, i, n, s) {
  return function(t2, i2) {
    if ("transparent" === t2) return t2;
    const n2 = T(t2), s2 = n2[3];
    return `rgba(${n2[0]}, ${n2[1]}, ${n2[2]}, ${i2 * s2})`;
  }(t, n + (s - n) * i);
}
function ui(t, i) {
  const n = t % 2600 / 2600;
  let s;
  for (const t2 of oi) if (n >= t2.Vr && n <= t2.Or) {
    s = t2;
    break;
  }
  p(void 0 !== s, "Last price animation internal logic error");
  const e2 = (n - s.Vr) / (s.Or - s.Vr);
  return { Rr: _i(i, e2, s.Ir, s.zr), Dr: _i(i, e2, s.Lr, s.Er), ht: (r2 = e2, h2 = s.Br, l2 = s.Ar, h2 + (l2 - h2) * r2) };
  var r2, h2, l2;
}
class ci {
  constructor(t) {
    this.Wt = new ai(), this.ft = true, this.Nr = true, this.Fr = performance.now(), this.Wr = this.Fr - 1, this.jr = t;
  }
  Hr() {
    this.Wr = this.Fr - 1, this.bt();
  }
  $r() {
    if (this.bt(), 2 === this.jr.W().lastPriceAnimation) {
      const t = performance.now(), i = this.Wr - t;
      if (i > 0) return void (i < 650 && (this.Wr += 2600));
      this.Fr = t, this.Wr = t + 2600;
    }
  }
  bt() {
    this.ft = true;
  }
  Ur() {
    this.Nr = true;
  }
  yt() {
    return 0 !== this.jr.W().lastPriceAnimation;
  }
  qr() {
    switch (this.jr.W().lastPriceAnimation) {
      case 0:
        return false;
      case 1:
        return true;
      case 2:
        return performance.now() <= this.Wr;
    }
  }
  gt() {
    return this.ft ? (this.Mt(), this.ft = false, this.Nr = false) : this.Nr && (this.Yr(), this.Nr = false), this.Wt;
  }
  Mt() {
    this.Wt.J(null);
    const t = this.jr.$t().St(), i = t.Xs(), n = this.jr.Ct();
    if (null === i || null === n) return;
    const s = this.jr.Zr(true);
    if (s.Xr || !i.Kr(s.ee)) return;
    const e2 = { x: t.It(s.ee), y: this.jr.Dt().Rt(s._t, n.Vt) }, r2 = s.V, h2 = this.jr.W().lineWidth, l2 = ui(this.Gr(), r2);
    this.Wt.J({ Tr: r2, Pr: h2, Rr: l2.Rr, Dr: l2.Dr, ht: l2.ht, Xe: e2 });
  }
  Yr() {
    const t = this.Wt.$e();
    if (null !== t) {
      const i = ui(this.Gr(), t.Tr);
      t.Rr = i.Rr, t.Dr = i.Dr, t.ht = i.ht;
    }
  }
  Gr() {
    return this.qr() ? performance.now() - this.Fr : 2599;
  }
}
function di(t, i) {
  return Ct(Math.min(Math.max(t, 12), 30) * i);
}
function fi(t, i) {
  switch (t) {
    case "arrowDown":
    case "arrowUp":
      return di(i, 1);
    case "circle":
      return di(i, 0.8);
    case "square":
      return di(i, 0.7);
  }
}
function vi(t) {
  return function(t2) {
    const i = Math.ceil(t2);
    return i % 2 != 0 ? i - 1 : i;
  }(di(t, 1));
}
function pi(t) {
  return Math.max(di(t, 0.1), 3);
}
function mi(t, i, n) {
  return i ? t : n ? Math.ceil(t / 2) : 0;
}
function bi(t, i, n, s, e2) {
  const r2 = fi("square", n), h2 = (r2 - 1) / 2, l2 = t - h2, a2 = i - h2;
  return s >= l2 && s <= l2 + r2 && e2 >= a2 && e2 <= a2 + r2;
}
function wi(t, i, n, s) {
  const e2 = (fi("arrowUp", s) - 1) / 2 * n.Jr, r2 = (Ct(s / 2) - 1) / 2 * n.Jr;
  i.beginPath(), t ? (i.moveTo(n.nt - e2, n.st), i.lineTo(n.nt, n.st - e2), i.lineTo(n.nt + e2, n.st), i.lineTo(n.nt + r2, n.st), i.lineTo(n.nt + r2, n.st + e2), i.lineTo(n.nt - r2, n.st + e2), i.lineTo(n.nt - r2, n.st)) : (i.moveTo(n.nt - e2, n.st), i.lineTo(n.nt, n.st + e2), i.lineTo(n.nt + e2, n.st), i.lineTo(n.nt + r2, n.st), i.lineTo(n.nt + r2, n.st - e2), i.lineTo(n.nt - r2, n.st - e2), i.lineTo(n.nt - r2, n.st)), i.fill();
}
function gi(t, i, n, s, e2, r2) {
  return bi(i, n, s, e2, r2);
}
class Mi extends H {
  constructor() {
    super(...arguments), this.zt = null, this.ar = new ni(), this.j = -1, this.H = "", this.Qr = "";
  }
  J(t) {
    this.zt = t;
  }
  _r(t, i) {
    this.j === t && this.H === i || (this.j = t, this.H = i, this.Qr = F(t, i), this.ar.nr());
  }
  wr(t, i) {
    if (null === this.zt || null === this.zt.tt) return null;
    for (let n = this.zt.tt.from; n < this.zt.tt.to; n++) {
      const s = this.zt.it[n];
      if (Si(s, t, i)) return { Mr: s.th, gr: s.gr };
    }
    return null;
  }
  K({ context: t, horizontalPixelRatio: i, verticalPixelRatio: n }, s, e2) {
    if (null !== this.zt && null !== this.zt.tt) {
      t.textBaseline = "middle", t.font = this.Qr;
      for (let s2 = this.zt.tt.from; s2 < this.zt.tt.to; s2++) {
        const e3 = this.zt.it[s2];
        void 0 !== e3.Kt && (e3.Kt.Hi = this.ar.xi(t, e3.Kt.ih), e3.Kt.At = this.j, e3.Kt.nt = e3.nt - e3.Kt.Hi / 2), xi(e3, t, i, n);
      }
    }
  }
}
function xi(t, i, n, s) {
  i.fillStyle = t.V, void 0 !== t.Kt && function(t2, i2, n2, s2, e2, r2) {
    t2.save(), t2.scale(e2, r2), t2.fillText(i2, n2, s2), t2.restore();
  }(i, t.Kt.ih, t.Kt.nt, t.Kt.st, n, s), function(t2, i2, n2) {
    if (0 === t2.Ks) return;
    switch (t2.nh) {
      case "arrowDown":
        return void wi(false, i2, n2, t2.Ks);
      case "arrowUp":
        return void wi(true, i2, n2, t2.Ks);
      case "circle":
        return void function(t3, i3, n3) {
          const s2 = (fi("circle", n3) - 1) / 2;
          t3.beginPath(), t3.arc(i3.nt, i3.st, s2 * i3.Jr, 0, 2 * Math.PI, false), t3.fill();
        }(i2, n2, t2.Ks);
      case "square":
        return void function(t3, i3, n3) {
          const s2 = fi("square", n3), e2 = (s2 - 1) * i3.Jr / 2, r2 = i3.nt - e2, h2 = i3.st - e2;
          t3.fillRect(r2, h2, s2 * i3.Jr, s2 * i3.Jr);
        }(i2, n2, t2.Ks);
    }
    t2.nh;
  }(t, i, function(t2, i2, n2) {
    const s2 = Math.max(1, Math.floor(i2)) % 2 / 2;
    return { nt: Math.round(t2.nt * i2) + s2, st: t2.st * n2, Jr: i2 };
  }(t, n, s));
}
function Si(t, i, n) {
  return !(void 0 === t.Kt || !function(t2, i2, n2, s, e2, r2) {
    const h2 = s / 2;
    return e2 >= t2 && e2 <= t2 + n2 && r2 >= i2 - h2 && r2 <= i2 + h2;
  }(t.Kt.nt, t.Kt.st, t.Kt.Hi, t.Kt.At, i, n)) || function(t2, i2, n2) {
    if (0 === t2.Ks) return false;
    switch (t2.nh) {
      case "arrowDown":
      case "arrowUp":
        return gi(0, t2.nt, t2.st, t2.Ks, i2, n2);
      case "circle":
        return function(t3, i3, n3, s, e2) {
          const r2 = 2 + fi("circle", n3) / 2, h2 = t3 - s, l2 = i3 - e2;
          return Math.sqrt(h2 * h2 + l2 * l2) <= r2;
        }(t2.nt, t2.st, t2.Ks, i2, n2);
      case "square":
        return bi(t2.nt, t2.st, t2.Ks, i2, n2);
    }
  }(t, i, n);
}
function ki(t, i, n, s, e2, r2, h2, l2, a2) {
  const o2 = O(n) ? n : n.Se, _2 = O(n) ? n : n.Me, u2 = O(n) ? n : n.xe, c2 = O(i.size) ? Math.max(i.size, 0) : 1, d2 = vi(l2.le()) * c2, f2 = d2 / 2;
  switch (t.Ks = d2, i.position) {
    case "inBar":
      return t.st = h2.Rt(o2, a2), void (void 0 !== t.Kt && (t.Kt.st = t.st + f2 + r2 + 0.6 * e2));
    case "aboveBar":
      return t.st = h2.Rt(_2, a2) - f2 - s.sh, void 0 !== t.Kt && (t.Kt.st = t.st - f2 - 0.6 * e2, s.sh += 1.2 * e2), void (s.sh += d2 + r2);
    case "belowBar":
      return t.st = h2.Rt(u2, a2) + f2 + s.eh, void 0 !== t.Kt && (t.Kt.st = t.st + f2 + r2 + 0.6 * e2, s.eh += 1.2 * e2), void (s.eh += d2 + r2);
  }
  i.position;
}
class yi {
  constructor(t, i) {
    this.ft = true, this.rh = true, this.hh = true, this.ah = null, this.oh = null, this.Wt = new Mi(), this.jr = t, this.$i = i, this.zt = { it: [], tt: null };
  }
  bt(t) {
    this.ft = true, this.hh = true, "data" === t && (this.rh = true, this.oh = null);
  }
  gt(t) {
    if (!this.jr.yt()) return null;
    this.ft && this._h();
    const i = this.$i.W().layout;
    return this.Wt._r(i.fontSize, i.fontFamily), this.Wt.J(this.zt), this.Wt;
  }
  uh() {
    if (this.hh) {
      if (this.jr.dh().length > 0) {
        const t = this.$i.St().le(), i = pi(t), n = 1.5 * vi(t) + 2 * i, s = this.fh();
        this.ah = { above: mi(n, s.aboveBar, s.inBar), below: mi(n, s.belowBar, s.inBar) };
      } else this.ah = null;
      this.hh = false;
    }
    return this.ah;
  }
  fh() {
    return null === this.oh && (this.oh = this.jr.dh().reduce((t, i) => (t[i.position] || (t[i.position] = true), t), { inBar: false, aboveBar: false, belowBar: false })), this.oh;
  }
  _h() {
    const t = this.jr.Dt(), i = this.$i.St(), n = this.jr.dh();
    this.rh && (this.zt.it = n.map((t2) => ({ ot: t2.time, nt: 0, st: 0, Ks: 0, nh: t2.shape, V: t2.color, th: t2.th, gr: t2.id, Kt: void 0 })), this.rh = false);
    const s = this.$i.W().layout;
    this.zt.tt = null;
    const e2 = i.Xs();
    if (null === e2) return;
    const r2 = this.jr.Ct();
    if (null === r2) return;
    if (0 === this.zt.it.length) return;
    let h2 = NaN;
    const l2 = pi(i.le()), a2 = { sh: l2, eh: l2 };
    this.zt.tt = Lt(this.zt.it, e2, true);
    for (let e3 = this.zt.tt.from; e3 < this.zt.tt.to; e3++) {
      const o2 = n[e3];
      o2.time !== h2 && (a2.sh = l2, a2.eh = l2, h2 = o2.time);
      const _2 = this.zt.it[e3];
      _2.nt = i.It(o2.time), void 0 !== o2.text && o2.text.length > 0 && (_2.Kt = { ih: o2.text, nt: 0, st: 0, Hi: 0, At: 0 });
      const u2 = this.jr.ph(o2.time);
      null !== u2 && ki(_2, o2, u2, a2, s.fontSize, l2, t, i, r2.Vt);
    }
    this.ft = false;
  }
}
class Ci extends hi {
  constructor(t) {
    super(t);
  }
  yr() {
    const t = this.Sr;
    t.yt = false;
    const i = this.Es.W();
    if (!i.priceLineVisible || !this.Es.yt()) return;
    const n = this.Es.Zr(0 === i.priceLineSource);
    n.Xr || (t.yt = true, t.st = n.ki, t.V = this.Es.mh(n.V), t.et = i.priceLineWidth, t.Nt = i.priceLineStyle);
  }
}
class Ti extends nt {
  constructor(t) {
    super(), this.jt = t;
  }
  zi(t, i, n) {
    t.yt = false, i.yt = false;
    const s = this.jt;
    if (!s.yt()) return;
    const e2 = s.W(), r2 = e2.lastValueVisible, h2 = "" !== s.bh(), l2 = 0 === e2.seriesLastValueMode, a2 = s.Zr(false);
    if (a2.Xr) return;
    r2 && (t.Kt = this.wh(a2, r2, l2), t.yt = 0 !== t.Kt.length), (h2 || l2) && (i.Kt = this.gh(a2, r2, h2, l2), i.yt = i.Kt.length > 0);
    const o2 = s.mh(a2.V), _2 = R(o2);
    n.t = _2.t, n.ki = a2.ki, i.Ot = s.$t().Bt(a2.ki / s.Dt().At()), t.Ot = o2, t.V = _2.i, i.V = _2.i;
  }
  gh(t, i, n, s) {
    let e2 = "";
    const r2 = this.jt.bh();
    return n && 0 !== r2.length && (e2 += `${r2} `), i && s && (e2 += this.jt.Dt().Mh() ? t.xh : t.Sh), e2.trim();
  }
  wh(t, i, n) {
    return i ? n ? this.jt.Dt().Mh() ? t.Sh : t.xh : t.Kt : "";
  }
}
function Pi(t, i, n, s) {
  const e2 = Number.isFinite(i), r2 = Number.isFinite(n);
  return e2 && r2 ? t(i, n) : e2 || r2 ? e2 ? i : n : s;
}
class Ri {
  constructor(t, i) {
    this.kh = t, this.yh = i;
  }
  Ch(t) {
    return null !== t && (this.kh === t.kh && this.yh === t.yh);
  }
  Th() {
    return new Ri(this.kh, this.yh);
  }
  Ph() {
    return this.kh;
  }
  Rh() {
    return this.yh;
  }
  Dh() {
    return this.yh - this.kh;
  }
  Ni() {
    return this.yh === this.kh || Number.isNaN(this.yh) || Number.isNaN(this.kh);
  }
  ts(t) {
    return null === t ? this : new Ri(Pi(Math.min, this.Ph(), t.Ph(), -1 / 0), Pi(Math.max, this.Rh(), t.Rh(), 1 / 0));
  }
  Vh(t) {
    if (!O(t)) return;
    if (0 === this.yh - this.kh) return;
    const i = 0.5 * (this.yh + this.kh);
    let n = this.yh - i, s = this.kh - i;
    n *= t, s *= t, this.yh = i + n, this.kh = i + s;
  }
  Oh(t) {
    O(t) && (this.yh += t, this.kh += t);
  }
  Bh() {
    return { minValue: this.kh, maxValue: this.yh };
  }
  static Ah(t) {
    return null === t ? null : new Ri(t.minValue, t.maxValue);
  }
}
class Di {
  constructor(t, i) {
    this.Ih = t, this.zh = i || null;
  }
  Lh() {
    return this.Ih;
  }
  Eh() {
    return this.zh;
  }
  Bh() {
    return null === this.Ih ? null : { priceRange: this.Ih.Bh(), margins: this.zh || void 0 };
  }
  static Ah(t) {
    return null === t ? null : new Di(Ri.Ah(t.priceRange), t.margins);
  }
}
class Vi extends hi {
  constructor(t, i) {
    super(t), this.Nh = i;
  }
  yr() {
    const t = this.Sr;
    t.yt = false;
    const i = this.Nh.W();
    if (!this.Es.yt() || !i.lineVisible) return;
    const n = this.Nh.Fh();
    null !== n && (t.yt = true, t.st = n, t.V = i.color, t.et = i.lineWidth, t.Nt = i.lineStyle, t.gr = this.Nh.W().id);
  }
}
class Oi extends nt {
  constructor(t, i) {
    super(), this.jr = t, this.Nh = i;
  }
  zi(t, i, n) {
    t.yt = false, i.yt = false;
    const s = this.Nh.W(), e2 = s.axisLabelVisible, r2 = "" !== s.title, h2 = this.jr;
    if (!e2 || !h2.yt()) return;
    const l2 = this.Nh.Fh();
    if (null === l2) return;
    r2 && (i.Kt = s.title, i.yt = true), i.Ot = h2.$t().Bt(l2 / h2.Dt().At()), t.Kt = this.Wh(s.price), t.yt = true;
    const a2 = R(s.axisLabelColor || s.color);
    n.t = a2.t;
    const o2 = s.axisLabelTextColor || a2.i;
    t.V = o2, i.V = o2, n.ki = l2;
  }
  Wh(t) {
    const i = this.jr.Ct();
    return null === i ? "" : this.jr.Dt().Fi(t, i.Vt);
  }
}
class Bi {
  constructor(t, i) {
    this.jr = t, this.cn = i, this.jh = new Vi(t, this), this.ur = new Oi(t, this), this.Hh = new ei(this.ur, t, t.$t());
  }
  $h(t) {
    V(this.cn, t), this.bt(), this.jr.$t().Uh();
  }
  W() {
    return this.cn;
  }
  qh() {
    return this.jh;
  }
  Yh() {
    return this.Hh;
  }
  Zh() {
    return this.ur;
  }
  bt() {
    this.jh.bt(), this.ur.bt();
  }
  Fh() {
    const t = this.jr, i = t.Dt();
    if (t.$t().St().Ni() || i.Ni()) return null;
    const n = t.Ct();
    return null === n ? null : i.Rt(this.cn.price, n.Vt);
  }
}
class Ai extends lt {
  constructor(t) {
    super(), this.$i = t;
  }
  $t() {
    return this.$i;
  }
}
const Ii = { Bar: (t, i, n, s) => {
  var e2;
  const r2 = i.upColor, h2 = i.downColor, l2 = b(t(n, s)), a2 = w(l2.Vt[0]) <= w(l2.Vt[3]);
  return { ce: null !== (e2 = l2.V) && void 0 !== e2 ? e2 : a2 ? r2 : h2 };
}, Candlestick: (t, i, n, s) => {
  var e2, r2, h2;
  const l2 = i.upColor, a2 = i.downColor, o2 = i.borderUpColor, _2 = i.borderDownColor, u2 = i.wickUpColor, c2 = i.wickDownColor, d2 = b(t(n, s)), f2 = w(d2.Vt[0]) <= w(d2.Vt[3]);
  return { ce: null !== (e2 = d2.V) && void 0 !== e2 ? e2 : f2 ? l2 : a2, Ne: null !== (r2 = d2.Ot) && void 0 !== r2 ? r2 : f2 ? o2 : _2, Ee: null !== (h2 = d2.Xh) && void 0 !== h2 ? h2 : f2 ? u2 : c2 };
}, Custom: (t, i, n, s) => {
  var e2;
  return { ce: null !== (e2 = b(t(n, s)).V) && void 0 !== e2 ? e2 : i.color };
}, Area: (t, i, n, s) => {
  var e2, r2, h2, l2;
  const a2 = b(t(n, s));
  return { ce: null !== (e2 = a2.lt) && void 0 !== e2 ? e2 : i.lineColor, lt: null !== (r2 = a2.lt) && void 0 !== r2 ? r2 : i.lineColor, Ps: null !== (h2 = a2.Ps) && void 0 !== h2 ? h2 : i.topColor, Rs: null !== (l2 = a2.Rs) && void 0 !== l2 ? l2 : i.bottomColor };
}, Baseline: (t, i, n, s) => {
  var e2, r2, h2, l2, a2, o2;
  const _2 = b(t(n, s));
  return { ce: _2.Vt[3] >= i.baseValue.price ? i.topLineColor : i.bottomLineColor, Re: null !== (e2 = _2.Re) && void 0 !== e2 ? e2 : i.topLineColor, De: null !== (r2 = _2.De) && void 0 !== r2 ? r2 : i.bottomLineColor, ke: null !== (h2 = _2.ke) && void 0 !== h2 ? h2 : i.topFillColor1, ye: null !== (l2 = _2.ye) && void 0 !== l2 ? l2 : i.topFillColor2, Ce: null !== (a2 = _2.Ce) && void 0 !== a2 ? a2 : i.bottomFillColor1, Te: null !== (o2 = _2.Te) && void 0 !== o2 ? o2 : i.bottomFillColor2 };
}, Line: (t, i, n, s) => {
  var e2, r2;
  const h2 = b(t(n, s));
  return { ce: null !== (e2 = h2.V) && void 0 !== e2 ? e2 : i.color, lt: null !== (r2 = h2.V) && void 0 !== r2 ? r2 : i.color };
}, Histogram: (t, i, n, s) => {
  var e2;
  return { ce: null !== (e2 = b(t(n, s)).V) && void 0 !== e2 ? e2 : i.color };
} };
class zi {
  constructor(t) {
    this.Kh = (t2, i) => void 0 !== i ? i.Vt : this.jr.In().Gh(t2), this.jr = t, this.Jh = Ii[t.Qh()];
  }
  $s(t, i) {
    return this.Jh(this.Kh, this.jr.W(), t, i);
  }
}
var Li;
!function(t) {
  t[t.NearestLeft = -1] = "NearestLeft", t[t.None = 0] = "None", t[t.NearestRight = 1] = "NearestRight";
}(Li || (Li = {}));
const Ei = 30;
class Ni {
  constructor() {
    this.tl = [], this.il = /* @__PURE__ */ new Map(), this.nl = /* @__PURE__ */ new Map();
  }
  sl() {
    return this.Ks() > 0 ? this.tl[this.tl.length - 1] : null;
  }
  el() {
    return this.Ks() > 0 ? this.rl(0) : null;
  }
  An() {
    return this.Ks() > 0 ? this.rl(this.tl.length - 1) : null;
  }
  Ks() {
    return this.tl.length;
  }
  Ni() {
    return 0 === this.Ks();
  }
  Kr(t) {
    return null !== this.hl(t, 0);
  }
  Gh(t) {
    return this.ll(t);
  }
  ll(t, i = 0) {
    const n = this.hl(t, i);
    return null === n ? null : Object.assign(Object.assign({}, this.al(n)), { ee: this.rl(n) });
  }
  ne() {
    return this.tl;
  }
  ol(t, i, n) {
    if (this.Ni()) return null;
    let s = null;
    for (const e2 of n) {
      s = Fi(s, this._l(t, i, e2));
    }
    return s;
  }
  J(t) {
    this.nl.clear(), this.il.clear(), this.tl = t;
  }
  rl(t) {
    return this.tl[t].ee;
  }
  al(t) {
    return this.tl[t];
  }
  hl(t, i) {
    const n = this.ul(t);
    if (null === n && 0 !== i) switch (i) {
      case -1:
        return this.cl(t);
      case 1:
        return this.dl(t);
      default:
        throw new TypeError("Unknown search mode");
    }
    return n;
  }
  cl(t) {
    let i = this.fl(t);
    return i > 0 && (i -= 1), i !== this.tl.length && this.rl(i) < t ? i : null;
  }
  dl(t) {
    const i = this.vl(t);
    return i !== this.tl.length && t < this.rl(i) ? i : null;
  }
  ul(t) {
    const i = this.fl(t);
    return i === this.tl.length || t < this.tl[i].ee ? null : i;
  }
  fl(t) {
    return Bt(this.tl, t, (t2, i) => t2.ee < i);
  }
  vl(t) {
    return At(this.tl, t, (t2, i) => t2.ee > i);
  }
  pl(t, i, n) {
    let s = null;
    for (let e2 = t; e2 < i; e2++) {
      const t2 = this.tl[e2].Vt[n];
      Number.isNaN(t2) || (null === s ? s = { ml: t2, bl: t2 } : (t2 < s.ml && (s.ml = t2), t2 > s.bl && (s.bl = t2)));
    }
    return s;
  }
  _l(t, i, n) {
    if (this.Ni()) return null;
    let s = null;
    const e2 = b(this.el()), r2 = b(this.An()), h2 = Math.max(t, e2), l2 = Math.min(i, r2), a2 = Math.ceil(h2 / Ei) * Ei, o2 = Math.max(a2, Math.floor(l2 / Ei) * Ei);
    {
      const t2 = this.fl(h2), e3 = this.vl(Math.min(l2, a2, i));
      s = Fi(s, this.pl(t2, e3, n));
    }
    let _2 = this.il.get(n);
    void 0 === _2 && (_2 = /* @__PURE__ */ new Map(), this.il.set(n, _2));
    for (let t2 = Math.max(a2 + 1, h2); t2 < o2; t2 += Ei) {
      const i2 = Math.floor(t2 / Ei);
      let e3 = _2.get(i2);
      if (void 0 === e3) {
        const t3 = this.fl(i2 * Ei), s2 = this.vl((i2 + 1) * Ei - 1);
        e3 = this.pl(t3, s2, n), _2.set(i2, e3);
      }
      s = Fi(s, e3);
    }
    {
      const t2 = this.fl(o2), i2 = this.vl(l2);
      s = Fi(s, this.pl(t2, i2, n));
    }
    return s;
  }
}
function Fi(t, i) {
  if (null === t) return i;
  if (null === i) return t;
  return { ml: Math.min(t.ml, i.ml), bl: Math.max(t.bl, i.bl) };
}
class Wi {
  constructor(t) {
    this.wl = t;
  }
  X(t, i, n) {
    this.wl.draw(t);
  }
  gl(t, i, n) {
    var s, e2;
    null === (e2 = (s = this.wl).drawBackground) || void 0 === e2 || e2.call(s, t);
  }
}
class ji {
  constructor(t) {
    this.tr = null, this.wn = t;
  }
  gt() {
    var t;
    const i = this.wn.renderer();
    if (null === i) return null;
    if ((null === (t = this.tr) || void 0 === t ? void 0 : t.Ml) === i) return this.tr.xl;
    const n = new Wi(i);
    return this.tr = { Ml: i, xl: n }, n;
  }
  Sl() {
    var t, i, n;
    return null !== (n = null === (i = (t = this.wn).zOrder) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : "normal";
  }
}
function Hi(t) {
  var i, n, s, e2, r2;
  return { Kt: t.text(), ki: t.coordinate(), Si: null === (i = t.fixedCoordinate) || void 0 === i ? void 0 : i.call(t), V: t.textColor(), t: t.backColor(), yt: null === (s = null === (n = t.visible) || void 0 === n ? void 0 : n.call(t)) || void 0 === s || s, hi: null === (r2 = null === (e2 = t.tickVisible) || void 0 === e2 ? void 0 : e2.call(t)) || void 0 === r2 || r2 };
}
class $i {
  constructor(t, i) {
    this.Wt = new rt(), this.kl = t, this.yl = i;
  }
  gt() {
    return this.Wt.J(Object.assign({ Hi: this.yl.Hi() }, Hi(this.kl))), this.Wt;
  }
}
class Ui extends nt {
  constructor(t, i) {
    super(), this.kl = t, this.Li = i;
  }
  zi(t, i, n) {
    const s = Hi(this.kl);
    n.t = s.t, t.V = s.V;
    const e2 = 2 / 12 * this.Li.P();
    n.wi = e2, n.gi = e2, n.ki = s.ki, n.Si = s.Si, t.Kt = s.Kt, t.yt = s.yt, t.hi = s.hi;
  }
}
class qi {
  constructor(t, i) {
    this.Cl = null, this.Tl = null, this.Pl = null, this.Rl = null, this.Dl = null, this.Vl = t, this.jr = i;
  }
  Ol() {
    return this.Vl;
  }
  Vn() {
    var t, i;
    null === (i = (t = this.Vl).updateAllViews) || void 0 === i || i.call(t);
  }
  Pn() {
    var t, i, n, s;
    const e2 = null !== (n = null === (i = (t = this.Vl).paneViews) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : [];
    if ((null === (s = this.Cl) || void 0 === s ? void 0 : s.Ml) === e2) return this.Cl.xl;
    const r2 = e2.map((t2) => new ji(t2));
    return this.Cl = { Ml: e2, xl: r2 }, r2;
  }
  Qi() {
    var t, i, n, s;
    const e2 = null !== (n = null === (i = (t = this.Vl).timeAxisViews) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : [];
    if ((null === (s = this.Tl) || void 0 === s ? void 0 : s.Ml) === e2) return this.Tl.xl;
    const r2 = this.jr.$t().St(), h2 = e2.map((t2) => new $i(t2, r2));
    return this.Tl = { Ml: e2, xl: h2 }, h2;
  }
  Rn() {
    var t, i, n, s;
    const e2 = null !== (n = null === (i = (t = this.Vl).priceAxisViews) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : [];
    if ((null === (s = this.Pl) || void 0 === s ? void 0 : s.Ml) === e2) return this.Pl.xl;
    const r2 = this.jr.Dt(), h2 = e2.map((t2) => new Ui(t2, r2));
    return this.Pl = { Ml: e2, xl: h2 }, h2;
  }
  Bl() {
    var t, i, n, s;
    const e2 = null !== (n = null === (i = (t = this.Vl).priceAxisPaneViews) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : [];
    if ((null === (s = this.Rl) || void 0 === s ? void 0 : s.Ml) === e2) return this.Rl.xl;
    const r2 = e2.map((t2) => new ji(t2));
    return this.Rl = { Ml: e2, xl: r2 }, r2;
  }
  Al() {
    var t, i, n, s;
    const e2 = null !== (n = null === (i = (t = this.Vl).timeAxisPaneViews) || void 0 === i ? void 0 : i.call(t)) && void 0 !== n ? n : [];
    if ((null === (s = this.Dl) || void 0 === s ? void 0 : s.Ml) === e2) return this.Dl.xl;
    const r2 = e2.map((t2) => new ji(t2));
    return this.Dl = { Ml: e2, xl: r2 }, r2;
  }
  Il(t, i) {
    var n, s, e2;
    return null !== (e2 = null === (s = (n = this.Vl).autoscaleInfo) || void 0 === s ? void 0 : s.call(n, t, i)) && void 0 !== e2 ? e2 : null;
  }
  wr(t, i) {
    var n, s, e2;
    return null !== (e2 = null === (s = (n = this.Vl).hitTest) || void 0 === s ? void 0 : s.call(n, t, i)) && void 0 !== e2 ? e2 : null;
  }
}
function Yi(t, i, n, s) {
  t.forEach((t2) => {
    i(t2).forEach((t3) => {
      t3.Sl() === n && s.push(t3);
    });
  });
}
function Zi(t) {
  return t.Pn();
}
function Xi(t) {
  return t.Bl();
}
function Ki(t) {
  return t.Al();
}
class Gi extends Ai {
  constructor(t, i, n, s, e2) {
    super(t), this.zt = new Ni(), this.jh = new Ci(this), this.zl = [], this.Ll = new li(this), this.El = null, this.Nl = null, this.Fl = [], this.Wl = [], this.jl = null, this.Hl = [], this.cn = i, this.$l = n;
    const r2 = new Ti(this);
    this.rn = [r2], this.Hh = new ei(r2, this, t), "Area" !== n && "Line" !== n && "Baseline" !== n || (this.El = new ci(this)), this.Ul(), this.ql(e2);
  }
  S() {
    null !== this.jl && clearTimeout(this.jl);
  }
  mh(t) {
    return this.cn.priceLineColor || t;
  }
  Zr(t) {
    const i = { Xr: true }, n = this.Dt();
    if (this.$t().St().Ni() || n.Ni() || this.zt.Ni()) return i;
    const s = this.$t().St().Xs(), e2 = this.Ct();
    if (null === s || null === e2) return i;
    let r2, h2;
    if (t) {
      const t2 = this.zt.sl();
      if (null === t2) return i;
      r2 = t2, h2 = t2.ee;
    } else {
      const t2 = this.zt.ll(s.ui(), -1);
      if (null === t2) return i;
      if (r2 = this.zt.Gh(t2.ee), null === r2) return i;
      h2 = t2.ee;
    }
    const l2 = r2.Vt[3], a2 = this.Us().$s(h2, { Vt: r2 }), o2 = n.Rt(l2, e2.Vt);
    return { Xr: false, _t: l2, Kt: n.Fi(l2, e2.Vt), xh: n.Yl(l2), Sh: n.Zl(l2, e2.Vt), V: a2.ce, ki: o2, ee: h2 };
  }
  Us() {
    return null !== this.Nl || (this.Nl = new zi(this)), this.Nl;
  }
  W() {
    return this.cn;
  }
  $h(t) {
    const i = t.priceScaleId;
    void 0 !== i && i !== this.cn.priceScaleId && this.$t().Xl(this, i), V(this.cn, t), void 0 !== t.priceFormat && (this.Ul(), this.$t().Kl()), this.$t().Gl(this), this.$t().Jl(), this.wn.bt("options");
  }
  J(t, i) {
    this.zt.J(t), this.Ql(), this.wn.bt("data"), this.dn.bt("data"), null !== this.El && (i && i.ta ? this.El.$r() : 0 === t.length && this.El.Hr());
    const n = this.$t().dr(this);
    this.$t().ia(n), this.$t().Gl(this), this.$t().Jl(), this.$t().Uh();
  }
  na(t) {
    this.Fl = t, this.Ql();
    const i = this.$t().dr(this);
    this.dn.bt("data"), this.$t().ia(i), this.$t().Gl(this), this.$t().Jl(), this.$t().Uh();
  }
  sa() {
    return this.Fl;
  }
  dh() {
    return this.Wl;
  }
  ea(t) {
    const i = new Bi(this, t);
    return this.zl.push(i), this.$t().Gl(this), i;
  }
  ra(t) {
    const i = this.zl.indexOf(t);
    -1 !== i && this.zl.splice(i, 1), this.$t().Gl(this);
  }
  Qh() {
    return this.$l;
  }
  Ct() {
    const t = this.ha();
    return null === t ? null : { Vt: t.Vt[3], la: t.ot };
  }
  ha() {
    const t = this.$t().St().Xs();
    if (null === t) return null;
    const i = t.Os();
    return this.zt.ll(i, 1);
  }
  In() {
    return this.zt;
  }
  ph(t) {
    const i = this.zt.Gh(t);
    return null === i ? null : "Bar" === this.$l || "Candlestick" === this.$l || "Custom" === this.$l ? { ge: i.Vt[0], Me: i.Vt[1], xe: i.Vt[2], Se: i.Vt[3] } : i.Vt[3];
  }
  aa(t) {
    const i = [];
    Yi(this.Hl, Zi, "top", i);
    const n = this.El;
    return null !== n && n.yt() ? (null === this.jl && n.qr() && (this.jl = setTimeout(() => {
      this.jl = null, this.$t().oa();
    }, 0)), n.Ur(), i.unshift(n), i) : i;
  }
  Pn() {
    const t = [];
    this._a() || t.push(this.Ll), t.push(this.wn, this.jh, this.dn);
    const i = this.zl.map((t2) => t2.qh());
    return t.push(...i), Yi(this.Hl, Zi, "normal", t), t;
  }
  ua() {
    return this.ca(Zi, "bottom");
  }
  da(t) {
    return this.ca(Xi, t);
  }
  fa(t) {
    return this.ca(Ki, t);
  }
  va(t, i) {
    return this.Hl.map((n) => n.wr(t, i)).filter((t2) => null !== t2);
  }
  Ji(t) {
    return [this.Hh, ...this.zl.map((t2) => t2.Yh())];
  }
  Rn(t, i) {
    if (i !== this.Yi && !this._a()) return [];
    const n = [...this.rn];
    for (const t2 of this.zl) n.push(t2.Zh());
    return this.Hl.forEach((t2) => {
      n.push(...t2.Rn());
    }), n;
  }
  Qi() {
    const t = [];
    return this.Hl.forEach((i) => {
      t.push(...i.Qi());
    }), t;
  }
  Il(t, i) {
    if (void 0 !== this.cn.autoscaleInfoProvider) {
      const n = this.cn.autoscaleInfoProvider(() => {
        const n2 = this.pa(t, i);
        return null === n2 ? null : n2.Bh();
      });
      return Di.Ah(n);
    }
    return this.pa(t, i);
  }
  ma() {
    return this.cn.priceFormat.minMove;
  }
  ba() {
    return this.wa;
  }
  Vn() {
    var t;
    this.wn.bt(), this.dn.bt();
    for (const t2 of this.rn) t2.bt();
    for (const t2 of this.zl) t2.bt();
    this.jh.bt(), this.Ll.bt(), null === (t = this.El) || void 0 === t || t.bt(), this.Hl.forEach((t2) => t2.Vn());
  }
  Dt() {
    return b(super.Dt());
  }
  kt(t) {
    if (!(("Line" === this.$l || "Area" === this.$l || "Baseline" === this.$l) && this.cn.crosshairMarkerVisible)) return null;
    const i = this.zt.Gh(t);
    if (null === i) return null;
    return { _t: i.Vt[3], ht: this.ga(), Ot: this.Ma(), Pt: this.xa(), Tt: this.Sa(t) };
  }
  bh() {
    return this.cn.title;
  }
  yt() {
    return this.cn.visible;
  }
  ka(t) {
    this.Hl.push(new qi(t, this));
  }
  ya(t) {
    this.Hl = this.Hl.filter((i) => i.Ol() !== t);
  }
  Ca() {
    if (this.wn instanceof Kt != false) return (t) => this.wn.We(t);
  }
  Ta() {
    if (this.wn instanceof Kt != false) return (t) => this.wn.je(t);
  }
  _a() {
    return !_t(this.Dt().Pa());
  }
  pa(t, i) {
    if (!B(t) || !B(i) || this.zt.Ni()) return null;
    const n = "Line" === this.$l || "Area" === this.$l || "Baseline" === this.$l || "Histogram" === this.$l ? [3] : [2, 1], s = this.zt.ol(t, i, n);
    let e2 = null !== s ? new Ri(s.ml, s.bl) : null;
    if ("Histogram" === this.Qh()) {
      const t2 = this.cn.base, i2 = new Ri(t2, t2);
      e2 = null !== e2 ? e2.ts(i2) : i2;
    }
    let r2 = this.dn.uh();
    return this.Hl.forEach((n2) => {
      const s2 = n2.Il(t, i);
      if (null == s2 ? void 0 : s2.priceRange) {
        const t2 = new Ri(s2.priceRange.minValue, s2.priceRange.maxValue);
        e2 = null !== e2 ? e2.ts(t2) : t2;
      }
      var h2, l2, a2, o2;
      (null == s2 ? void 0 : s2.margins) && (h2 = r2, l2 = s2.margins, r2 = { above: Math.max(null !== (a2 = null == h2 ? void 0 : h2.above) && void 0 !== a2 ? a2 : 0, l2.above), below: Math.max(null !== (o2 = null == h2 ? void 0 : h2.below) && void 0 !== o2 ? o2 : 0, l2.below) });
    }), new Di(e2, r2);
  }
  ga() {
    switch (this.$l) {
      case "Line":
      case "Area":
      case "Baseline":
        return this.cn.crosshairMarkerRadius;
    }
    return 0;
  }
  Ma() {
    switch (this.$l) {
      case "Line":
      case "Area":
      case "Baseline": {
        const t = this.cn.crosshairMarkerBorderColor;
        if (0 !== t.length) return t;
      }
    }
    return null;
  }
  xa() {
    switch (this.$l) {
      case "Line":
      case "Area":
      case "Baseline":
        return this.cn.crosshairMarkerBorderWidth;
    }
    return 0;
  }
  Sa(t) {
    switch (this.$l) {
      case "Line":
      case "Area":
      case "Baseline": {
        const t2 = this.cn.crosshairMarkerBackgroundColor;
        if (0 !== t2.length) return t2;
      }
    }
    return this.Us().$s(t).ce;
  }
  Ul() {
    switch (this.cn.priceFormat.type) {
      case "custom":
        this.wa = { format: this.cn.priceFormat.formatter };
        break;
      case "volume":
        this.wa = new pt(this.cn.priceFormat.precision);
        break;
      case "percent":
        this.wa = new vt(this.cn.priceFormat.precision);
        break;
      default: {
        const t = Math.pow(10, this.cn.priceFormat.precision);
        this.wa = new ft(t, this.cn.priceFormat.minMove * t);
      }
    }
    null !== this.Yi && this.Yi.Ra();
  }
  Ql() {
    const t = this.$t().St();
    if (!t.Da() || this.zt.Ni()) return void (this.Wl = []);
    const i = b(this.zt.el());
    this.Wl = this.Fl.map((n, s) => {
      const e2 = b(t.Va(n.time, true)), r2 = e2 < i ? 1 : -1;
      return { time: b(this.zt.ll(e2, r2)).ee, position: n.position, shape: n.shape, color: n.color, id: n.id, th: s, text: n.text, size: n.size, originalTime: n.originalTime };
    });
  }
  ql(t) {
    switch (this.dn = new yi(this, this.$t()), this.$l) {
      case "Bar":
        this.wn = new Ht(this, this.$t());
        break;
      case "Candlestick":
        this.wn = new Zt(this, this.$t());
        break;
      case "Line":
        this.wn = new ti(this, this.$t());
        break;
      case "Custom":
        this.wn = new Kt(this, this.$t(), m(t));
        break;
      case "Area":
        this.wn = new Ft(this, this.$t());
        break;
      case "Baseline":
        this.wn = new qt(this, this.$t());
        break;
      case "Histogram":
        this.wn = new Qt(this, this.$t());
        break;
      default:
        throw Error("Unknown chart style assigned: " + this.$l);
    }
  }
  ca(t, i) {
    const n = [];
    return Yi(this.Hl, t, i, n), n;
  }
}
class Ji {
  constructor(t) {
    this.cn = t;
  }
  Oa(t, i, n) {
    let s = t;
    if (0 === this.cn.mode) return s;
    const e2 = n.vn(), r2 = e2.Ct();
    if (null === r2) return s;
    const h2 = e2.Rt(t, r2), l2 = n.Ba().filter((t2) => t2 instanceof Gi).reduce((t2, s2) => {
      if (n.vr(s2) || !s2.yt()) return t2;
      const e3 = s2.Dt(), r3 = s2.In();
      if (e3.Ni() || !r3.Kr(i)) return t2;
      const h3 = r3.Gh(i);
      if (null === h3) return t2;
      const l3 = w(s2.Ct());
      return t2.concat([e3.Rt(h3.Vt[3], l3.Vt)]);
    }, []);
    if (0 === l2.length) return s;
    l2.sort((t2, i2) => Math.abs(t2 - h2) - Math.abs(i2 - h2));
    const a2 = l2[0];
    return s = e2.pn(a2, r2), s;
  }
}
class Qi extends H {
  constructor() {
    super(...arguments), this.zt = null;
  }
  J(t) {
    this.zt = t;
  }
  K({ context: t, bitmapSize: i, horizontalPixelRatio: n, verticalPixelRatio: s }) {
    if (null === this.zt) return;
    const e2 = Math.max(1, Math.floor(n));
    t.lineWidth = e2, function(t2, i2) {
      t2.save(), t2.lineWidth % 2 && t2.translate(0.5, 0.5), i2(), t2.restore();
    }(t, () => {
      const r2 = b(this.zt);
      if (r2.Aa) {
        t.strokeStyle = r2.Ia, f(t, r2.za), t.beginPath();
        for (const s2 of r2.La) {
          const r3 = Math.round(s2.Ea * n);
          t.moveTo(r3, -e2), t.lineTo(r3, i.height + e2);
        }
        t.stroke();
      }
      if (r2.Na) {
        t.strokeStyle = r2.Fa, f(t, r2.Wa), t.beginPath();
        for (const n2 of r2.ja) {
          const r3 = Math.round(n2.Ea * s);
          t.moveTo(-e2, r3), t.lineTo(i.width + e2, r3);
        }
        t.stroke();
      }
    });
  }
}
class tn {
  constructor(t) {
    this.Wt = new Qi(), this.ft = true, this.tn = t;
  }
  bt() {
    this.ft = true;
  }
  gt() {
    if (this.ft) {
      const t = this.tn.$t().W().grid, i = { Na: t.horzLines.visible, Aa: t.vertLines.visible, Fa: t.horzLines.color, Ia: t.vertLines.color, Wa: t.horzLines.style, za: t.vertLines.style, ja: this.tn.vn().Ha(), La: (this.tn.$t().St().Ha() || []).map((t2) => ({ Ea: t2.coord })) };
      this.Wt.J(i), this.ft = false;
    }
    return this.Wt;
  }
}
class nn {
  constructor(t) {
    this.wn = new tn(t);
  }
  qh() {
    return this.wn;
  }
}
const sn = { $a: 4, Ua: 1e-4 };
function en(t, i) {
  const n = 100 * (t - i) / i;
  return i < 0 ? -n : n;
}
function rn(t, i) {
  const n = en(t.Ph(), i), s = en(t.Rh(), i);
  return new Ri(n, s);
}
function hn(t, i) {
  const n = 100 * (t - i) / i + 100;
  return i < 0 ? -n : n;
}
function ln(t, i) {
  const n = hn(t.Ph(), i), s = hn(t.Rh(), i);
  return new Ri(n, s);
}
function an(t, i) {
  const n = Math.abs(t);
  if (n < 1e-15) return 0;
  const s = Math.log10(n + i.Ua) + i.$a;
  return t < 0 ? -s : s;
}
function on(t, i) {
  const n = Math.abs(t);
  if (n < 1e-15) return 0;
  const s = Math.pow(10, n - i.$a) - i.Ua;
  return t < 0 ? -s : s;
}
function _n(t, i) {
  if (null === t) return null;
  const n = an(t.Ph(), i), s = an(t.Rh(), i);
  return new Ri(n, s);
}
function un(t, i) {
  if (null === t) return null;
  const n = on(t.Ph(), i), s = on(t.Rh(), i);
  return new Ri(n, s);
}
function cn(t) {
  if (null === t) return sn;
  const i = Math.abs(t.Rh() - t.Ph());
  if (i >= 1 || i < 1e-15) return sn;
  const n = Math.ceil(Math.abs(Math.log10(i))), s = sn.$a + n;
  return { $a: s, Ua: 1 / Math.pow(10, s) };
}
class dn {
  constructor(t, i) {
    if (this.qa = t, this.Ya = i, function(t2) {
      if (t2 < 0) return false;
      for (let i2 = t2; i2 > 1; i2 /= 10) if (i2 % 10 != 0) return false;
      return true;
    }(this.qa)) this.Za = [2, 2.5, 2];
    else {
      this.Za = [];
      for (let t2 = this.qa; 1 !== t2; ) {
        if (t2 % 2 == 0) this.Za.push(2), t2 /= 2;
        else {
          if (t2 % 5 != 0) throw new Error("unexpected base");
          this.Za.push(2, 2.5), t2 /= 5;
        }
        if (this.Za.length > 100) throw new Error("something wrong with base");
      }
    }
  }
  Xa(t, i, n) {
    const s = 0 === this.qa ? 0 : 1 / this.qa;
    let e2 = Math.pow(10, Math.max(0, Math.ceil(Math.log10(t - i)))), r2 = 0, h2 = this.Ya[0];
    for (; ; ) {
      const t2 = yt(e2, s, 1e-14) && e2 > s + 1e-14, i2 = yt(e2, n * h2, 1e-14), l3 = yt(e2, 1, 1e-14);
      if (!(t2 && i2 && l3)) break;
      e2 /= h2, h2 = this.Ya[++r2 % this.Ya.length];
    }
    if (e2 <= s + 1e-14 && (e2 = s), e2 = Math.max(1, e2), this.Za.length > 0 && (l2 = e2, a2 = 1, o2 = 1e-14, Math.abs(l2 - a2) < o2)) for (r2 = 0, h2 = this.Za[0]; yt(e2, n * h2, 1e-14) && e2 > s + 1e-14; ) e2 /= h2, h2 = this.Za[++r2 % this.Za.length];
    var l2, a2, o2;
    return e2;
  }
}
class fn {
  constructor(t, i, n, s) {
    this.Ka = [], this.Li = t, this.qa = i, this.Ga = n, this.Ja = s;
  }
  Xa(t, i) {
    if (t < i) throw new Error("high < low");
    const n = this.Li.At(), s = (t - i) * this.Qa() / n, e2 = new dn(this.qa, [2, 2.5, 2]), r2 = new dn(this.qa, [2, 2, 2.5]), h2 = new dn(this.qa, [2.5, 2, 2]), l2 = [];
    return l2.push(e2.Xa(t, i, s), r2.Xa(t, i, s), h2.Xa(t, i, s)), function(t2) {
      if (t2.length < 1) throw Error("array is empty");
      let i2 = t2[0];
      for (let n2 = 1; n2 < t2.length; ++n2) t2[n2] < i2 && (i2 = t2[n2]);
      return i2;
    }(l2);
  }
  io() {
    const t = this.Li, i = t.Ct();
    if (null === i) return void (this.Ka = []);
    const n = t.At(), s = this.Ga(n - 1, i), e2 = this.Ga(0, i), r2 = this.Li.W().entireTextOnly ? this.no() / 2 : 0, h2 = r2, l2 = n - 1 - r2, a2 = Math.max(s, e2), o2 = Math.min(s, e2);
    if (a2 === o2) return void (this.Ka = []);
    let _2 = this.Xa(a2, o2), u2 = a2 % _2;
    u2 += u2 < 0 ? _2 : 0;
    const c2 = a2 >= o2 ? 1 : -1;
    let d2 = null, f2 = 0;
    for (let n2 = a2 - u2; n2 > o2; n2 -= _2) {
      const s2 = this.Ja(n2, i, true);
      null !== d2 && Math.abs(s2 - d2) < this.Qa() || (s2 < h2 || s2 > l2 || (f2 < this.Ka.length ? (this.Ka[f2].Ea = s2, this.Ka[f2].so = t.eo(n2)) : this.Ka.push({ Ea: s2, so: t.eo(n2) }), f2++, d2 = s2, t.ro() && (_2 = this.Xa(n2 * c2, o2))));
    }
    this.Ka.length = f2;
  }
  Ha() {
    return this.Ka;
  }
  no() {
    return this.Li.P();
  }
  Qa() {
    return Math.ceil(2.5 * this.no());
  }
}
function vn(t) {
  return t.slice().sort((t2, i) => b(t2.Xi()) - b(i.Xi()));
}
var pn;
!function(t) {
  t[t.Normal = 0] = "Normal", t[t.Logarithmic = 1] = "Logarithmic", t[t.Percentage = 2] = "Percentage", t[t.IndexedTo100 = 3] = "IndexedTo100";
}(pn || (pn = {}));
const mn = new vt(), bn = new ft(100, 1);
class wn {
  constructor(t, i, n, s) {
    this.ho = 0, this.lo = null, this.Ih = null, this.ao = null, this.oo = { _o: false, uo: null }, this.co = 0, this.do = 0, this.fo = new D(), this.vo = new D(), this.po = [], this.mo = null, this.bo = null, this.wo = null, this.Mo = null, this.wa = bn, this.xo = cn(null), this.So = t, this.cn = i, this.ko = n, this.yo = s, this.Co = new fn(this, 100, this.To.bind(this), this.Po.bind(this));
  }
  Pa() {
    return this.So;
  }
  W() {
    return this.cn;
  }
  $h(t) {
    if (V(this.cn, t), this.Ra(), void 0 !== t.mode && this.Ro({ Cr: t.mode }), void 0 !== t.scaleMargins) {
      const i = m(t.scaleMargins.top), n = m(t.scaleMargins.bottom);
      if (i < 0 || i > 1) throw new Error(`Invalid top margin - expect value between 0 and 1, given=${i}`);
      if (n < 0 || n > 1) throw new Error(`Invalid bottom margin - expect value between 0 and 1, given=${n}`);
      if (i + n > 1) throw new Error(`Invalid margins - sum of margins must be less than 1, given=${i + n}`);
      this.Do(), this.bo = null;
    }
  }
  Vo() {
    return this.cn.autoScale;
  }
  ro() {
    return 1 === this.cn.mode;
  }
  Mh() {
    return 2 === this.cn.mode;
  }
  Oo() {
    return 3 === this.cn.mode;
  }
  Cr() {
    return { Wn: this.cn.autoScale, Bo: this.cn.invertScale, Cr: this.cn.mode };
  }
  Ro(t) {
    const i = this.Cr();
    let n = null;
    void 0 !== t.Wn && (this.cn.autoScale = t.Wn), void 0 !== t.Cr && (this.cn.mode = t.Cr, 2 !== t.Cr && 3 !== t.Cr || (this.cn.autoScale = true), this.oo._o = false), 1 === i.Cr && t.Cr !== i.Cr && (!function(t2, i2) {
      if (null === t2) return false;
      const n2 = on(t2.Ph(), i2), s2 = on(t2.Rh(), i2);
      return isFinite(n2) && isFinite(s2);
    }(this.Ih, this.xo) ? this.cn.autoScale = true : (n = un(this.Ih, this.xo), null !== n && this.Ao(n))), 1 === t.Cr && t.Cr !== i.Cr && (n = _n(this.Ih, this.xo), null !== n && this.Ao(n));
    const s = i.Cr !== this.cn.mode;
    s && (2 === i.Cr || this.Mh()) && this.Ra(), s && (3 === i.Cr || this.Oo()) && this.Ra(), void 0 !== t.Bo && i.Bo !== t.Bo && (this.cn.invertScale = t.Bo, this.Io()), this.vo.m(i, this.Cr());
  }
  zo() {
    return this.vo;
  }
  P() {
    return this.ko.fontSize;
  }
  At() {
    return this.ho;
  }
  Lo(t) {
    this.ho !== t && (this.ho = t, this.Do(), this.bo = null);
  }
  Eo() {
    if (this.lo) return this.lo;
    const t = this.At() - this.No() - this.Fo();
    return this.lo = t, t;
  }
  Lh() {
    return this.Wo(), this.Ih;
  }
  Ao(t, i) {
    const n = this.Ih;
    (i || null === n && null !== t || null !== n && !n.Ch(t)) && (this.bo = null, this.Ih = t);
  }
  Ni() {
    return this.Wo(), 0 === this.ho || !this.Ih || this.Ih.Ni();
  }
  jo(t) {
    return this.Bo() ? t : this.At() - 1 - t;
  }
  Rt(t, i) {
    return this.Mh() ? t = en(t, i) : this.Oo() && (t = hn(t, i)), this.Po(t, i);
  }
  te(t, i, n) {
    this.Wo();
    const s = this.Fo(), e2 = b(this.Lh()), r2 = e2.Ph(), h2 = e2.Rh(), l2 = this.Eo() - 1, a2 = this.Bo(), o2 = l2 / (h2 - r2), _2 = void 0 === n ? 0 : n.from, u2 = void 0 === n ? t.length : n.to, c2 = this.Ho();
    for (let n2 = _2; n2 < u2; n2++) {
      const e3 = t[n2], h3 = e3._t;
      if (isNaN(h3)) continue;
      let l3 = h3;
      null !== c2 && (l3 = c2(e3._t, i));
      const _3 = s + o2 * (l3 - r2), u3 = a2 ? _3 : this.ho - 1 - _3;
      e3.st = u3;
    }
  }
  be(t, i, n) {
    this.Wo();
    const s = this.Fo(), e2 = b(this.Lh()), r2 = e2.Ph(), h2 = e2.Rh(), l2 = this.Eo() - 1, a2 = this.Bo(), o2 = l2 / (h2 - r2), _2 = void 0 === n ? 0 : n.from, u2 = void 0 === n ? t.length : n.to, c2 = this.Ho();
    for (let n2 = _2; n2 < u2; n2++) {
      const e3 = t[n2];
      let h3 = e3.ge, l3 = e3.Me, _3 = e3.xe, u3 = e3.Se;
      null !== c2 && (h3 = c2(e3.ge, i), l3 = c2(e3.Me, i), _3 = c2(e3.xe, i), u3 = c2(e3.Se, i));
      let d2 = s + o2 * (h3 - r2), f2 = a2 ? d2 : this.ho - 1 - d2;
      e3.pe = f2, d2 = s + o2 * (l3 - r2), f2 = a2 ? d2 : this.ho - 1 - d2, e3.de = f2, d2 = s + o2 * (_3 - r2), f2 = a2 ? d2 : this.ho - 1 - d2, e3.fe = f2, d2 = s + o2 * (u3 - r2), f2 = a2 ? d2 : this.ho - 1 - d2, e3.me = f2;
    }
  }
  pn(t, i) {
    const n = this.To(t, i);
    return this.$o(n, i);
  }
  $o(t, i) {
    let n = t;
    return this.Mh() ? n = function(t2, i2) {
      return i2 < 0 && (t2 = -t2), t2 / 100 * i2 + i2;
    }(n, i) : this.Oo() && (n = function(t2, i2) {
      return t2 -= 100, i2 < 0 && (t2 = -t2), t2 / 100 * i2 + i2;
    }(n, i)), n;
  }
  Ba() {
    return this.po;
  }
  Uo() {
    if (this.mo) return this.mo;
    let t = [];
    for (let i = 0; i < this.po.length; i++) {
      const n = this.po[i];
      null === n.Xi() && n.Ki(i + 1), t.push(n);
    }
    return t = vn(t), this.mo = t, this.mo;
  }
  qo(t) {
    -1 === this.po.indexOf(t) && (this.po.push(t), this.Ra(), this.Yo());
  }
  Zo(t) {
    const i = this.po.indexOf(t);
    if (-1 === i) throw new Error("source is not attached to scale");
    this.po.splice(i, 1), 0 === this.po.length && (this.Ro({ Wn: true }), this.Ao(null)), this.Ra(), this.Yo();
  }
  Ct() {
    let t = null;
    for (const i of this.po) {
      const n = i.Ct();
      null !== n && ((null === t || n.la < t.la) && (t = n));
    }
    return null === t ? null : t.Vt;
  }
  Bo() {
    return this.cn.invertScale;
  }
  Ha() {
    const t = null === this.Ct();
    if (null !== this.bo && (t || this.bo.Xo === t)) return this.bo.Ha;
    this.Co.io();
    const i = this.Co.Ha();
    return this.bo = { Ha: i, Xo: t }, this.fo.m(), i;
  }
  Ko() {
    return this.fo;
  }
  Go(t) {
    this.Mh() || this.Oo() || null === this.wo && null === this.ao && (this.Ni() || (this.wo = this.ho - t, this.ao = b(this.Lh()).Th()));
  }
  Jo(t) {
    if (this.Mh() || this.Oo()) return;
    if (null === this.wo) return;
    this.Ro({ Wn: false }), (t = this.ho - t) < 0 && (t = 0);
    let i = (this.wo + 0.2 * (this.ho - 1)) / (t + 0.2 * (this.ho - 1));
    const n = b(this.ao).Th();
    i = Math.max(i, 0.1), n.Vh(i), this.Ao(n);
  }
  Qo() {
    this.Mh() || this.Oo() || (this.wo = null, this.ao = null);
  }
  t_(t) {
    this.Vo() || null === this.Mo && null === this.ao && (this.Ni() || (this.Mo = t, this.ao = b(this.Lh()).Th()));
  }
  i_(t) {
    if (this.Vo()) return;
    if (null === this.Mo) return;
    const i = b(this.Lh()).Dh() / (this.Eo() - 1);
    let n = t - this.Mo;
    this.Bo() && (n *= -1);
    const s = n * i, e2 = b(this.ao).Th();
    e2.Oh(s), this.Ao(e2, true), this.bo = null;
  }
  n_() {
    this.Vo() || null !== this.Mo && (this.Mo = null, this.ao = null);
  }
  ba() {
    return this.wa || this.Ra(), this.wa;
  }
  Fi(t, i) {
    switch (this.cn.mode) {
      case 2:
        return this.s_(en(t, i));
      case 3:
        return this.ba().format(hn(t, i));
      default:
        return this.Wh(t);
    }
  }
  eo(t) {
    switch (this.cn.mode) {
      case 2:
        return this.s_(t);
      case 3:
        return this.ba().format(t);
      default:
        return this.Wh(t);
    }
  }
  Yl(t) {
    return this.Wh(t, b(this.e_()).ba());
  }
  Zl(t, i) {
    return t = en(t, i), this.s_(t, mn);
  }
  r_() {
    return this.po;
  }
  h_(t) {
    this.oo = { uo: t, _o: false };
  }
  Vn() {
    this.po.forEach((t) => t.Vn());
  }
  Ra() {
    this.bo = null;
    const t = this.e_();
    let i = 100;
    null !== t && (i = Math.round(1 / t.ma())), this.wa = bn, this.Mh() ? (this.wa = mn, i = 100) : this.Oo() ? (this.wa = new ft(100, 1), i = 100) : null !== t && (this.wa = t.ba()), this.Co = new fn(this, i, this.To.bind(this), this.Po.bind(this)), this.Co.io();
  }
  Yo() {
    this.mo = null;
  }
  e_() {
    return this.po[0] || null;
  }
  No() {
    return this.Bo() ? this.cn.scaleMargins.bottom * this.At() + this.do : this.cn.scaleMargins.top * this.At() + this.co;
  }
  Fo() {
    return this.Bo() ? this.cn.scaleMargins.top * this.At() + this.co : this.cn.scaleMargins.bottom * this.At() + this.do;
  }
  Wo() {
    this.oo._o || (this.oo._o = true, this.l_());
  }
  Do() {
    this.lo = null;
  }
  Po(t, i) {
    if (this.Wo(), this.Ni()) return 0;
    t = this.ro() && t ? an(t, this.xo) : t;
    const n = b(this.Lh()), s = this.Fo() + (this.Eo() - 1) * (t - n.Ph()) / n.Dh();
    return this.jo(s);
  }
  To(t, i) {
    if (this.Wo(), this.Ni()) return 0;
    const n = this.jo(t), s = b(this.Lh()), e2 = s.Ph() + s.Dh() * ((n - this.Fo()) / (this.Eo() - 1));
    return this.ro() ? on(e2, this.xo) : e2;
  }
  Io() {
    this.bo = null, this.Co.io();
  }
  l_() {
    const t = this.oo.uo;
    if (null === t) return;
    let i = null;
    const n = this.r_();
    let s = 0, e2 = 0;
    for (const r3 of n) {
      if (!r3.yt()) continue;
      const n2 = r3.Ct();
      if (null === n2) continue;
      const h3 = r3.Il(t.Os(), t.ui());
      let l2 = h3 && h3.Lh();
      if (null !== l2) {
        switch (this.cn.mode) {
          case 1:
            l2 = _n(l2, this.xo);
            break;
          case 2:
            l2 = rn(l2, n2.Vt);
            break;
          case 3:
            l2 = ln(l2, n2.Vt);
        }
        if (i = null === i ? l2 : i.ts(b(l2)), null !== h3) {
          const t2 = h3.Eh();
          null !== t2 && (s = Math.max(s, t2.above), e2 = Math.max(e2, t2.below));
        }
      }
    }
    if (s === this.co && e2 === this.do || (this.co = s, this.do = e2, this.bo = null, this.Do()), null !== i) {
      if (i.Ph() === i.Rh()) {
        const t2 = this.e_(), n2 = 5 * (null === t2 || this.Mh() || this.Oo() ? 1 : t2.ma());
        this.ro() && (i = un(i, this.xo)), i = new Ri(i.Ph() - n2, i.Rh() + n2), this.ro() && (i = _n(i, this.xo));
      }
      if (this.ro()) {
        const t2 = un(i, this.xo), n2 = cn(t2);
        if (r2 = n2, h2 = this.xo, r2.$a !== h2.$a || r2.Ua !== h2.Ua) {
          const s2 = null !== this.ao ? un(this.ao, this.xo) : null;
          this.xo = n2, i = _n(t2, n2), null !== s2 && (this.ao = _n(s2, n2));
        }
      }
      this.Ao(i);
    } else null === this.Ih && (this.Ao(new Ri(-0.5, 0.5)), this.xo = cn(null));
    var r2, h2;
    this.oo._o = true;
  }
  Ho() {
    return this.Mh() ? en : this.Oo() ? hn : this.ro() ? (t) => an(t, this.xo) : null;
  }
  a_(t, i, n) {
    return void 0 === i ? (void 0 === n && (n = this.ba()), n.format(t)) : i(t);
  }
  Wh(t, i) {
    return this.a_(t, this.yo.priceFormatter, i);
  }
  s_(t, i) {
    return this.a_(t, this.yo.percentageFormatter, i);
  }
}
class gn {
  constructor(t, i) {
    this.po = [], this.o_ = /* @__PURE__ */ new Map(), this.ho = 0, this.__ = 0, this.u_ = 1e3, this.mo = null, this.c_ = new D(), this.yl = t, this.$i = i, this.d_ = new nn(this);
    const n = i.W();
    this.f_ = this.v_("left", n.leftPriceScale), this.p_ = this.v_("right", n.rightPriceScale), this.f_.zo().l(this.m_.bind(this, this.f_), this), this.p_.zo().l(this.m_.bind(this, this.p_), this), this.b_(n);
  }
  b_(t) {
    if (t.leftPriceScale && this.f_.$h(t.leftPriceScale), t.rightPriceScale && this.p_.$h(t.rightPriceScale), t.localization && (this.f_.Ra(), this.p_.Ra()), t.overlayPriceScales) {
      const i = Array.from(this.o_.values());
      for (const n of i) {
        const i2 = b(n[0].Dt());
        i2.$h(t.overlayPriceScales), t.localization && i2.Ra();
      }
    }
  }
  w_(t) {
    switch (t) {
      case "left":
        return this.f_;
      case "right":
        return this.p_;
    }
    return this.o_.has(t) ? m(this.o_.get(t))[0].Dt() : null;
  }
  S() {
    this.$t().g_().p(this), this.f_.zo().p(this), this.p_.zo().p(this), this.po.forEach((t) => {
      t.S && t.S();
    }), this.c_.m();
  }
  M_() {
    return this.u_;
  }
  x_(t) {
    this.u_ = t;
  }
  $t() {
    return this.$i;
  }
  Hi() {
    return this.__;
  }
  At() {
    return this.ho;
  }
  S_(t) {
    this.__ = t, this.k_();
  }
  Lo(t) {
    this.ho = t, this.f_.Lo(t), this.p_.Lo(t), this.po.forEach((i) => {
      if (this.vr(i)) {
        const n = i.Dt();
        null !== n && n.Lo(t);
      }
    }), this.k_();
  }
  Ba() {
    return this.po;
  }
  vr(t) {
    const i = t.Dt();
    return null === i || this.f_ !== i && this.p_ !== i;
  }
  qo(t, i, n) {
    const s = void 0 !== n ? n : this.C_().y_ + 1;
    this.T_(t, i, s);
  }
  Zo(t) {
    const i = this.po.indexOf(t);
    p(-1 !== i, "removeDataSource: invalid data source"), this.po.splice(i, 1);
    const n = b(t.Dt()).Pa();
    if (this.o_.has(n)) {
      const i2 = m(this.o_.get(n)), s2 = i2.indexOf(t);
      -1 !== s2 && (i2.splice(s2, 1), 0 === i2.length && this.o_.delete(n));
    }
    const s = t.Dt();
    s && s.Ba().indexOf(t) >= 0 && s.Zo(t), null !== s && (s.Yo(), this.P_(s)), this.mo = null;
  }
  mr(t) {
    return t === this.f_ ? "left" : t === this.p_ ? "right" : "overlay";
  }
  R_() {
    return this.f_;
  }
  D_() {
    return this.p_;
  }
  V_(t, i) {
    t.Go(i);
  }
  O_(t, i) {
    t.Jo(i), this.k_();
  }
  B_(t) {
    t.Qo();
  }
  A_(t, i) {
    t.t_(i);
  }
  I_(t, i) {
    t.i_(i), this.k_();
  }
  z_(t) {
    t.n_();
  }
  k_() {
    this.po.forEach((t) => {
      t.Vn();
    });
  }
  vn() {
    let t = null;
    return this.$i.W().rightPriceScale.visible && 0 !== this.p_.Ba().length ? t = this.p_ : this.$i.W().leftPriceScale.visible && 0 !== this.f_.Ba().length ? t = this.f_ : 0 !== this.po.length && (t = this.po[0].Dt()), null === t && (t = this.p_), t;
  }
  pr() {
    let t = null;
    return this.$i.W().rightPriceScale.visible ? t = this.p_ : this.$i.W().leftPriceScale.visible && (t = this.f_), t;
  }
  P_(t) {
    null !== t && t.Vo() && this.L_(t);
  }
  E_(t) {
    const i = this.yl.Xs();
    t.Ro({ Wn: true }), null !== i && t.h_(i), this.k_();
  }
  N_() {
    this.L_(this.f_), this.L_(this.p_);
  }
  F_() {
    this.P_(this.f_), this.P_(this.p_), this.po.forEach((t) => {
      this.vr(t) && this.P_(t.Dt());
    }), this.k_(), this.$i.Uh();
  }
  Uo() {
    return null === this.mo && (this.mo = vn(this.po)), this.mo;
  }
  W_() {
    return this.c_;
  }
  j_() {
    return this.d_;
  }
  L_(t) {
    const i = t.r_();
    if (i && i.length > 0 && !this.yl.Ni()) {
      const i2 = this.yl.Xs();
      null !== i2 && t.h_(i2);
    }
    t.Vn();
  }
  C_() {
    const t = this.Uo();
    if (0 === t.length) return { H_: 0, y_: 0 };
    let i = 0, n = 0;
    for (let s = 0; s < t.length; s++) {
      const e2 = t[s].Xi();
      null !== e2 && (e2 < i && (i = e2), e2 > n && (n = e2));
    }
    return { H_: i, y_: n };
  }
  T_(t, i, n) {
    let s = this.w_(i);
    if (null === s && (s = this.v_(i, this.$i.W().overlayPriceScales)), this.po.push(t), !_t(i)) {
      const n2 = this.o_.get(i) || [];
      n2.push(t), this.o_.set(i, n2);
    }
    s.qo(t), t.Gi(s), t.Ki(n), this.P_(s), this.mo = null;
  }
  m_(t, i, n) {
    i.Cr !== n.Cr && this.L_(t);
  }
  v_(t, i) {
    const n = Object.assign({ visible: true, autoScale: true }, z(i)), s = new wn(t, n, this.$i.W().layout, this.$i.W().localization);
    return s.Lo(this.At()), s;
  }
}
class Mn {
  constructor(t, i, n = 50) {
    this.Ke = 0, this.Ge = 1, this.Je = 1, this.tr = /* @__PURE__ */ new Map(), this.Qe = /* @__PURE__ */ new Map(), this.U_ = t, this.q_ = i, this.ir = n;
  }
  Y_(t) {
    const i = t.time, n = this.q_.cacheKey(i), s = this.tr.get(n);
    if (void 0 !== s) return s.Z_;
    if (this.Ke === this.ir) {
      const t2 = this.Qe.get(this.Je);
      this.Qe.delete(this.Je), this.tr.delete(m(t2)), this.Je++, this.Ke--;
    }
    const e2 = this.U_(t);
    return this.tr.set(n, { Z_: e2, rr: this.Ge }), this.Qe.set(this.Ge, n), this.Ke++, this.Ge++, e2;
  }
}
class xn {
  constructor(t, i) {
    p(t <= i, "right should be >= left"), this.X_ = t, this.K_ = i;
  }
  Os() {
    return this.X_;
  }
  ui() {
    return this.K_;
  }
  G_() {
    return this.K_ - this.X_ + 1;
  }
  Kr(t) {
    return this.X_ <= t && t <= this.K_;
  }
  Ch(t) {
    return this.X_ === t.Os() && this.K_ === t.ui();
  }
}
function Sn(t, i) {
  return null === t || null === i ? t === i : t.Ch(i);
}
class kn {
  constructor() {
    this.J_ = /* @__PURE__ */ new Map(), this.tr = null, this.Q_ = false;
  }
  tu(t) {
    this.Q_ = t, this.tr = null;
  }
  iu(t, i) {
    this.nu(i), this.tr = null;
    for (let n = i; n < t.length; ++n) {
      const i2 = t[n];
      let s = this.J_.get(i2.timeWeight);
      void 0 === s && (s = [], this.J_.set(i2.timeWeight, s)), s.push({ index: n, time: i2.time, weight: i2.timeWeight, originalTime: i2.originalTime });
    }
  }
  su(t, i) {
    const n = Math.ceil(i / t);
    return null !== this.tr && this.tr.eu === n || (this.tr = { Ha: this.ru(n), eu: n }), this.tr.Ha;
  }
  nu(t) {
    if (0 === t) return void this.J_.clear();
    const i = [];
    this.J_.forEach((n, s) => {
      t <= n[0].index ? i.push(s) : n.splice(Bt(n, t, (i2) => i2.index < t), 1 / 0);
    });
    for (const t2 of i) this.J_.delete(t2);
  }
  ru(t) {
    let i = [];
    for (const n of Array.from(this.J_.keys()).sort((t2, i2) => i2 - t2)) {
      if (!this.J_.get(n)) continue;
      const s = i;
      i = [];
      const e2 = s.length;
      let r2 = 0;
      const h2 = m(this.J_.get(n)), l2 = h2.length;
      let a2 = 1 / 0, o2 = -1 / 0;
      for (let n2 = 0; n2 < l2; n2++) {
        const l3 = h2[n2], _2 = l3.index;
        for (; r2 < e2; ) {
          const t2 = s[r2], n3 = t2.index;
          if (!(n3 < _2)) {
            a2 = n3;
            break;
          }
          r2++, i.push(t2), o2 = n3, a2 = 1 / 0;
        }
        if (a2 - _2 >= t && _2 - o2 >= t) i.push(l3), o2 = _2;
        else if (this.Q_) return s;
      }
      for (; r2 < e2; r2++) i.push(s[r2]);
    }
    return i;
  }
}
class yn {
  constructor(t) {
    this.hu = t;
  }
  lu() {
    return null === this.hu ? null : new xn(Math.floor(this.hu.Os()), Math.ceil(this.hu.ui()));
  }
  au() {
    return this.hu;
  }
  static ou() {
    return new yn(null);
  }
}
function Cn(t, i) {
  return t.weight > i.weight ? t : i;
}
class Tn {
  constructor(t, i, n, s) {
    this.__ = 0, this._u = null, this.uu = [], this.Mo = null, this.wo = null, this.cu = new kn(), this.du = /* @__PURE__ */ new Map(), this.fu = yn.ou(), this.vu = true, this.pu = new D(), this.mu = new D(), this.bu = new D(), this.wu = null, this.gu = null, this.Mu = [], this.cn = i, this.yo = n, this.xu = i.rightOffset, this.Su = i.barSpacing, this.$i = t, this.q_ = s, this.ku(), this.cu.tu(i.uniformDistribution);
  }
  W() {
    return this.cn;
  }
  yu(t) {
    V(this.yo, t), this.Cu(), this.ku();
  }
  $h(t, i) {
    var n;
    V(this.cn, t), this.cn.fixLeftEdge && this.Tu(), this.cn.fixRightEdge && this.Pu(), void 0 !== t.barSpacing && this.$i.Gn(t.barSpacing), void 0 !== t.rightOffset && this.$i.Jn(t.rightOffset), void 0 !== t.minBarSpacing && this.$i.Gn(null !== (n = t.barSpacing) && void 0 !== n ? n : this.Su), this.Cu(), this.ku(), this.bu.m();
  }
  mn(t) {
    var i, n;
    return null !== (n = null === (i = this.uu[t]) || void 0 === i ? void 0 : i.time) && void 0 !== n ? n : null;
  }
  Ui(t) {
    var i;
    return null !== (i = this.uu[t]) && void 0 !== i ? i : null;
  }
  Va(t, i) {
    if (this.uu.length < 1) return null;
    if (this.q_.key(t) > this.q_.key(this.uu[this.uu.length - 1].time)) return i ? this.uu.length - 1 : null;
    const n = Bt(this.uu, this.q_.key(t), (t2, i2) => this.q_.key(t2.time) < i2);
    return this.q_.key(t) < this.q_.key(this.uu[n].time) ? i ? n : null : n;
  }
  Ni() {
    return 0 === this.__ || 0 === this.uu.length || null === this._u;
  }
  Da() {
    return this.uu.length > 0;
  }
  Xs() {
    return this.Ru(), this.fu.lu();
  }
  Du() {
    return this.Ru(), this.fu.au();
  }
  Vu() {
    const t = this.Xs();
    if (null === t) return null;
    const i = { from: t.Os(), to: t.ui() };
    return this.Ou(i);
  }
  Ou(t) {
    const i = Math.round(t.from), n = Math.round(t.to), s = b(this.Bu()), e2 = b(this.Au());
    return { from: b(this.Ui(Math.max(s, i))), to: b(this.Ui(Math.min(e2, n))) };
  }
  Iu(t) {
    return { from: b(this.Va(t.from, true)), to: b(this.Va(t.to, true)) };
  }
  Hi() {
    return this.__;
  }
  S_(t) {
    if (!isFinite(t) || t <= 0) return;
    if (this.__ === t) return;
    const i = this.Du(), n = this.__;
    if (this.__ = t, this.vu = true, this.cn.lockVisibleTimeRangeOnResize && 0 !== n) {
      const i2 = this.Su * t / n;
      this.Su = i2;
    }
    if (this.cn.fixLeftEdge && null !== i && i.Os() <= 0) {
      const i2 = n - t;
      this.xu -= Math.round(i2 / this.Su) + 1, this.vu = true;
    }
    this.zu(), this.Lu();
  }
  It(t) {
    if (this.Ni() || !B(t)) return 0;
    const i = this.Eu() + this.xu - t;
    return this.__ - (i + 0.5) * this.Su - 1;
  }
  Qs(t, i) {
    const n = this.Eu(), s = void 0 === i ? 0 : i.from, e2 = void 0 === i ? t.length : i.to;
    for (let i2 = s; i2 < e2; i2++) {
      const s2 = t[i2].ot, e3 = n + this.xu - s2, r2 = this.__ - (e3 + 0.5) * this.Su - 1;
      t[i2].nt = r2;
    }
  }
  Nu(t) {
    return Math.ceil(this.Fu(t));
  }
  Jn(t) {
    this.vu = true, this.xu = t, this.Lu(), this.$i.Wu(), this.$i.Uh();
  }
  le() {
    return this.Su;
  }
  Gn(t) {
    this.ju(t), this.Lu(), this.$i.Wu(), this.$i.Uh();
  }
  Hu() {
    return this.xu;
  }
  Ha() {
    if (this.Ni()) return null;
    if (null !== this.gu) return this.gu;
    const t = this.Su, i = 5 * (this.$i.W().layout.fontSize + 4) / 8 * (this.cn.tickMarkMaxCharacterLength || 8), n = Math.round(i / t), s = b(this.Xs()), e2 = Math.max(s.Os(), s.Os() - n), r2 = Math.max(s.ui(), s.ui() - n), h2 = this.cu.su(t, i), l2 = this.Bu() + n, a2 = this.Au() - n, o2 = this.$u(), _2 = this.cn.fixLeftEdge || o2, u2 = this.cn.fixRightEdge || o2;
    let c2 = 0;
    for (const t2 of h2) {
      if (!(e2 <= t2.index && t2.index <= r2)) continue;
      let n2;
      c2 < this.Mu.length ? (n2 = this.Mu[c2], n2.coord = this.It(t2.index), n2.label = this.Uu(t2), n2.weight = t2.weight) : (n2 = { needAlignCoordinate: false, coord: this.It(t2.index), label: this.Uu(t2), weight: t2.weight }, this.Mu.push(n2)), this.Su > i / 2 && !o2 ? n2.needAlignCoordinate = false : n2.needAlignCoordinate = _2 && t2.index <= l2 || u2 && t2.index >= a2, c2++;
    }
    return this.Mu.length = c2, this.gu = this.Mu, this.Mu;
  }
  qu() {
    this.vu = true, this.Gn(this.cn.barSpacing), this.Jn(this.cn.rightOffset);
  }
  Yu(t) {
    this.vu = true, this._u = t, this.Lu(), this.Tu();
  }
  Zu(t, i) {
    const n = this.Fu(t), s = this.le(), e2 = s + i * (s / 10);
    this.Gn(e2), this.cn.rightBarStaysOnScroll || this.Jn(this.Hu() + (n - this.Fu(t)));
  }
  Go(t) {
    this.Mo && this.n_(), null === this.wo && null === this.wu && (this.Ni() || (this.wo = t, this.Xu()));
  }
  Jo(t) {
    if (null === this.wu) return;
    const i = kt(this.__ - t, 0, this.__), n = kt(this.__ - b(this.wo), 0, this.__);
    0 !== i && 0 !== n && this.Gn(this.wu.le * i / n);
  }
  Qo() {
    null !== this.wo && (this.wo = null, this.Ku());
  }
  t_(t) {
    null === this.Mo && null === this.wu && (this.Ni() || (this.Mo = t, this.Xu()));
  }
  i_(t) {
    if (null === this.Mo) return;
    const i = (this.Mo - t) / this.le();
    this.xu = b(this.wu).Hu + i, this.vu = true, this.Lu();
  }
  n_() {
    null !== this.Mo && (this.Mo = null, this.Ku());
  }
  Gu() {
    this.Ju(this.cn.rightOffset);
  }
  Ju(t, i = 400) {
    if (!isFinite(t)) throw new RangeError("offset is required and must be finite number");
    if (!isFinite(i) || i <= 0) throw new RangeError("animationDuration (optional) must be finite positive number");
    const n = this.xu, s = performance.now();
    this.$i.Zn({ Qu: (t2) => (t2 - s) / i >= 1, tc: (e2) => {
      const r2 = (e2 - s) / i;
      return r2 >= 1 ? t : n + (t - n) * r2;
    } });
  }
  bt(t, i) {
    this.vu = true, this.uu = t, this.cu.iu(t, i), this.Lu();
  }
  nc() {
    return this.pu;
  }
  sc() {
    return this.mu;
  }
  ec() {
    return this.bu;
  }
  Eu() {
    return this._u || 0;
  }
  rc(t) {
    const i = t.G_();
    this.ju(this.__ / i), this.xu = t.ui() - this.Eu(), this.Lu(), this.vu = true, this.$i.Wu(), this.$i.Uh();
  }
  hc() {
    const t = this.Bu(), i = this.Au();
    null !== t && null !== i && this.rc(new xn(t, i + this.cn.rightOffset));
  }
  lc(t) {
    const i = new xn(t.from, t.to);
    this.rc(i);
  }
  qi(t) {
    return void 0 !== this.yo.timeFormatter ? this.yo.timeFormatter(t.originalTime) : this.q_.formatHorzItem(t.time);
  }
  $u() {
    const { handleScroll: t, handleScale: i } = this.$i.W();
    return !(t.horzTouchDrag || t.mouseWheel || t.pressedMouseMove || t.vertTouchDrag || i.axisDoubleClickReset.time || i.axisPressedMouseMove.time || i.mouseWheel || i.pinch);
  }
  Bu() {
    return 0 === this.uu.length ? null : 0;
  }
  Au() {
    return 0 === this.uu.length ? null : this.uu.length - 1;
  }
  ac(t) {
    return (this.__ - 1 - t) / this.Su;
  }
  Fu(t) {
    const i = this.ac(t), n = this.Eu() + this.xu - i;
    return Math.round(1e6 * n) / 1e6;
  }
  ju(t) {
    const i = this.Su;
    this.Su = t, this.zu(), i !== this.Su && (this.vu = true, this.oc());
  }
  Ru() {
    if (!this.vu) return;
    if (this.vu = false, this.Ni()) return void this._c(yn.ou());
    const t = this.Eu(), i = this.__ / this.Su, n = this.xu + t, s = new xn(n - i + 1, n);
    this._c(new yn(s));
  }
  zu() {
    const t = this.uc();
    if (this.Su < t && (this.Su = t, this.vu = true), 0 !== this.__) {
      const t2 = 0.5 * this.__;
      this.Su > t2 && (this.Su = t2, this.vu = true);
    }
  }
  uc() {
    return this.cn.fixLeftEdge && this.cn.fixRightEdge && 0 !== this.uu.length ? this.__ / this.uu.length : this.cn.minBarSpacing;
  }
  Lu() {
    const t = this.cc();
    null !== t && this.xu < t && (this.xu = t, this.vu = true);
    const i = this.dc();
    this.xu > i && (this.xu = i, this.vu = true);
  }
  cc() {
    const t = this.Bu(), i = this._u;
    if (null === t || null === i) return null;
    return t - i - 1 + (this.cn.fixLeftEdge ? this.__ / this.Su : Math.min(2, this.uu.length));
  }
  dc() {
    return this.cn.fixRightEdge ? 0 : this.__ / this.Su - Math.min(2, this.uu.length);
  }
  Xu() {
    this.wu = { le: this.le(), Hu: this.Hu() };
  }
  Ku() {
    this.wu = null;
  }
  Uu(t) {
    let i = this.du.get(t.weight);
    return void 0 === i && (i = new Mn((t2) => this.fc(t2), this.q_), this.du.set(t.weight, i)), i.Y_(t);
  }
  fc(t) {
    return this.q_.formatTickmark(t, this.yo);
  }
  _c(t) {
    const i = this.fu;
    this.fu = t, Sn(i.lu(), this.fu.lu()) || this.pu.m(), Sn(i.au(), this.fu.au()) || this.mu.m(), this.oc();
  }
  oc() {
    this.gu = null;
  }
  Cu() {
    this.oc(), this.du.clear();
  }
  ku() {
    this.q_.updateFormatter(this.yo);
  }
  Tu() {
    if (!this.cn.fixLeftEdge) return;
    const t = this.Bu();
    if (null === t) return;
    const i = this.Xs();
    if (null === i) return;
    const n = i.Os() - t;
    if (n < 0) {
      const t2 = this.xu - n - 1;
      this.Jn(t2);
    }
    this.zu();
  }
  Pu() {
    this.Lu(), this.zu();
  }
}
class Pn {
  X(t, i, n) {
    t.useMediaCoordinateSpace((t2) => this.K(t2, i, n));
  }
  gl(t, i, n) {
    t.useMediaCoordinateSpace((t2) => this.vc(t2, i, n));
  }
  vc(t, i, n) {
  }
}
class Rn extends Pn {
  constructor(t) {
    super(), this.mc = /* @__PURE__ */ new Map(), this.zt = t;
  }
  K(t) {
  }
  vc(t) {
    if (!this.zt.yt) return;
    const { context: i, mediaSize: n } = t;
    let s = 0;
    for (const t2 of this.zt.bc) {
      if (0 === t2.Kt.length) continue;
      i.font = t2.R;
      const e3 = this.wc(i, t2.Kt);
      e3 > n.width ? t2.Zu = n.width / e3 : t2.Zu = 1, s += t2.gc * t2.Zu;
    }
    let e2 = 0;
    switch (this.zt.Mc) {
      case "top":
        e2 = 0;
        break;
      case "center":
        e2 = Math.max((n.height - s) / 2, 0);
        break;
      case "bottom":
        e2 = Math.max(n.height - s, 0);
    }
    i.fillStyle = this.zt.V;
    for (const t2 of this.zt.bc) {
      i.save();
      let s2 = 0;
      switch (this.zt.xc) {
        case "left":
          i.textAlign = "left", s2 = t2.gc / 2;
          break;
        case "center":
          i.textAlign = "center", s2 = n.width / 2;
          break;
        case "right":
          i.textAlign = "right", s2 = n.width - 1 - t2.gc / 2;
      }
      i.translate(s2, e2), i.textBaseline = "top", i.font = t2.R, i.scale(t2.Zu, t2.Zu), i.fillText(t2.Kt, 0, t2.Sc), i.restore(), e2 += t2.gc * t2.Zu;
    }
  }
  wc(t, i) {
    const n = this.kc(t.font);
    let s = n.get(i);
    return void 0 === s && (s = t.measureText(i).width, n.set(i, s)), s;
  }
  kc(t) {
    let i = this.mc.get(t);
    return void 0 === i && (i = /* @__PURE__ */ new Map(), this.mc.set(t, i)), i;
  }
}
class Dn {
  constructor(t) {
    this.ft = true, this.Ft = { yt: false, V: "", bc: [], Mc: "center", xc: "center" }, this.Wt = new Rn(this.Ft), this.jt = t;
  }
  bt() {
    this.ft = true;
  }
  gt() {
    return this.ft && (this.Mt(), this.ft = false), this.Wt;
  }
  Mt() {
    const t = this.jt.W(), i = this.Ft;
    i.yt = t.visible, i.yt && (i.V = t.color, i.xc = t.horzAlign, i.Mc = t.vertAlign, i.bc = [{ Kt: t.text, R: F(t.fontSize, t.fontFamily, t.fontStyle), gc: 1.2 * t.fontSize, Sc: 0, Zu: 0 }]);
  }
}
class Vn extends lt {
  constructor(t, i) {
    super(), this.cn = i, this.wn = new Dn(this);
  }
  Rn() {
    return [];
  }
  Pn() {
    return [this.wn];
  }
  W() {
    return this.cn;
  }
  Vn() {
    this.wn.bt();
  }
}
var On, Bn, An, In, zn;
!function(t) {
  t[t.OnTouchEnd = 0] = "OnTouchEnd", t[t.OnNextTap = 1] = "OnNextTap";
}(On || (On = {}));
class Ln {
  constructor(t, i, n) {
    this.yc = [], this.Cc = [], this.__ = 0, this.Tc = null, this.Pc = new D(), this.Rc = new D(), this.Dc = null, this.Vc = t, this.cn = i, this.q_ = n, this.Oc = new W(this), this.yl = new Tn(this, i.timeScale, this.cn.localization, n), this.vt = new ot(this, i.crosshair), this.Bc = new Ji(i.crosshair), this.Ac = new Vn(this, i.watermark), this.Ic(), this.yc[0].x_(2e3), this.zc = this.Lc(0), this.Ec = this.Lc(1);
  }
  Kl() {
    this.Nc(ut.es());
  }
  Uh() {
    this.Nc(ut.ss());
  }
  oa() {
    this.Nc(new ut(1));
  }
  Gl(t) {
    const i = this.Fc(t);
    this.Nc(i);
  }
  Wc() {
    return this.Tc;
  }
  jc(t) {
    const i = this.Tc;
    this.Tc = t, null !== i && this.Gl(i.Hc), null !== t && this.Gl(t.Hc);
  }
  W() {
    return this.cn;
  }
  $h(t) {
    V(this.cn, t), this.yc.forEach((i) => i.b_(t)), void 0 !== t.timeScale && this.yl.$h(t.timeScale), void 0 !== t.localization && this.yl.yu(t.localization), (t.leftPriceScale || t.rightPriceScale) && this.Pc.m(), this.zc = this.Lc(0), this.Ec = this.Lc(1), this.Kl();
  }
  $c(t, i) {
    if ("left" === t) return void this.$h({ leftPriceScale: i });
    if ("right" === t) return void this.$h({ rightPriceScale: i });
    const n = this.Uc(t);
    null !== n && (n.Dt.$h(i), this.Pc.m());
  }
  Uc(t) {
    for (const i of this.yc) {
      const n = i.w_(t);
      if (null !== n) return { Ht: i, Dt: n };
    }
    return null;
  }
  St() {
    return this.yl;
  }
  qc() {
    return this.yc;
  }
  Yc() {
    return this.Ac;
  }
  Zc() {
    return this.vt;
  }
  Xc() {
    return this.Rc;
  }
  Kc(t, i) {
    t.Lo(i), this.Wu();
  }
  S_(t) {
    this.__ = t, this.yl.S_(this.__), this.yc.forEach((i) => i.S_(t)), this.Wu();
  }
  Ic(t) {
    const i = new gn(this.yl, this);
    void 0 !== t ? this.yc.splice(t, 0, i) : this.yc.push(i);
    const n = void 0 === t ? this.yc.length - 1 : t, s = ut.es();
    return s.Nn(n, { Fn: 0, Wn: true }), this.Nc(s), i;
  }
  V_(t, i, n) {
    t.V_(i, n);
  }
  O_(t, i, n) {
    t.O_(i, n), this.Jl(), this.Nc(this.Gc(t, 2));
  }
  B_(t, i) {
    t.B_(i), this.Nc(this.Gc(t, 2));
  }
  A_(t, i, n) {
    i.Vo() || t.A_(i, n);
  }
  I_(t, i, n) {
    i.Vo() || (t.I_(i, n), this.Jl(), this.Nc(this.Gc(t, 2)));
  }
  z_(t, i) {
    i.Vo() || (t.z_(i), this.Nc(this.Gc(t, 2)));
  }
  E_(t, i) {
    t.E_(i), this.Nc(this.Gc(t, 2));
  }
  Jc(t) {
    this.yl.Go(t);
  }
  Qc(t, i) {
    const n = this.St();
    if (n.Ni() || 0 === i) return;
    const s = n.Hi();
    t = Math.max(1, Math.min(t, s)), n.Zu(t, i), this.Wu();
  }
  td(t) {
    this.nd(0), this.sd(t), this.ed();
  }
  rd(t) {
    this.yl.Jo(t), this.Wu();
  }
  hd() {
    this.yl.Qo(), this.Uh();
  }
  nd(t) {
    this.yl.t_(t);
  }
  sd(t) {
    this.yl.i_(t), this.Wu();
  }
  ed() {
    this.yl.n_(), this.Uh();
  }
  wt() {
    return this.Cc;
  }
  ld(t, i, n, s, e2) {
    this.vt.gn(t, i);
    let r2 = NaN, h2 = this.yl.Nu(t);
    const l2 = this.yl.Xs();
    null !== l2 && (h2 = Math.min(Math.max(l2.Os(), h2), l2.ui()));
    const a2 = s.vn(), o2 = a2.Ct();
    null !== o2 && (r2 = a2.pn(i, o2)), r2 = this.Bc.Oa(r2, h2, s), this.vt.kn(h2, r2, s), this.oa(), e2 || this.Rc.m(this.vt.xt(), { x: t, y: i }, n);
  }
  ad(t, i, n) {
    const s = n.vn(), e2 = s.Ct(), r2 = s.Rt(t, b(e2)), h2 = this.yl.Va(i, true), l2 = this.yl.It(b(h2));
    this.ld(l2, r2, null, n, true);
  }
  od(t) {
    this.Zc().Cn(), this.oa(), t || this.Rc.m(null, null, null);
  }
  Jl() {
    const t = this.vt.Ht();
    if (null !== t) {
      const i = this.vt.xn(), n = this.vt.Sn();
      this.ld(i, n, null, t);
    }
    this.vt.Vn();
  }
  _d(t, i, n) {
    const s = this.yl.mn(0);
    void 0 !== i && void 0 !== n && this.yl.bt(i, n);
    const e2 = this.yl.mn(0), r2 = this.yl.Eu(), h2 = this.yl.Xs();
    if (null !== h2 && null !== s && null !== e2) {
      const i2 = h2.Kr(r2), l2 = this.q_.key(s) > this.q_.key(e2), a2 = null !== t && t > r2 && !l2, o2 = this.yl.W().allowShiftVisibleRangeOnWhitespaceReplacement, _2 = i2 && (!(void 0 === n) || o2) && this.yl.W().shiftVisibleRangeOnNewBar;
      if (a2 && !_2) {
        const i3 = t - r2;
        this.yl.Jn(this.yl.Hu() - i3);
      }
    }
    this.yl.Yu(t);
  }
  ia(t) {
    null !== t && t.F_();
  }
  dr(t) {
    const i = this.yc.find((i2) => i2.Uo().includes(t));
    return void 0 === i ? null : i;
  }
  Wu() {
    this.Ac.Vn(), this.yc.forEach((t) => t.F_()), this.Jl();
  }
  S() {
    this.yc.forEach((t) => t.S()), this.yc.length = 0, this.cn.localization.priceFormatter = void 0, this.cn.localization.percentageFormatter = void 0, this.cn.localization.timeFormatter = void 0;
  }
  ud() {
    return this.Oc;
  }
  br() {
    return this.Oc.W();
  }
  g_() {
    return this.Pc;
  }
  dd(t, i, n) {
    const s = this.yc[0], e2 = this.fd(i, t, s, n);
    return this.Cc.push(e2), 1 === this.Cc.length ? this.Kl() : this.Uh(), e2;
  }
  vd(t) {
    const i = this.dr(t), n = this.Cc.indexOf(t);
    p(-1 !== n, "Series not found"), this.Cc.splice(n, 1), b(i).Zo(t), t.S && t.S();
  }
  Xl(t, i) {
    const n = b(this.dr(t));
    n.Zo(t);
    const s = this.Uc(i);
    if (null === s) {
      const s2 = t.Xi();
      n.qo(t, i, s2);
    } else {
      const e2 = s.Ht === n ? t.Xi() : void 0;
      s.Ht.qo(t, i, e2);
    }
  }
  hc() {
    const t = ut.ss();
    t.$n(), this.Nc(t);
  }
  pd(t) {
    const i = ut.ss();
    i.Yn(t), this.Nc(i);
  }
  Kn() {
    const t = ut.ss();
    t.Kn(), this.Nc(t);
  }
  Gn(t) {
    const i = ut.ss();
    i.Gn(t), this.Nc(i);
  }
  Jn(t) {
    const i = ut.ss();
    i.Jn(t), this.Nc(i);
  }
  Zn(t) {
    const i = ut.ss();
    i.Zn(t), this.Nc(i);
  }
  Un() {
    const t = ut.ss();
    t.Un(), this.Nc(t);
  }
  md() {
    return this.cn.rightPriceScale.visible ? "right" : "left";
  }
  bd() {
    return this.Ec;
  }
  q() {
    return this.zc;
  }
  Bt(t) {
    const i = this.Ec, n = this.zc;
    if (i === n) return i;
    if (t = Math.max(0, Math.min(100, Math.round(100 * t))), null === this.Dc || this.Dc.Ps !== n || this.Dc.Rs !== i) this.Dc = { Ps: n, Rs: i, wd: /* @__PURE__ */ new Map() };
    else {
      const i2 = this.Dc.wd.get(t);
      if (void 0 !== i2) return i2;
    }
    const s = function(t2, i2, n2) {
      const [s2, e2, r2, h2] = T(t2), [l2, a2, o2, _2] = T(i2), u2 = [M(s2 + n2 * (l2 - s2)), M(e2 + n2 * (a2 - e2)), M(r2 + n2 * (o2 - r2)), x(h2 + n2 * (_2 - h2))];
      return `rgba(${u2[0]}, ${u2[1]}, ${u2[2]}, ${u2[3]})`;
    }(n, i, t / 100);
    return this.Dc.wd.set(t, s), s;
  }
  Gc(t, i) {
    const n = new ut(i);
    if (null !== t) {
      const s = this.yc.indexOf(t);
      n.Nn(s, { Fn: i });
    }
    return n;
  }
  Fc(t, i) {
    return void 0 === i && (i = 2), this.Gc(this.dr(t), i);
  }
  Nc(t) {
    this.Vc && this.Vc(t), this.yc.forEach((t2) => t2.j_().qh().bt());
  }
  fd(t, i, n, s) {
    const e2 = new Gi(this, t, i, n, s), r2 = void 0 !== t.priceScaleId ? t.priceScaleId : this.md();
    return n.qo(e2, r2), _t(r2) || e2.$h(t), e2;
  }
  Lc(t) {
    const i = this.cn.layout;
    return "gradient" === i.background.type ? 0 === t ? i.background.topColor : i.background.bottomColor : i.background.color;
  }
}
function En(t) {
  return !O(t) && !A(t);
}
function Nn(t) {
  return O(t);
}
!function(t) {
  t[t.Disabled = 0] = "Disabled", t[t.Continuous = 1] = "Continuous", t[t.OnDataUpdate = 2] = "OnDataUpdate";
}(Bn || (Bn = {})), function(t) {
  t[t.LastBar = 0] = "LastBar", t[t.LastVisible = 1] = "LastVisible";
}(An || (An = {})), function(t) {
  t.Solid = "solid", t.VerticalGradient = "gradient";
}(In || (In = {})), function(t) {
  t[t.Year = 0] = "Year", t[t.Month = 1] = "Month", t[t.DayOfMonth = 2] = "DayOfMonth", t[t.Time = 3] = "Time", t[t.TimeWithSeconds = 4] = "TimeWithSeconds";
}(zn || (zn = {}));
const Fn = (t) => t.getUTCFullYear();
function Wn(t, i, n) {
  return i.replace(/yyyy/g, ((t2) => dt(Fn(t2), 4))(t)).replace(/yy/g, ((t2) => dt(Fn(t2) % 100, 2))(t)).replace(/MMMM/g, ((t2, i2) => new Date(t2.getUTCFullYear(), t2.getUTCMonth(), 1).toLocaleString(i2, { month: "long" }))(t, n)).replace(/MMM/g, ((t2, i2) => new Date(t2.getUTCFullYear(), t2.getUTCMonth(), 1).toLocaleString(i2, { month: "short" }))(t, n)).replace(/MM/g, ((t2) => dt(((t3) => t3.getUTCMonth() + 1)(t2), 2))(t)).replace(/dd/g, ((t2) => dt(((t3) => t3.getUTCDate())(t2), 2))(t));
}
class jn {
  constructor(t = "yyyy-MM-dd", i = "default") {
    this.gd = t, this.Md = i;
  }
  Y_(t) {
    return Wn(t, this.gd, this.Md);
  }
}
class Hn {
  constructor(t) {
    this.xd = t || "%h:%m:%s";
  }
  Y_(t) {
    return this.xd.replace("%h", dt(t.getUTCHours(), 2)).replace("%m", dt(t.getUTCMinutes(), 2)).replace("%s", dt(t.getUTCSeconds(), 2));
  }
}
const $n = { Sd: "yyyy-MM-dd", kd: "%h:%m:%s", yd: " ", Cd: "default" };
class Un {
  constructor(t = {}) {
    const i = Object.assign(Object.assign({}, $n), t);
    this.Td = new jn(i.Sd, i.Cd), this.Pd = new Hn(i.kd), this.Rd = i.yd;
  }
  Y_(t) {
    return `${this.Td.Y_(t)}${this.Rd}${this.Pd.Y_(t)}`;
  }
}
function qn(t) {
  return 60 * t * 60 * 1e3;
}
function Yn(t) {
  return 60 * t * 1e3;
}
const Zn = [{ Dd: (Xn = 1, 1e3 * Xn), Vd: 10 }, { Dd: Yn(1), Vd: 20 }, { Dd: Yn(5), Vd: 21 }, { Dd: Yn(30), Vd: 22 }, { Dd: qn(1), Vd: 30 }, { Dd: qn(3), Vd: 31 }, { Dd: qn(6), Vd: 32 }, { Dd: qn(12), Vd: 33 }];
var Xn;
function Kn(t, i) {
  if (t.getUTCFullYear() !== i.getUTCFullYear()) return 70;
  if (t.getUTCMonth() !== i.getUTCMonth()) return 60;
  if (t.getUTCDate() !== i.getUTCDate()) return 50;
  for (let n = Zn.length - 1; n >= 0; --n) if (Math.floor(i.getTime() / Zn[n].Dd) !== Math.floor(t.getTime() / Zn[n].Dd)) return Zn[n].Vd;
  return 0;
}
function Gn(t) {
  let i = t;
  if (A(t) && (i = Qn(t)), !En(i)) throw new Error("time must be of type BusinessDay");
  const n = new Date(Date.UTC(i.year, i.month - 1, i.day, 0, 0, 0, 0));
  return { Od: Math.round(n.getTime() / 1e3), Bd: i };
}
function Jn(t) {
  if (!Nn(t)) throw new Error("time must be of type isUTCTimestamp");
  return { Od: t };
}
function Qn(t) {
  const i = new Date(t);
  if (isNaN(i.getTime())) throw new Error(`Invalid date string=${t}, expected format=yyyy-mm-dd`);
  return { day: i.getUTCDate(), month: i.getUTCMonth() + 1, year: i.getUTCFullYear() };
}
function ts(t) {
  A(t.time) && (t.time = Qn(t.time));
}
class is {
  options() {
    return this.cn;
  }
  setOptions(t) {
    this.cn = t, this.updateFormatter(t.localization);
  }
  preprocessData(t) {
    Array.isArray(t) ? function(t2) {
      t2.forEach(ts);
    }(t) : ts(t);
  }
  createConverterToInternalObj(t) {
    return b(function(t2) {
      return 0 === t2.length ? null : En(t2[0].time) || A(t2[0].time) ? Gn : Jn;
    }(t));
  }
  key(t) {
    return "object" == typeof t && "Od" in t ? t.Od : this.key(this.convertHorzItemToInternal(t));
  }
  cacheKey(t) {
    const i = t;
    return void 0 === i.Bd ? new Date(1e3 * i.Od).getTime() : new Date(Date.UTC(i.Bd.year, i.Bd.month - 1, i.Bd.day)).getTime();
  }
  convertHorzItemToInternal(t) {
    return Nn(i = t) ? Jn(i) : En(i) ? Gn(i) : Gn(Qn(i));
    var i;
  }
  updateFormatter(t) {
    if (!this.cn) return;
    const i = t.dateFormat;
    this.cn.timeScale.timeVisible ? this.Ad = new Un({ Sd: i, kd: this.cn.timeScale.secondsVisible ? "%h:%m:%s" : "%h:%m", yd: "   ", Cd: t.locale }) : this.Ad = new jn(i, t.locale);
  }
  formatHorzItem(t) {
    const i = t;
    return this.Ad.Y_(new Date(1e3 * i.Od));
  }
  formatTickmark(t, i) {
    const n = function(t2, i2, n2) {
      switch (t2) {
        case 0:
        case 10:
          return i2 ? n2 ? 4 : 3 : 2;
        case 20:
        case 21:
        case 22:
        case 30:
        case 31:
        case 32:
        case 33:
          return i2 ? 3 : 2;
        case 50:
          return 2;
        case 60:
          return 1;
        case 70:
          return 0;
      }
    }(t.weight, this.cn.timeScale.timeVisible, this.cn.timeScale.secondsVisible), s = this.cn.timeScale;
    if (void 0 !== s.tickMarkFormatter) {
      const e2 = s.tickMarkFormatter(t.originalTime, n, i.locale);
      if (null !== e2) return e2;
    }
    return function(t2, i2, n2) {
      const s2 = {};
      switch (i2) {
        case 0:
          s2.year = "numeric";
          break;
        case 1:
          s2.month = "short";
          break;
        case 2:
          s2.day = "numeric";
          break;
        case 3:
          s2.hour12 = false, s2.hour = "2-digit", s2.minute = "2-digit";
          break;
        case 4:
          s2.hour12 = false, s2.hour = "2-digit", s2.minute = "2-digit", s2.second = "2-digit";
      }
      const e2 = void 0 === t2.Bd ? new Date(1e3 * t2.Od) : new Date(Date.UTC(t2.Bd.year, t2.Bd.month - 1, t2.Bd.day));
      return new Date(e2.getUTCFullYear(), e2.getUTCMonth(), e2.getUTCDate(), e2.getUTCHours(), e2.getUTCMinutes(), e2.getUTCSeconds(), e2.getUTCMilliseconds()).toLocaleString(n2, s2);
    }(t.time, n, i.locale);
  }
  maxTickMarkWeight(t) {
    let i = t.reduce(Cn, t[0]).weight;
    return i > 30 && i < 50 && (i = 30), i;
  }
  fillWeightsForPoints(t, i) {
    !function(t2, i2 = 0) {
      if (0 === t2.length) return;
      let n = 0 === i2 ? null : t2[i2 - 1].time.Od, s = null !== n ? new Date(1e3 * n) : null, e2 = 0;
      for (let r2 = i2; r2 < t2.length; ++r2) {
        const i3 = t2[r2], h2 = new Date(1e3 * i3.time.Od);
        null !== s && (i3.timeWeight = Kn(h2, s)), e2 += i3.time.Od - (n || i3.time.Od), n = i3.time.Od, s = h2;
      }
      if (0 === i2 && t2.length > 1) {
        const i3 = Math.ceil(e2 / (t2.length - 1)), n2 = new Date(1e3 * (t2[0].time.Od - i3));
        t2[0].timeWeight = Kn(new Date(1e3 * t2[0].time.Od), n2);
      }
    }(t, i);
  }
  static Id(t) {
    return V({ localization: { dateFormat: "dd MMM 'yy" } }, null != t ? t : {});
  }
}
const ns = "undefined" != typeof window;
function ss() {
  return !!ns && window.navigator.userAgent.toLowerCase().indexOf("firefox") > -1;
}
function es() {
  return !!ns && /iPhone|iPad|iPod/.test(window.navigator.platform);
}
function rs(t) {
  return t + t % 2;
}
function hs(t, i) {
  return t.zd - i.zd;
}
function ls(t, i, n) {
  const s = (t.zd - i.zd) / (t.ot - i.ot);
  return Math.sign(s) * Math.min(Math.abs(s), n);
}
class as {
  constructor(t, i, n, s) {
    this.Ld = null, this.Ed = null, this.Nd = null, this.Fd = null, this.Wd = null, this.jd = 0, this.Hd = 0, this.$d = t, this.Ud = i, this.qd = n, this.rs = s;
  }
  Yd(t, i) {
    if (null !== this.Ld) {
      if (this.Ld.ot === i) return void (this.Ld.zd = t);
      if (Math.abs(this.Ld.zd - t) < this.rs) return;
    }
    this.Fd = this.Nd, this.Nd = this.Ed, this.Ed = this.Ld, this.Ld = { ot: i, zd: t };
  }
  Vr(t, i) {
    if (null === this.Ld || null === this.Ed) return;
    if (i - this.Ld.ot > 50) return;
    let n = 0;
    const s = ls(this.Ld, this.Ed, this.Ud), e2 = hs(this.Ld, this.Ed), r2 = [s], h2 = [e2];
    if (n += e2, null !== this.Nd) {
      const t2 = ls(this.Ed, this.Nd, this.Ud);
      if (Math.sign(t2) === Math.sign(s)) {
        const i2 = hs(this.Ed, this.Nd);
        if (r2.push(t2), h2.push(i2), n += i2, null !== this.Fd) {
          const t3 = ls(this.Nd, this.Fd, this.Ud);
          if (Math.sign(t3) === Math.sign(s)) {
            const i3 = hs(this.Nd, this.Fd);
            r2.push(t3), h2.push(i3), n += i3;
          }
        }
      }
    }
    let l2 = 0;
    for (let t2 = 0; t2 < r2.length; ++t2) l2 += h2[t2] / n * r2[t2];
    Math.abs(l2) < this.$d || (this.Wd = { zd: t, ot: i }, this.Hd = l2, this.jd = function(t2, i2) {
      const n2 = Math.log(i2);
      return Math.log(1 * n2 / -t2) / n2;
    }(Math.abs(l2), this.qd));
  }
  tc(t) {
    const i = b(this.Wd), n = t - i.ot;
    return i.zd + this.Hd * (Math.pow(this.qd, n) - 1) / Math.log(this.qd);
  }
  Qu(t) {
    return null === this.Wd || this.Zd(t) === this.jd;
  }
  Zd(t) {
    const i = t - b(this.Wd).ot;
    return Math.min(i, this.jd);
  }
}
class os {
  constructor(t, i) {
    this.Xd = void 0, this.Kd = void 0, this.Gd = void 0, this.en = false, this.Jd = t, this.Qd = i, this.tf();
  }
  bt() {
    this.tf();
  }
  if() {
    this.Xd && this.Jd.removeChild(this.Xd), this.Kd && this.Jd.removeChild(this.Kd), this.Xd = void 0, this.Kd = void 0;
  }
  nf() {
    return this.en !== this.sf() || this.Gd !== this.ef();
  }
  ef() {
    return P(T(this.Qd.W().layout.textColor)) > 160 ? "dark" : "light";
  }
  sf() {
    return this.Qd.W().layout.attributionLogo;
  }
  rf() {
    const t = new URL(location.href);
    return t.hostname ? "&utm_source=" + t.hostname + t.pathname : "";
  }
  tf() {
    this.nf() && (this.if(), this.en = this.sf(), this.en && (this.Gd = this.ef(), this.Kd = document.createElement("style"), this.Kd.innerText = "a#tv-attr-logo{--fill:#131722;--stroke:#fff;position:absolute;left:10px;bottom:10px;height:19px;width:35px;margin:0;padding:0;border:0;z-index:3;}a#tv-attr-logo[data-dark]{--fill:#D1D4DC;--stroke:#131722;}", this.Xd = document.createElement("a"), this.Xd.href = `https://www.tradingview.com/?utm_medium=lwc-link&utm_campaign=lwc-chart${this.rf()}`, this.Xd.title = "Charting by TradingView", this.Xd.id = "tv-attr-logo", this.Xd.target = "_blank", this.Xd.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 35 19" width="35" height="19" fill="none"><g fill-rule="evenodd" clip-path="url(#a)" clip-rule="evenodd"><path fill="var(--stroke)" d="M2 0H0v10h6v9h21.4l.5-1.3 6-15 1-2.7H23.7l-.5 1.3-.2.6a5 5 0 0 0-7-.9V0H2Zm20 17h4l5.2-13 .8-2h-7l-1 2.5-.2.5-1.5 3.8-.3.7V17Zm-.8-10a3 3 0 0 0 .7-2.7A3 3 0 1 0 16.8 7h4.4ZM14 7V2H2v6h6v9h4V7h2Z"/><path fill="var(--fill)" d="M14 2H2v6h6v9h6V2Zm12 15h-7l6-15h7l-6 15Zm-7-9a3 3 0 1 0 0-6 3 3 0 0 0 0 6Z"/></g><defs><clipPath id="a"><path fill="var(--stroke)" d="M0 0h35v19H0z"/></clipPath></defs></svg>', this.Xd.toggleAttribute("data-dark", "dark" === this.Gd), this.Jd.appendChild(this.Kd), this.Jd.appendChild(this.Xd)));
  }
}
function _s(t, n) {
  const s = b(t.ownerDocument).createElement("canvas");
  t.appendChild(s);
  const e2 = bindTo(s, { options: { allowResizeObserver: false }, transform: (t2, i) => ({ width: Math.max(t2.width, i.width), height: Math.max(t2.height, i.height) }) });
  return e2.resizeCanvasElement(n), e2;
}
function us(t) {
  var i;
  t.width = 1, t.height = 1, null === (i = t.getContext("2d")) || void 0 === i || i.clearRect(0, 0, 1, 1);
}
function cs(t, i, n, s) {
  t.gl && t.gl(i, n, s);
}
function ds(t, i, n, s) {
  t.X(i, n, s);
}
function fs(t, i, n, s) {
  const e2 = t(n, s);
  for (const t2 of e2) {
    const n2 = t2.gt();
    null !== n2 && i(n2);
  }
}
function vs(t) {
  ns && void 0 !== window.chrome && t.addEventListener("mousedown", (t2) => {
    if (1 === t2.button) return t2.preventDefault(), false;
  });
}
class ps {
  constructor(t, i, n) {
    this.hf = 0, this.lf = null, this.af = { nt: Number.NEGATIVE_INFINITY, st: Number.POSITIVE_INFINITY }, this._f = 0, this.uf = null, this.cf = { nt: Number.NEGATIVE_INFINITY, st: Number.POSITIVE_INFINITY }, this.df = null, this.ff = false, this.vf = null, this.pf = null, this.mf = false, this.bf = false, this.wf = false, this.gf = null, this.Mf = null, this.xf = null, this.Sf = null, this.kf = null, this.yf = null, this.Cf = null, this.Tf = 0, this.Pf = false, this.Rf = false, this.Df = false, this.Vf = 0, this.Of = null, this.Bf = !es(), this.Af = (t2) => {
      this.If(t2);
    }, this.zf = (t2) => {
      if (this.Lf(t2)) {
        const i2 = this.Ef(t2);
        if (++this._f, this.uf && this._f > 1) {
          const { Nf: n2 } = this.Ff(ws(t2), this.cf);
          n2 < 30 && !this.wf && this.Wf(i2, this.Hf.jf), this.$f();
        }
      } else {
        const i2 = this.Ef(t2);
        if (++this.hf, this.lf && this.hf > 1) {
          const { Nf: n2 } = this.Ff(ws(t2), this.af);
          n2 < 5 && !this.bf && this.Uf(i2, this.Hf.qf), this.Yf();
        }
      }
    }, this.Zf = t, this.Hf = i, this.cn = n, this.Xf();
  }
  S() {
    null !== this.gf && (this.gf(), this.gf = null), null !== this.Mf && (this.Mf(), this.Mf = null), null !== this.Sf && (this.Sf(), this.Sf = null), null !== this.kf && (this.kf(), this.kf = null), null !== this.yf && (this.yf(), this.yf = null), null !== this.xf && (this.xf(), this.xf = null), this.Kf(), this.Yf();
  }
  Gf(t) {
    this.Sf && this.Sf();
    const i = this.Jf.bind(this);
    if (this.Sf = () => {
      this.Zf.removeEventListener("mousemove", i);
    }, this.Zf.addEventListener("mousemove", i), this.Lf(t)) return;
    const n = this.Ef(t);
    this.Uf(n, this.Hf.Qf), this.Bf = true;
  }
  Yf() {
    null !== this.lf && clearTimeout(this.lf), this.hf = 0, this.lf = null, this.af = { nt: Number.NEGATIVE_INFINITY, st: Number.POSITIVE_INFINITY };
  }
  $f() {
    null !== this.uf && clearTimeout(this.uf), this._f = 0, this.uf = null, this.cf = { nt: Number.NEGATIVE_INFINITY, st: Number.POSITIVE_INFINITY };
  }
  Jf(t) {
    if (this.Df || null !== this.pf) return;
    if (this.Lf(t)) return;
    const i = this.Ef(t);
    this.Uf(i, this.Hf.tv), this.Bf = true;
  }
  iv(t) {
    const i = Ms(t.changedTouches, b(this.Of));
    if (null === i) return;
    if (this.Vf = gs(t), null !== this.Cf) return;
    if (this.Rf) return;
    this.Pf = true;
    const n = this.Ff(ws(i), b(this.pf)), { nv: s, sv: e2, Nf: r2 } = n;
    if (this.mf || !(r2 < 5)) {
      if (!this.mf) {
        const t2 = 0.5 * s, i2 = e2 >= t2 && !this.cn.ev(), n2 = t2 > e2 && !this.cn.rv();
        i2 || n2 || (this.Rf = true), this.mf = true, this.wf = true, this.Kf(), this.$f();
      }
      if (!this.Rf) {
        const n2 = this.Ef(t, i);
        this.Wf(n2, this.Hf.hv), bs(t);
      }
    }
  }
  lv(t) {
    if (0 !== t.button) return;
    const i = this.Ff(ws(t), b(this.vf)), { Nf: n } = i;
    if (n >= 5 && (this.bf = true, this.Yf()), this.bf) {
      const i2 = this.Ef(t);
      this.Uf(i2, this.Hf.av);
    }
  }
  Ff(t, i) {
    const n = Math.abs(i.nt - t.nt), s = Math.abs(i.st - t.st);
    return { nv: n, sv: s, Nf: n + s };
  }
  ov(t) {
    let i = Ms(t.changedTouches, b(this.Of));
    if (null === i && 0 === t.touches.length && (i = t.changedTouches[0]), null === i) return;
    this.Of = null, this.Vf = gs(t), this.Kf(), this.pf = null, this.yf && (this.yf(), this.yf = null);
    const n = this.Ef(t, i);
    if (this.Wf(n, this.Hf._v), ++this._f, this.uf && this._f > 1) {
      const { Nf: t2 } = this.Ff(ws(i), this.cf);
      t2 < 30 && !this.wf && this.Wf(n, this.Hf.jf), this.$f();
    } else this.wf || (this.Wf(n, this.Hf.uv), this.Hf.uv && bs(t));
    0 === this._f && bs(t), 0 === t.touches.length && this.ff && (this.ff = false, bs(t));
  }
  If(t) {
    if (0 !== t.button) return;
    const i = this.Ef(t);
    if (this.vf = null, this.Df = false, this.kf && (this.kf(), this.kf = null), ss()) {
      this.Zf.ownerDocument.documentElement.removeEventListener("mouseleave", this.Af);
    }
    if (!this.Lf(t)) if (this.Uf(i, this.Hf.cv), ++this.hf, this.lf && this.hf > 1) {
      const { Nf: n } = this.Ff(ws(t), this.af);
      n < 5 && !this.bf && this.Uf(i, this.Hf.qf), this.Yf();
    } else this.bf || this.Uf(i, this.Hf.dv);
  }
  Kf() {
    null !== this.df && (clearTimeout(this.df), this.df = null);
  }
  fv(t) {
    if (null !== this.Of) return;
    const i = t.changedTouches[0];
    this.Of = i.identifier, this.Vf = gs(t);
    const n = this.Zf.ownerDocument.documentElement;
    this.wf = false, this.mf = false, this.Rf = false, this.pf = ws(i), this.yf && (this.yf(), this.yf = null);
    {
      const i2 = this.iv.bind(this), s2 = this.ov.bind(this);
      this.yf = () => {
        n.removeEventListener("touchmove", i2), n.removeEventListener("touchend", s2);
      }, n.addEventListener("touchmove", i2, { passive: false }), n.addEventListener("touchend", s2, { passive: false }), this.Kf(), this.df = setTimeout(this.vv.bind(this, t), 240);
    }
    const s = this.Ef(t, i);
    this.Wf(s, this.Hf.pv), this.uf || (this._f = 0, this.uf = setTimeout(this.$f.bind(this), 500), this.cf = ws(i));
  }
  mv(t) {
    if (0 !== t.button) return;
    const i = this.Zf.ownerDocument.documentElement;
    ss() && i.addEventListener("mouseleave", this.Af), this.bf = false, this.vf = ws(t), this.kf && (this.kf(), this.kf = null);
    {
      const t2 = this.lv.bind(this), n2 = this.If.bind(this);
      this.kf = () => {
        i.removeEventListener("mousemove", t2), i.removeEventListener("mouseup", n2);
      }, i.addEventListener("mousemove", t2), i.addEventListener("mouseup", n2);
    }
    if (this.Df = true, this.Lf(t)) return;
    const n = this.Ef(t);
    this.Uf(n, this.Hf.bv), this.lf || (this.hf = 0, this.lf = setTimeout(this.Yf.bind(this), 500), this.af = ws(t));
  }
  Xf() {
    this.Zf.addEventListener("mouseenter", this.Gf.bind(this)), this.Zf.addEventListener("touchcancel", this.Kf.bind(this));
    {
      const t = this.Zf.ownerDocument, i = (t2) => {
        this.Hf.wv && (t2.composed && this.Zf.contains(t2.composedPath()[0]) || t2.target && this.Zf.contains(t2.target) || this.Hf.wv());
      };
      this.Mf = () => {
        t.removeEventListener("touchstart", i);
      }, this.gf = () => {
        t.removeEventListener("mousedown", i);
      }, t.addEventListener("mousedown", i), t.addEventListener("touchstart", i, { passive: true });
    }
    es() && (this.xf = () => {
      this.Zf.removeEventListener("dblclick", this.zf);
    }, this.Zf.addEventListener("dblclick", this.zf)), this.Zf.addEventListener("mouseleave", this.gv.bind(this)), this.Zf.addEventListener("touchstart", this.fv.bind(this), { passive: true }), vs(this.Zf), this.Zf.addEventListener("mousedown", this.mv.bind(this)), this.Mv(), this.Zf.addEventListener("touchmove", () => {
    }, { passive: false });
  }
  Mv() {
    void 0 === this.Hf.xv && void 0 === this.Hf.Sv && void 0 === this.Hf.kv || (this.Zf.addEventListener("touchstart", (t) => this.yv(t.touches), { passive: true }), this.Zf.addEventListener("touchmove", (t) => {
      if (2 === t.touches.length && null !== this.Cf && void 0 !== this.Hf.Sv) {
        const i = ms(t.touches[0], t.touches[1]) / this.Tf;
        this.Hf.Sv(this.Cf, i), bs(t);
      }
    }, { passive: false }), this.Zf.addEventListener("touchend", (t) => {
      this.yv(t.touches);
    }));
  }
  yv(t) {
    1 === t.length && (this.Pf = false), 2 !== t.length || this.Pf || this.ff ? this.Cv() : this.Tv(t);
  }
  Tv(t) {
    const i = this.Zf.getBoundingClientRect() || { left: 0, top: 0 };
    this.Cf = { nt: (t[0].clientX - i.left + (t[1].clientX - i.left)) / 2, st: (t[0].clientY - i.top + (t[1].clientY - i.top)) / 2 }, this.Tf = ms(t[0], t[1]), void 0 !== this.Hf.xv && this.Hf.xv(), this.Kf();
  }
  Cv() {
    null !== this.Cf && (this.Cf = null, void 0 !== this.Hf.kv && this.Hf.kv());
  }
  gv(t) {
    if (this.Sf && this.Sf(), this.Lf(t)) return;
    if (!this.Bf) return;
    const i = this.Ef(t);
    this.Uf(i, this.Hf.Pv), this.Bf = !es();
  }
  vv(t) {
    const i = Ms(t.touches, b(this.Of));
    if (null === i) return;
    const n = this.Ef(t, i);
    this.Wf(n, this.Hf.Rv), this.wf = true, this.ff = true;
  }
  Lf(t) {
    return t.sourceCapabilities && void 0 !== t.sourceCapabilities.firesTouchEvents ? t.sourceCapabilities.firesTouchEvents : gs(t) < this.Vf + 500;
  }
  Wf(t, i) {
    i && i.call(this.Hf, t);
  }
  Uf(t, i) {
    i && i.call(this.Hf, t);
  }
  Ef(t, i) {
    const n = i || t, s = this.Zf.getBoundingClientRect() || { left: 0, top: 0 };
    return { clientX: n.clientX, clientY: n.clientY, pageX: n.pageX, pageY: n.pageY, screenX: n.screenX, screenY: n.screenY, localX: n.clientX - s.left, localY: n.clientY - s.top, ctrlKey: t.ctrlKey, altKey: t.altKey, shiftKey: t.shiftKey, metaKey: t.metaKey, Dv: !t.type.startsWith("mouse") && "contextmenu" !== t.type && "click" !== t.type, Vv: t.type, Ov: n.target, Bv: t.view, Av: () => {
      "touchstart" !== t.type && bs(t);
    } };
  }
}
function ms(t, i) {
  const n = t.clientX - i.clientX, s = t.clientY - i.clientY;
  return Math.sqrt(n * n + s * s);
}
function bs(t) {
  t.cancelable && t.preventDefault();
}
function ws(t) {
  return { nt: t.pageX, st: t.pageY };
}
function gs(t) {
  return t.timeStamp || performance.now();
}
function Ms(t, i) {
  for (let n = 0; n < t.length; ++n) if (t[n].identifier === i) return t[n];
  return null;
}
function xs(t) {
  return { Hc: t.Hc, Iv: { gr: t.zv.externalId }, Lv: t.zv.cursorStyle };
}
function Ss(t, i, n) {
  for (const s of t) {
    const t2 = s.gt();
    if (null !== t2 && t2.wr) {
      const e2 = t2.wr(i, n);
      if (null !== e2) return { Bv: s, Iv: e2 };
    }
  }
  return null;
}
function ks(t, i) {
  return (n) => {
    var s, e2, r2, h2;
    return (null !== (e2 = null === (s = n.Dt()) || void 0 === s ? void 0 : s.Pa()) && void 0 !== e2 ? e2 : "") !== i ? [] : null !== (h2 = null === (r2 = n.da) || void 0 === r2 ? void 0 : r2.call(n, t)) && void 0 !== h2 ? h2 : [];
  };
}
function ys(t, i, n, s) {
  if (!t.length) return;
  let e2 = 0;
  const r2 = n / 2, h2 = t[0].At(s, true);
  let l2 = 1 === i ? r2 - (t[0].Vi() - h2 / 2) : t[0].Vi() - h2 / 2 - r2;
  l2 = Math.max(0, l2);
  for (let r3 = 1; r3 < t.length; r3++) {
    const h3 = t[r3], a2 = t[r3 - 1], o2 = a2.At(s, false), _2 = h3.Vi(), u2 = a2.Vi();
    if (1 === i ? _2 > u2 - o2 : _2 < u2 + o2) {
      const s2 = u2 - o2 * i;
      h3.Oi(s2);
      const r4 = s2 - i * o2 / 2;
      if ((1 === i ? r4 < 0 : r4 > n) && l2 > 0) {
        const s3 = 1 === i ? -1 - r4 : r4 - n, h4 = Math.min(s3, l2);
        for (let n2 = e2; n2 < t.length; n2++) t[n2].Oi(t[n2].Vi() + i * h4);
        l2 -= h4;
      }
    } else e2 = r3, l2 = 1 === i ? u2 - o2 - _2 : _2 - (u2 + o2);
  }
}
class Cs {
  constructor(i, n, s, e2) {
    this.Li = null, this.Ev = null, this.Nv = false, this.Fv = new ni(200), this.Qr = null, this.Wv = 0, this.jv = false, this.Hv = () => {
      this.jv || this.tn.$v().$t().Uh();
    }, this.Uv = () => {
      this.jv || this.tn.$v().$t().Uh();
    }, this.tn = i, this.cn = n, this.ko = n.layout, this.Oc = s, this.qv = "left" === e2, this.Yv = ks("normal", e2), this.Zv = ks("top", e2), this.Xv = ks("bottom", e2), this.Kv = document.createElement("div"), this.Kv.style.height = "100%", this.Kv.style.overflow = "hidden", this.Kv.style.width = "25px", this.Kv.style.left = "0", this.Kv.style.position = "relative", this.Gv = _s(this.Kv, size({ width: 16, height: 16 })), this.Gv.subscribeSuggestedBitmapSizeChanged(this.Hv);
    const r2 = this.Gv.canvasElement;
    r2.style.position = "absolute", r2.style.zIndex = "1", r2.style.left = "0", r2.style.top = "0", this.Jv = _s(this.Kv, size({ width: 16, height: 16 })), this.Jv.subscribeSuggestedBitmapSizeChanged(this.Uv);
    const h2 = this.Jv.canvasElement;
    h2.style.position = "absolute", h2.style.zIndex = "2", h2.style.left = "0", h2.style.top = "0";
    const l2 = { bv: this.Qv.bind(this), pv: this.Qv.bind(this), av: this.tp.bind(this), hv: this.tp.bind(this), wv: this.ip.bind(this), cv: this.np.bind(this), _v: this.np.bind(this), qf: this.sp.bind(this), jf: this.sp.bind(this), Qf: this.ep.bind(this), Pv: this.rp.bind(this) };
    this.hp = new ps(this.Jv.canvasElement, l2, { ev: () => !this.cn.handleScroll.vertTouchDrag, rv: () => true });
  }
  S() {
    this.hp.S(), this.Jv.unsubscribeSuggestedBitmapSizeChanged(this.Uv), us(this.Jv.canvasElement), this.Jv.dispose(), this.Gv.unsubscribeSuggestedBitmapSizeChanged(this.Hv), us(this.Gv.canvasElement), this.Gv.dispose(), null !== this.Li && this.Li.Ko().p(this), this.Li = null;
  }
  lp() {
    return this.Kv;
  }
  P() {
    return this.ko.fontSize;
  }
  ap() {
    const t = this.Oc.W();
    return this.Qr !== t.R && (this.Fv.nr(), this.Qr = t.R), t;
  }
  op() {
    if (null === this.Li) return 0;
    let t = 0;
    const i = this.ap(), n = b(this.Gv.canvasElement.getContext("2d"));
    n.save();
    const s = this.Li.Ha();
    n.font = this._p(), s.length > 0 && (t = Math.max(this.Fv.xi(n, s[0].so), this.Fv.xi(n, s[s.length - 1].so)));
    const e2 = this.up();
    for (let i2 = e2.length; i2--; ) {
      const s2 = this.Fv.xi(n, e2[i2].Kt());
      s2 > t && (t = s2);
    }
    const r2 = this.Li.Ct();
    if (null !== r2 && null !== this.Ev && (2 !== (h2 = this.cn.crosshair).mode && h2.horzLine.visible && h2.horzLine.labelVisible)) {
      const i2 = this.Li.pn(1, r2), s2 = this.Li.pn(this.Ev.height - 2, r2);
      t = Math.max(t, this.Fv.xi(n, this.Li.Fi(Math.floor(Math.min(i2, s2)) + 0.11111111111111, r2)), this.Fv.xi(n, this.Li.Fi(Math.ceil(Math.max(i2, s2)) - 0.11111111111111, r2)));
    }
    var h2;
    n.restore();
    const l2 = t || 34;
    return rs(Math.ceil(i.C + i.T + i.A + i.I + 5 + l2));
  }
  cp(t) {
    null !== this.Ev && equalSizes(this.Ev, t) || (this.Ev = t, this.jv = true, this.Gv.resizeCanvasElement(t), this.Jv.resizeCanvasElement(t), this.jv = false, this.Kv.style.width = `${t.width}px`, this.Kv.style.height = `${t.height}px`);
  }
  dp() {
    return b(this.Ev).width;
  }
  Gi(t) {
    this.Li !== t && (null !== this.Li && this.Li.Ko().p(this), this.Li = t, t.Ko().l(this.fo.bind(this), this));
  }
  Dt() {
    return this.Li;
  }
  nr() {
    const t = this.tn.fp();
    this.tn.$v().$t().E_(t, b(this.Dt()));
  }
  vp(t) {
    if (null === this.Ev) return;
    if (1 !== t) {
      this.pp(), this.Gv.applySuggestedBitmapSize();
      const t2 = tryCreateCanvasRenderingTarget2D(this.Gv);
      null !== t2 && (t2.useBitmapCoordinateSpace((t3) => {
        this.mp(t3), this.Ie(t3);
      }), this.tn.bp(t2, this.Xv), this.wp(t2), this.tn.bp(t2, this.Yv), this.gp(t2));
    }
    this.Jv.applySuggestedBitmapSize();
    const i = tryCreateCanvasRenderingTarget2D(this.Jv);
    null !== i && (i.useBitmapCoordinateSpace(({ context: t2, bitmapSize: i2 }) => {
      t2.clearRect(0, 0, i2.width, i2.height);
    }), this.Mp(i), this.tn.bp(i, this.Zv));
  }
  xp() {
    return this.Gv.bitmapSize;
  }
  Sp(t, i, n) {
    const s = this.xp();
    s.width > 0 && s.height > 0 && t.drawImage(this.Gv.canvasElement, i, n);
  }
  bt() {
    var t;
    null === (t = this.Li) || void 0 === t || t.Ha();
  }
  Qv(t) {
    if (null === this.Li || this.Li.Ni() || !this.cn.handleScale.axisPressedMouseMove.price) return;
    const i = this.tn.$v().$t(), n = this.tn.fp();
    this.Nv = true, i.V_(n, this.Li, t.localY);
  }
  tp(t) {
    if (null === this.Li || !this.cn.handleScale.axisPressedMouseMove.price) return;
    const i = this.tn.$v().$t(), n = this.tn.fp(), s = this.Li;
    i.O_(n, s, t.localY);
  }
  ip() {
    if (null === this.Li || !this.cn.handleScale.axisPressedMouseMove.price) return;
    const t = this.tn.$v().$t(), i = this.tn.fp(), n = this.Li;
    this.Nv && (this.Nv = false, t.B_(i, n));
  }
  np(t) {
    if (null === this.Li || !this.cn.handleScale.axisPressedMouseMove.price) return;
    const i = this.tn.$v().$t(), n = this.tn.fp();
    this.Nv = false, i.B_(n, this.Li);
  }
  sp(t) {
    this.cn.handleScale.axisDoubleClickReset.price && this.nr();
  }
  ep(t) {
    if (null === this.Li) return;
    !this.tn.$v().$t().W().handleScale.axisPressedMouseMove.price || this.Li.Mh() || this.Li.Oo() || this.kp(1);
  }
  rp(t) {
    this.kp(0);
  }
  up() {
    const t = [], i = null === this.Li ? void 0 : this.Li;
    return ((n) => {
      for (let s = 0; s < n.length; ++s) {
        const e2 = n[s].Rn(this.tn.fp(), i);
        for (let i2 = 0; i2 < e2.length; i2++) t.push(e2[i2]);
      }
    })(this.tn.fp().Uo()), t;
  }
  mp({ context: t, bitmapSize: i }) {
    const { width: n, height: s } = i, e2 = this.tn.fp().$t(), r2 = e2.q(), h2 = e2.bd();
    r2 === h2 ? G(t, 0, 0, n, s, r2) : tt(t, 0, 0, n, s, r2, h2);
  }
  Ie({ context: t, bitmapSize: i, horizontalPixelRatio: n }) {
    if (null === this.Ev || null === this.Li || !this.Li.W().borderVisible) return;
    t.fillStyle = this.Li.W().borderColor;
    const s = Math.max(1, Math.floor(this.ap().C * n));
    let e2;
    e2 = this.qv ? i.width - s : 0, t.fillRect(e2, 0, s, i.height);
  }
  wp(t) {
    if (null === this.Ev || null === this.Li) return;
    const i = this.Li.Ha(), n = this.Li.W(), s = this.ap(), e2 = this.qv ? this.Ev.width - s.T : 0;
    n.borderVisible && n.ticksVisible && t.useBitmapCoordinateSpace(({ context: t2, horizontalPixelRatio: r2, verticalPixelRatio: h2 }) => {
      t2.fillStyle = n.borderColor;
      const l2 = Math.max(1, Math.floor(h2)), a2 = Math.floor(0.5 * h2), o2 = Math.round(s.T * r2);
      t2.beginPath();
      for (const n2 of i) t2.rect(Math.floor(e2 * r2), Math.round(n2.Ea * h2) - a2, o2, l2);
      t2.fill();
    }), t.useMediaCoordinateSpace(({ context: t2 }) => {
      var r2;
      t2.font = this._p(), t2.fillStyle = null !== (r2 = n.textColor) && void 0 !== r2 ? r2 : this.ko.textColor, t2.textAlign = this.qv ? "right" : "left", t2.textBaseline = "middle";
      const h2 = this.qv ? Math.round(e2 - s.A) : Math.round(e2 + s.T + s.A), l2 = i.map((i2) => this.Fv.Mi(t2, i2.so));
      for (let n2 = i.length; n2--; ) {
        const s2 = i[n2];
        t2.fillText(s2.so, h2, s2.Ea + l2[n2]);
      }
    });
  }
  pp() {
    if (null === this.Ev || null === this.Li) return;
    const t = [], i = this.Li.Uo().slice(), n = this.tn.fp(), s = this.ap();
    this.Li === n.pr() && this.tn.fp().Uo().forEach((t2) => {
      n.vr(t2) && i.push(t2);
    });
    const e2 = this.Li;
    i.forEach((i2) => {
      i2.Rn(n, e2).forEach((i3) => {
        i3.Oi(null), i3.Bi() && t.push(i3);
      });
    }), t.forEach((t2) => t2.Oi(t2.ki()));
    this.Li.W().alignLabels && this.yp(t, s);
  }
  yp(t, i) {
    if (null === this.Ev) return;
    const n = this.Ev.height / 2, s = t.filter((t2) => t2.ki() <= n), e2 = t.filter((t2) => t2.ki() > n);
    s.sort((t2, i2) => i2.ki() - t2.ki()), e2.sort((t2, i2) => t2.ki() - i2.ki());
    for (const n2 of t) {
      const t2 = Math.floor(n2.At(i) / 2), s2 = n2.ki();
      s2 > -t2 && s2 < t2 && n2.Oi(t2), s2 > this.Ev.height - t2 && s2 < this.Ev.height + t2 && n2.Oi(this.Ev.height - t2);
    }
    ys(s, 1, this.Ev.height, i), ys(e2, -1, this.Ev.height, i);
  }
  gp(t) {
    if (null === this.Ev) return;
    const i = this.up(), n = this.ap(), s = this.qv ? "right" : "left";
    i.forEach((i2) => {
      if (i2.Ai()) {
        i2.gt(b(this.Li)).X(t, n, this.Fv, s);
      }
    });
  }
  Mp(t) {
    if (null === this.Ev || null === this.Li) return;
    const i = this.tn.$v().$t(), n = [], s = this.tn.fp(), e2 = i.Zc().Rn(s, this.Li);
    e2.length && n.push(e2);
    const r2 = this.ap(), h2 = this.qv ? "right" : "left";
    n.forEach((i2) => {
      i2.forEach((i3) => {
        i3.gt(b(this.Li)).X(t, r2, this.Fv, h2);
      });
    });
  }
  kp(t) {
    this.Kv.style.cursor = 1 === t ? "ns-resize" : "default";
  }
  fo() {
    const t = this.op();
    this.Wv < t && this.tn.$v().$t().Kl(), this.Wv = t;
  }
  _p() {
    return F(this.ko.fontSize, this.ko.fontFamily);
  }
}
function Ts(t, i) {
  var n, s;
  return null !== (s = null === (n = t.ua) || void 0 === n ? void 0 : n.call(t, i)) && void 0 !== s ? s : [];
}
function Ps(t, i) {
  var n, s;
  return null !== (s = null === (n = t.Pn) || void 0 === n ? void 0 : n.call(t, i)) && void 0 !== s ? s : [];
}
function Rs(t, i) {
  var n, s;
  return null !== (s = null === (n = t.Ji) || void 0 === n ? void 0 : n.call(t, i)) && void 0 !== s ? s : [];
}
function Ds(t, i) {
  var n, s;
  return null !== (s = null === (n = t.aa) || void 0 === n ? void 0 : n.call(t, i)) && void 0 !== s ? s : [];
}
class Vs {
  constructor(i, n) {
    this.Ev = size({ width: 0, height: 0 }), this.Cp = null, this.Tp = null, this.Pp = null, this.Rp = null, this.Dp = false, this.Vp = new D(), this.Op = new D(), this.Bp = 0, this.Ap = false, this.Ip = null, this.zp = false, this.Lp = null, this.Ep = null, this.jv = false, this.Hv = () => {
      this.jv || null === this.Np || this.$i().Uh();
    }, this.Uv = () => {
      this.jv || null === this.Np || this.$i().Uh();
    }, this.Qd = i, this.Np = n, this.Np.W_().l(this.Fp.bind(this), this, true), this.Wp = document.createElement("td"), this.Wp.style.padding = "0", this.Wp.style.position = "relative";
    const s = document.createElement("div");
    s.style.width = "100%", s.style.height = "100%", s.style.position = "relative", s.style.overflow = "hidden", this.jp = document.createElement("td"), this.jp.style.padding = "0", this.Hp = document.createElement("td"), this.Hp.style.padding = "0", this.Wp.appendChild(s), this.Gv = _s(s, size({ width: 16, height: 16 })), this.Gv.subscribeSuggestedBitmapSizeChanged(this.Hv);
    const e2 = this.Gv.canvasElement;
    e2.style.position = "absolute", e2.style.zIndex = "1", e2.style.left = "0", e2.style.top = "0", this.Jv = _s(s, size({ width: 16, height: 16 })), this.Jv.subscribeSuggestedBitmapSizeChanged(this.Uv);
    const r2 = this.Jv.canvasElement;
    r2.style.position = "absolute", r2.style.zIndex = "2", r2.style.left = "0", r2.style.top = "0", this.$p = document.createElement("tr"), this.$p.appendChild(this.jp), this.$p.appendChild(this.Wp), this.$p.appendChild(this.Hp), this.Up(), this.hp = new ps(this.Jv.canvasElement, this, { ev: () => null === this.Ip && !this.Qd.W().handleScroll.vertTouchDrag, rv: () => null === this.Ip && !this.Qd.W().handleScroll.horzTouchDrag });
  }
  S() {
    null !== this.Cp && this.Cp.S(), null !== this.Tp && this.Tp.S(), this.Pp = null, this.Jv.unsubscribeSuggestedBitmapSizeChanged(this.Uv), us(this.Jv.canvasElement), this.Jv.dispose(), this.Gv.unsubscribeSuggestedBitmapSizeChanged(this.Hv), us(this.Gv.canvasElement), this.Gv.dispose(), null !== this.Np && this.Np.W_().p(this), this.hp.S();
  }
  fp() {
    return b(this.Np);
  }
  qp(t) {
    var i, n;
    null !== this.Np && this.Np.W_().p(this), this.Np = t, null !== this.Np && this.Np.W_().l(Vs.prototype.Fp.bind(this), this, true), this.Up(), this.Qd.Yp().indexOf(this) === this.Qd.Yp().length - 1 ? (this.Pp = null !== (i = this.Pp) && void 0 !== i ? i : new os(this.Wp, this.Qd), this.Pp.bt()) : (null === (n = this.Pp) || void 0 === n || n.if(), this.Pp = null);
  }
  $v() {
    return this.Qd;
  }
  lp() {
    return this.$p;
  }
  Up() {
    if (null !== this.Np && (this.Zp(), 0 !== this.$i().wt().length)) {
      if (null !== this.Cp) {
        const t = this.Np.R_();
        this.Cp.Gi(b(t));
      }
      if (null !== this.Tp) {
        const t = this.Np.D_();
        this.Tp.Gi(b(t));
      }
    }
  }
  Xp() {
    null !== this.Cp && this.Cp.bt(), null !== this.Tp && this.Tp.bt();
  }
  M_() {
    return null !== this.Np ? this.Np.M_() : 0;
  }
  x_(t) {
    this.Np && this.Np.x_(t);
  }
  Qf(t) {
    if (!this.Np) return;
    this.Kp();
    const i = t.localX, n = t.localY;
    this.Gp(i, n, t);
  }
  bv(t) {
    this.Kp(), this.Jp(), this.Gp(t.localX, t.localY, t);
  }
  tv(t) {
    var i;
    if (!this.Np) return;
    this.Kp();
    const n = t.localX, s = t.localY;
    this.Gp(n, s, t);
    const e2 = this.wr(n, s);
    this.Qd.Qp(null !== (i = null == e2 ? void 0 : e2.Lv) && void 0 !== i ? i : null), this.$i().jc(e2 && { Hc: e2.Hc, Iv: e2.Iv });
  }
  dv(t) {
    null !== this.Np && (this.Kp(), this.tm(t));
  }
  qf(t) {
    null !== this.Np && this.im(this.Op, t);
  }
  jf(t) {
    this.qf(t);
  }
  av(t) {
    this.Kp(), this.nm(t), this.Gp(t.localX, t.localY, t);
  }
  cv(t) {
    null !== this.Np && (this.Kp(), this.Ap = false, this.sm(t));
  }
  uv(t) {
    null !== this.Np && this.tm(t);
  }
  Rv(t) {
    if (this.Ap = true, null === this.Ip) {
      const i = { x: t.localX, y: t.localY };
      this.rm(i, i, t);
    }
  }
  Pv(t) {
    null !== this.Np && (this.Kp(), this.Np.$t().jc(null), this.hm());
  }
  lm() {
    return this.Vp;
  }
  am() {
    return this.Op;
  }
  xv() {
    this.Bp = 1, this.$i().Un();
  }
  Sv(t, i) {
    if (!this.Qd.W().handleScale.pinch) return;
    const n = 5 * (i - this.Bp);
    this.Bp = i, this.$i().Qc(t.nt, n);
  }
  pv(t) {
    this.Ap = false, this.zp = null !== this.Ip, this.Jp();
    const i = this.$i().Zc();
    null !== this.Ip && i.yt() && (this.Lp = { x: i.Yt(), y: i.Zt() }, this.Ip = { x: t.localX, y: t.localY });
  }
  hv(t) {
    if (null === this.Np) return;
    const i = t.localX, n = t.localY;
    if (null === this.Ip) this.nm(t);
    else {
      this.zp = false;
      const s = b(this.Lp), e2 = s.x + (i - this.Ip.x), r2 = s.y + (n - this.Ip.y);
      this.Gp(e2, r2, t);
    }
  }
  _v(t) {
    0 === this.$v().W().trackingMode.exitMode && (this.zp = true), this.om(), this.sm(t);
  }
  wr(t, i) {
    const n = this.Np;
    return null === n ? null : function(t2, i2, n2) {
      const s = t2.Uo(), e2 = function(t3, i3, n3) {
        var s2, e3;
        let r2, h2;
        for (const o2 of t3) {
          const t4 = null !== (e3 = null === (s2 = o2.va) || void 0 === s2 ? void 0 : s2.call(o2, i3, n3)) && void 0 !== e3 ? e3 : [];
          for (const i4 of t4) l2 = i4.zOrder, (!(a2 = null == r2 ? void 0 : r2.zOrder) || "top" === l2 && "top" !== a2 || "normal" === l2 && "bottom" === a2) && (r2 = i4, h2 = o2);
        }
        var l2, a2;
        return r2 && h2 ? { zv: r2, Hc: h2 } : null;
      }(s, i2, n2);
      if ("top" === (null == e2 ? void 0 : e2.zv.zOrder)) return xs(e2);
      for (const r2 of s) {
        if (e2 && e2.Hc === r2 && "bottom" !== e2.zv.zOrder && !e2.zv.isBackground) return xs(e2);
        const s2 = Ss(r2.Pn(t2), i2, n2);
        if (null !== s2) return { Hc: r2, Bv: s2.Bv, Iv: s2.Iv };
        if (e2 && e2.Hc === r2 && "bottom" !== e2.zv.zOrder && e2.zv.isBackground) return xs(e2);
      }
      return (null == e2 ? void 0 : e2.zv) ? xs(e2) : null;
    }(n, t, i);
  }
  _m(i, n) {
    b("left" === n ? this.Cp : this.Tp).cp(size({ width: i, height: this.Ev.height }));
  }
  um() {
    return this.Ev;
  }
  cp(t) {
    equalSizes(this.Ev, t) || (this.Ev = t, this.jv = true, this.Gv.resizeCanvasElement(t), this.Jv.resizeCanvasElement(t), this.jv = false, this.Wp.style.width = t.width + "px", this.Wp.style.height = t.height + "px");
  }
  dm() {
    const t = b(this.Np);
    t.P_(t.R_()), t.P_(t.D_());
    for (const i of t.Ba()) if (t.vr(i)) {
      const n = i.Dt();
      null !== n && t.P_(n), i.Vn();
    }
  }
  xp() {
    return this.Gv.bitmapSize;
  }
  Sp(t, i, n) {
    const s = this.xp();
    s.width > 0 && s.height > 0 && t.drawImage(this.Gv.canvasElement, i, n);
  }
  vp(t) {
    if (0 === t) return;
    if (null === this.Np) return;
    if (t > 1 && this.dm(), null !== this.Cp && this.Cp.vp(t), null !== this.Tp && this.Tp.vp(t), 1 !== t) {
      this.Gv.applySuggestedBitmapSize();
      const t2 = tryCreateCanvasRenderingTarget2D(this.Gv);
      null !== t2 && (t2.useBitmapCoordinateSpace((t3) => {
        this.mp(t3);
      }), this.Np && (this.fm(t2, Ts), this.vm(t2), this.pm(t2), this.fm(t2, Ps), this.fm(t2, Rs)));
    }
    this.Jv.applySuggestedBitmapSize();
    const i = tryCreateCanvasRenderingTarget2D(this.Jv);
    null !== i && (i.useBitmapCoordinateSpace(({ context: t2, bitmapSize: i2 }) => {
      t2.clearRect(0, 0, i2.width, i2.height);
    }), this.bm(i), this.fm(i, Ds));
  }
  wm() {
    return this.Cp;
  }
  gm() {
    return this.Tp;
  }
  bp(t, i) {
    this.fm(t, i);
  }
  Fp() {
    null !== this.Np && this.Np.W_().p(this), this.Np = null;
  }
  tm(t) {
    this.im(this.Vp, t);
  }
  im(t, i) {
    const n = i.localX, s = i.localY;
    t.M() && t.m(this.$i().St().Nu(n), { x: n, y: s }, i);
  }
  mp({ context: t, bitmapSize: i }) {
    const { width: n, height: s } = i, e2 = this.$i(), r2 = e2.q(), h2 = e2.bd();
    r2 === h2 ? G(t, 0, 0, n, s, h2) : tt(t, 0, 0, n, s, r2, h2);
  }
  vm(t) {
    const i = b(this.Np).j_().qh().gt();
    null !== i && i.X(t, false);
  }
  pm(t) {
    const i = this.$i().Yc();
    this.Mm(t, Ps, cs, i), this.Mm(t, Ps, ds, i);
  }
  bm(t) {
    this.Mm(t, Ps, ds, this.$i().Zc());
  }
  fm(t, i) {
    const n = b(this.Np).Uo();
    for (const s of n) this.Mm(t, i, cs, s);
    for (const s of n) this.Mm(t, i, ds, s);
  }
  Mm(t, i, n, s) {
    const e2 = b(this.Np), r2 = e2.$t().Wc(), h2 = null !== r2 && r2.Hc === s, l2 = null !== r2 && h2 && void 0 !== r2.Iv ? r2.Iv.Mr : void 0;
    fs(i, (i2) => n(i2, t, h2, l2), s, e2);
  }
  Zp() {
    if (null === this.Np) return;
    const t = this.Qd, i = this.Np.R_().W().visible, n = this.Np.D_().W().visible;
    i || null === this.Cp || (this.jp.removeChild(this.Cp.lp()), this.Cp.S(), this.Cp = null), n || null === this.Tp || (this.Hp.removeChild(this.Tp.lp()), this.Tp.S(), this.Tp = null);
    const s = t.$t().ud();
    i && null === this.Cp && (this.Cp = new Cs(this, t.W(), s, "left"), this.jp.appendChild(this.Cp.lp())), n && null === this.Tp && (this.Tp = new Cs(this, t.W(), s, "right"), this.Hp.appendChild(this.Tp.lp()));
  }
  xm(t) {
    return t.Dv && this.Ap || null !== this.Ip;
  }
  Sm(t) {
    return Math.max(0, Math.min(t, this.Ev.width - 1));
  }
  km(t) {
    return Math.max(0, Math.min(t, this.Ev.height - 1));
  }
  Gp(t, i, n) {
    this.$i().ld(this.Sm(t), this.km(i), n, b(this.Np));
  }
  hm() {
    this.$i().od();
  }
  om() {
    this.zp && (this.Ip = null, this.hm());
  }
  rm(t, i, n) {
    this.Ip = t, this.zp = false, this.Gp(i.x, i.y, n);
    const s = this.$i().Zc();
    this.Lp = { x: s.Yt(), y: s.Zt() };
  }
  $i() {
    return this.Qd.$t();
  }
  sm(t) {
    if (!this.Dp) return;
    const i = this.$i(), n = this.fp();
    if (i.z_(n, n.vn()), this.Rp = null, this.Dp = false, i.ed(), null !== this.Ep) {
      const t2 = performance.now(), n2 = i.St();
      this.Ep.Vr(n2.Hu(), t2), this.Ep.Qu(t2) || i.Zn(this.Ep);
    }
  }
  Kp() {
    this.Ip = null;
  }
  Jp() {
    if (!this.Np) return;
    if (this.$i().Un(), document.activeElement !== document.body && document.activeElement !== document.documentElement) b(document.activeElement).blur();
    else {
      const t = document.getSelection();
      null !== t && t.removeAllRanges();
    }
    !this.Np.vn().Ni() && this.$i().St().Ni();
  }
  nm(t) {
    if (null === this.Np) return;
    const i = this.$i(), n = i.St();
    if (n.Ni()) return;
    const s = this.Qd.W(), e2 = s.handleScroll, r2 = s.kineticScroll;
    if ((!e2.pressedMouseMove || t.Dv) && (!e2.horzTouchDrag && !e2.vertTouchDrag || !t.Dv)) return;
    const h2 = this.Np.vn(), l2 = performance.now();
    if (null !== this.Rp || this.xm(t) || (this.Rp = { x: t.clientX, y: t.clientY, Od: l2, ym: t.localX, Cm: t.localY }), null !== this.Rp && !this.Dp && (this.Rp.x !== t.clientX || this.Rp.y !== t.clientY)) {
      if (t.Dv && r2.touch || !t.Dv && r2.mouse) {
        const t2 = n.le();
        this.Ep = new as(0.2 / t2, 7 / t2, 0.997, 15 / t2), this.Ep.Yd(n.Hu(), this.Rp.Od);
      } else this.Ep = null;
      h2.Ni() || i.A_(this.Np, h2, t.localY), i.nd(t.localX), this.Dp = true;
    }
    this.Dp && (h2.Ni() || i.I_(this.Np, h2, t.localY), i.sd(t.localX), null !== this.Ep && this.Ep.Yd(n.Hu(), l2));
  }
}
class Os {
  constructor(i, n, s, e2, r2) {
    this.ft = true, this.Ev = size({ width: 0, height: 0 }), this.Hv = () => this.vp(3), this.qv = "left" === i, this.Oc = s.ud, this.cn = n, this.Tm = e2, this.Pm = r2, this.Kv = document.createElement("div"), this.Kv.style.width = "25px", this.Kv.style.height = "100%", this.Kv.style.overflow = "hidden", this.Gv = _s(this.Kv, size({ width: 16, height: 16 })), this.Gv.subscribeSuggestedBitmapSizeChanged(this.Hv);
  }
  S() {
    this.Gv.unsubscribeSuggestedBitmapSizeChanged(this.Hv), us(this.Gv.canvasElement), this.Gv.dispose();
  }
  lp() {
    return this.Kv;
  }
  um() {
    return this.Ev;
  }
  cp(t) {
    equalSizes(this.Ev, t) || (this.Ev = t, this.Gv.resizeCanvasElement(t), this.Kv.style.width = `${t.width}px`, this.Kv.style.height = `${t.height}px`, this.ft = true);
  }
  vp(t) {
    if (t < 3 && !this.ft) return;
    if (0 === this.Ev.width || 0 === this.Ev.height) return;
    this.ft = false, this.Gv.applySuggestedBitmapSize();
    const i = tryCreateCanvasRenderingTarget2D(this.Gv);
    null !== i && i.useBitmapCoordinateSpace((t2) => {
      this.mp(t2), this.Ie(t2);
    });
  }
  xp() {
    return this.Gv.bitmapSize;
  }
  Sp(t, i, n) {
    const s = this.xp();
    s.width > 0 && s.height > 0 && t.drawImage(this.Gv.canvasElement, i, n);
  }
  Ie({ context: t, bitmapSize: i, horizontalPixelRatio: n, verticalPixelRatio: s }) {
    if (!this.Tm()) return;
    t.fillStyle = this.cn.timeScale.borderColor;
    const e2 = Math.floor(this.Oc.W().C * n), r2 = Math.floor(this.Oc.W().C * s), h2 = this.qv ? i.width - e2 : 0;
    t.fillRect(h2, 0, e2, r2);
  }
  mp({ context: t, bitmapSize: i }) {
    G(t, 0, 0, i.width, i.height, this.Pm());
  }
}
function Bs(t) {
  return (i) => {
    var n, s;
    return null !== (s = null === (n = i.fa) || void 0 === n ? void 0 : n.call(i, t)) && void 0 !== s ? s : [];
  };
}
const As = Bs("normal"), Is = Bs("top"), zs = Bs("bottom");
class Ls {
  constructor(i, n) {
    this.Rm = null, this.Dm = null, this.k = null, this.Vm = false, this.Ev = size({ width: 0, height: 0 }), this.Om = new D(), this.Fv = new ni(5), this.jv = false, this.Hv = () => {
      this.jv || this.Qd.$t().Uh();
    }, this.Uv = () => {
      this.jv || this.Qd.$t().Uh();
    }, this.Qd = i, this.q_ = n, this.cn = i.W().layout, this.Xd = document.createElement("tr"), this.Bm = document.createElement("td"), this.Bm.style.padding = "0", this.Am = document.createElement("td"), this.Am.style.padding = "0", this.Kv = document.createElement("td"), this.Kv.style.height = "25px", this.Kv.style.padding = "0", this.Im = document.createElement("div"), this.Im.style.width = "100%", this.Im.style.height = "100%", this.Im.style.position = "relative", this.Im.style.overflow = "hidden", this.Kv.appendChild(this.Im), this.Gv = _s(this.Im, size({ width: 16, height: 16 })), this.Gv.subscribeSuggestedBitmapSizeChanged(this.Hv);
    const s = this.Gv.canvasElement;
    s.style.position = "absolute", s.style.zIndex = "1", s.style.left = "0", s.style.top = "0", this.Jv = _s(this.Im, size({ width: 16, height: 16 })), this.Jv.subscribeSuggestedBitmapSizeChanged(this.Uv);
    const e2 = this.Jv.canvasElement;
    e2.style.position = "absolute", e2.style.zIndex = "2", e2.style.left = "0", e2.style.top = "0", this.Xd.appendChild(this.Bm), this.Xd.appendChild(this.Kv), this.Xd.appendChild(this.Am), this.zm(), this.Qd.$t().g_().l(this.zm.bind(this), this), this.hp = new ps(this.Jv.canvasElement, this, { ev: () => true, rv: () => !this.Qd.W().handleScroll.horzTouchDrag });
  }
  S() {
    this.hp.S(), null !== this.Rm && this.Rm.S(), null !== this.Dm && this.Dm.S(), this.Jv.unsubscribeSuggestedBitmapSizeChanged(this.Uv), us(this.Jv.canvasElement), this.Jv.dispose(), this.Gv.unsubscribeSuggestedBitmapSizeChanged(this.Hv), us(this.Gv.canvasElement), this.Gv.dispose();
  }
  lp() {
    return this.Xd;
  }
  Lm() {
    return this.Rm;
  }
  Em() {
    return this.Dm;
  }
  bv(t) {
    if (this.Vm) return;
    this.Vm = true;
    const i = this.Qd.$t();
    !i.St().Ni() && this.Qd.W().handleScale.axisPressedMouseMove.time && i.Jc(t.localX);
  }
  pv(t) {
    this.bv(t);
  }
  wv() {
    const t = this.Qd.$t();
    !t.St().Ni() && this.Vm && (this.Vm = false, this.Qd.W().handleScale.axisPressedMouseMove.time && t.hd());
  }
  av(t) {
    const i = this.Qd.$t();
    !i.St().Ni() && this.Qd.W().handleScale.axisPressedMouseMove.time && i.rd(t.localX);
  }
  hv(t) {
    this.av(t);
  }
  cv() {
    this.Vm = false;
    const t = this.Qd.$t();
    t.St().Ni() && !this.Qd.W().handleScale.axisPressedMouseMove.time || t.hd();
  }
  _v() {
    this.cv();
  }
  qf() {
    this.Qd.W().handleScale.axisDoubleClickReset.time && this.Qd.$t().Kn();
  }
  jf() {
    this.qf();
  }
  Qf() {
    this.Qd.$t().W().handleScale.axisPressedMouseMove.time && this.kp(1);
  }
  Pv() {
    this.kp(0);
  }
  um() {
    return this.Ev;
  }
  Nm() {
    return this.Om;
  }
  Fm(i, s, e2) {
    equalSizes(this.Ev, i) || (this.Ev = i, this.jv = true, this.Gv.resizeCanvasElement(i), this.Jv.resizeCanvasElement(i), this.jv = false, this.Kv.style.width = `${i.width}px`, this.Kv.style.height = `${i.height}px`, this.Om.m(i)), null !== this.Rm && this.Rm.cp(size({ width: s, height: i.height })), null !== this.Dm && this.Dm.cp(size({ width: e2, height: i.height }));
  }
  Wm() {
    const t = this.jm();
    return Math.ceil(t.C + t.T + t.P + t.L + t.B + t.Hm);
  }
  bt() {
    this.Qd.$t().St().Ha();
  }
  xp() {
    return this.Gv.bitmapSize;
  }
  Sp(t, i, n) {
    const s = this.xp();
    s.width > 0 && s.height > 0 && t.drawImage(this.Gv.canvasElement, i, n);
  }
  vp(t) {
    if (0 === t) return;
    if (1 !== t) {
      this.Gv.applySuggestedBitmapSize();
      const i2 = tryCreateCanvasRenderingTarget2D(this.Gv);
      null !== i2 && (i2.useBitmapCoordinateSpace((t2) => {
        this.mp(t2), this.Ie(t2), this.$m(i2, zs);
      }), this.wp(i2), this.$m(i2, As)), null !== this.Rm && this.Rm.vp(t), null !== this.Dm && this.Dm.vp(t);
    }
    this.Jv.applySuggestedBitmapSize();
    const i = tryCreateCanvasRenderingTarget2D(this.Jv);
    null !== i && (i.useBitmapCoordinateSpace(({ context: t2, bitmapSize: i2 }) => {
      t2.clearRect(0, 0, i2.width, i2.height);
    }), this.Um([...this.Qd.$t().wt(), this.Qd.$t().Zc()], i), this.$m(i, Is));
  }
  $m(t, i) {
    const n = this.Qd.$t().wt();
    for (const s of n) fs(i, (i2) => cs(i2, t, false, void 0), s, void 0);
    for (const s of n) fs(i, (i2) => ds(i2, t, false, void 0), s, void 0);
  }
  mp({ context: t, bitmapSize: i }) {
    G(t, 0, 0, i.width, i.height, this.Qd.$t().bd());
  }
  Ie({ context: t, bitmapSize: i, verticalPixelRatio: n }) {
    if (this.Qd.W().timeScale.borderVisible) {
      t.fillStyle = this.qm();
      const s = Math.max(1, Math.floor(this.jm().C * n));
      t.fillRect(0, 0, i.width, s);
    }
  }
  wp(t) {
    const i = this.Qd.$t().St(), n = i.Ha();
    if (!n || 0 === n.length) return;
    const s = this.q_.maxTickMarkWeight(n), e2 = this.jm(), r2 = i.W();
    r2.borderVisible && r2.ticksVisible && t.useBitmapCoordinateSpace(({ context: t2, horizontalPixelRatio: i2, verticalPixelRatio: s2 }) => {
      t2.strokeStyle = this.qm(), t2.fillStyle = this.qm();
      const r3 = Math.max(1, Math.floor(i2)), h2 = Math.floor(0.5 * i2);
      t2.beginPath();
      const l2 = Math.round(e2.T * s2);
      for (let s3 = n.length; s3--; ) {
        const e3 = Math.round(n[s3].coord * i2);
        t2.rect(e3 - h2, 0, r3, l2);
      }
      t2.fill();
    }), t.useMediaCoordinateSpace(({ context: t2 }) => {
      const i2 = e2.C + e2.T + e2.L + e2.P / 2;
      t2.textAlign = "center", t2.textBaseline = "middle", t2.fillStyle = this.$(), t2.font = this._p();
      for (const e3 of n) if (e3.weight < s) {
        const n2 = e3.needAlignCoordinate ? this.Ym(t2, e3.coord, e3.label) : e3.coord;
        t2.fillText(e3.label, n2, i2);
      }
      this.Qd.W().timeScale.allowBoldLabels && (t2.font = this.Zm());
      for (const e3 of n) if (e3.weight >= s) {
        const n2 = e3.needAlignCoordinate ? this.Ym(t2, e3.coord, e3.label) : e3.coord;
        t2.fillText(e3.label, n2, i2);
      }
    });
  }
  Ym(t, i, n) {
    const s = this.Fv.xi(t, n), e2 = s / 2, r2 = Math.floor(i - e2) + 0.5;
    return r2 < 0 ? i += Math.abs(0 - r2) : r2 + s > this.Ev.width && (i -= Math.abs(this.Ev.width - (r2 + s))), i;
  }
  Um(t, i) {
    const n = this.jm();
    for (const s of t) for (const t2 of s.Qi()) t2.gt().X(i, n);
  }
  qm() {
    return this.Qd.W().timeScale.borderColor;
  }
  $() {
    return this.cn.textColor;
  }
  j() {
    return this.cn.fontSize;
  }
  _p() {
    return F(this.j(), this.cn.fontFamily);
  }
  Zm() {
    return F(this.j(), this.cn.fontFamily, "bold");
  }
  jm() {
    null === this.k && (this.k = { C: 1, N: NaN, L: NaN, B: NaN, ji: NaN, T: 5, P: NaN, R: "", Wi: new ni(), Hm: 0 });
    const t = this.k, i = this._p();
    if (t.R !== i) {
      const n = this.j();
      t.P = n, t.R = i, t.L = 3 * n / 12, t.B = 3 * n / 12, t.ji = 9 * n / 12, t.N = 0, t.Hm = 4 * n / 12, t.Wi.nr();
    }
    return this.k;
  }
  kp(t) {
    this.Kv.style.cursor = 1 === t ? "ew-resize" : "default";
  }
  zm() {
    const t = this.Qd.$t(), i = t.W();
    i.leftPriceScale.visible || null === this.Rm || (this.Bm.removeChild(this.Rm.lp()), this.Rm.S(), this.Rm = null), i.rightPriceScale.visible || null === this.Dm || (this.Am.removeChild(this.Dm.lp()), this.Dm.S(), this.Dm = null);
    const n = { ud: this.Qd.$t().ud() }, s = () => i.leftPriceScale.borderVisible && t.St().W().borderVisible, e2 = () => t.bd();
    i.leftPriceScale.visible && null === this.Rm && (this.Rm = new Os("left", i, n, s, e2), this.Bm.appendChild(this.Rm.lp())), i.rightPriceScale.visible && null === this.Dm && (this.Dm = new Os("right", i, n, s, e2), this.Am.appendChild(this.Dm.lp()));
  }
}
const Es = !!ns && !!navigator.userAgentData && navigator.userAgentData.brands.some((t) => t.brand.includes("Chromium")) && !!ns && ((null === (Ns = null === navigator || void 0 === navigator ? void 0 : navigator.userAgentData) || void 0 === Ns ? void 0 : Ns.platform) ? "Windows" === navigator.userAgentData.platform : navigator.userAgent.toLowerCase().indexOf("win") >= 0);
var Ns;
class Fs {
  constructor(t, i, n) {
    var s;
    this.Xm = [], this.Km = 0, this.ho = 0, this.__ = 0, this.Gm = 0, this.Jm = 0, this.Qm = null, this.tb = false, this.Vp = new D(), this.Op = new D(), this.Rc = new D(), this.ib = null, this.nb = null, this.Jd = t, this.cn = i, this.q_ = n, this.Xd = document.createElement("div"), this.Xd.classList.add("tv-lightweight-charts"), this.Xd.style.overflow = "hidden", this.Xd.style.direction = "ltr", this.Xd.style.width = "100%", this.Xd.style.height = "100%", (s = this.Xd).style.userSelect = "none", s.style.webkitUserSelect = "none", s.style.msUserSelect = "none", s.style.MozUserSelect = "none", s.style.webkitTapHighlightColor = "transparent", this.sb = document.createElement("table"), this.sb.setAttribute("cellspacing", "0"), this.Xd.appendChild(this.sb), this.eb = this.rb.bind(this), Ws(this.cn) && this.hb(true), this.$i = new Ln(this.Vc.bind(this), this.cn, n), this.$t().Xc().l(this.lb.bind(this), this), this.ab = new Ls(this, this.q_), this.sb.appendChild(this.ab.lp());
    const e2 = i.autoSize && this.ob();
    let r2 = this.cn.width, h2 = this.cn.height;
    if (e2 || 0 === r2 || 0 === h2) {
      const i2 = t.getBoundingClientRect();
      r2 = r2 || i2.width, h2 = h2 || i2.height;
    }
    this._b(r2, h2), this.ub(), t.appendChild(this.Xd), this.cb(), this.$i.St().ec().l(this.$i.Kl.bind(this.$i), this), this.$i.g_().l(this.$i.Kl.bind(this.$i), this);
  }
  $t() {
    return this.$i;
  }
  W() {
    return this.cn;
  }
  Yp() {
    return this.Xm;
  }
  fb() {
    return this.ab;
  }
  S() {
    this.hb(false), 0 !== this.Km && window.cancelAnimationFrame(this.Km), this.$i.Xc().p(this), this.$i.St().ec().p(this), this.$i.g_().p(this), this.$i.S();
    for (const t of this.Xm) this.sb.removeChild(t.lp()), t.lm().p(this), t.am().p(this), t.S();
    this.Xm = [], b(this.ab).S(), null !== this.Xd.parentElement && this.Xd.parentElement.removeChild(this.Xd), this.Rc.S(), this.Vp.S(), this.Op.S(), this.pb();
  }
  _b(i, n, s = false) {
    if (this.ho === n && this.__ === i) return;
    const e2 = function(i2) {
      const n2 = Math.floor(i2.width), s2 = Math.floor(i2.height);
      return size({ width: n2 - n2 % 2, height: s2 - s2 % 2 });
    }(size({ width: i, height: n }));
    this.ho = e2.height, this.__ = e2.width;
    const r2 = this.ho + "px", h2 = this.__ + "px";
    b(this.Xd).style.height = r2, b(this.Xd).style.width = h2, this.sb.style.height = r2, this.sb.style.width = h2, s ? this.mb(ut.es(), performance.now()) : this.$i.Kl();
  }
  vp(t) {
    void 0 === t && (t = ut.es());
    for (let i = 0; i < this.Xm.length; i++) this.Xm[i].vp(t.Hn(i).Fn);
    this.cn.timeScale.visible && this.ab.vp(t.jn());
  }
  $h(t) {
    const i = Ws(this.cn);
    this.$i.$h(t);
    const n = Ws(this.cn);
    n !== i && this.hb(n), this.cb(), this.bb(t);
  }
  lm() {
    return this.Vp;
  }
  am() {
    return this.Op;
  }
  Xc() {
    return this.Rc;
  }
  wb() {
    null !== this.Qm && (this.mb(this.Qm, performance.now()), this.Qm = null);
    const t = this.gb(null), i = document.createElement("canvas");
    i.width = t.width, i.height = t.height;
    const n = b(i.getContext("2d"));
    return this.gb(n), i;
  }
  Mb(t) {
    if ("left" === t && !this.xb()) return 0;
    if ("right" === t && !this.Sb()) return 0;
    if (0 === this.Xm.length) return 0;
    return b("left" === t ? this.Xm[0].wm() : this.Xm[0].gm()).dp();
  }
  kb() {
    return this.cn.autoSize && null !== this.ib;
  }
  yb() {
    return this.Xd;
  }
  Qp(t) {
    this.nb = t, this.nb ? this.yb().style.setProperty("cursor", t) : this.yb().style.removeProperty("cursor");
  }
  Cb() {
    return this.nb;
  }
  Tb() {
    return m(this.Xm[0]).um();
  }
  bb(t) {
    (void 0 !== t.autoSize || !this.ib || void 0 === t.width && void 0 === t.height) && (t.autoSize && !this.ib && this.ob(), false === t.autoSize && null !== this.ib && this.pb(), t.autoSize || void 0 === t.width && void 0 === t.height || this._b(t.width || this.__, t.height || this.ho));
  }
  gb(i) {
    let n = 0, s = 0;
    const e2 = this.Xm[0], r2 = (t, n2) => {
      let s2 = 0;
      for (let e3 = 0; e3 < this.Xm.length; e3++) {
        const r3 = this.Xm[e3], h3 = b("left" === t ? r3.wm() : r3.gm()), l2 = h3.xp();
        null !== i && h3.Sp(i, n2, s2), s2 += l2.height;
      }
    };
    if (this.xb()) {
      r2("left", 0);
      n += b(e2.wm()).xp().width;
    }
    for (let t = 0; t < this.Xm.length; t++) {
      const e3 = this.Xm[t], r3 = e3.xp();
      null !== i && e3.Sp(i, n, s), s += r3.height;
    }
    if (n += e2.xp().width, this.Sb()) {
      r2("right", n);
      n += b(e2.gm()).xp().width;
    }
    const h2 = (t, n2, s2) => {
      b("left" === t ? this.ab.Lm() : this.ab.Em()).Sp(b(i), n2, s2);
    };
    if (this.cn.timeScale.visible) {
      const t = this.ab.xp();
      if (null !== i) {
        let n2 = 0;
        this.xb() && (h2("left", n2, s), n2 = b(e2.wm()).xp().width), this.ab.Sp(i, n2, s), n2 += t.width, this.Sb() && h2("right", n2, s);
      }
      s += t.height;
    }
    return size({ width: n, height: s });
  }
  Pb() {
    let i = 0, n = 0, s = 0;
    for (const t of this.Xm) this.xb() && (n = Math.max(n, b(t.wm()).op(), this.cn.leftPriceScale.minimumWidth)), this.Sb() && (s = Math.max(s, b(t.gm()).op(), this.cn.rightPriceScale.minimumWidth)), i += t.M_();
    n = rs(n), s = rs(s);
    const e2 = this.__, r2 = this.ho, h2 = Math.max(e2 - n - s, 0), l2 = this.cn.timeScale.visible;
    let a2 = l2 ? Math.max(this.ab.Wm(), this.cn.timeScale.minimumHeight) : 0;
    var o2;
    a2 = (o2 = a2) + o2 % 2;
    const _2 = 0 + a2, u2 = r2 < _2 ? 0 : r2 - _2, c2 = u2 / i;
    let d2 = 0;
    for (let i2 = 0; i2 < this.Xm.length; ++i2) {
      const e3 = this.Xm[i2];
      e3.qp(this.$i.qc()[i2]);
      let r3 = 0, l3 = 0;
      l3 = i2 === this.Xm.length - 1 ? u2 - d2 : Math.round(e3.M_() * c2), r3 = Math.max(l3, 2), d2 += r3, e3.cp(size({ width: h2, height: r3 })), this.xb() && e3._m(n, "left"), this.Sb() && e3._m(s, "right"), e3.fp() && this.$i.Kc(e3.fp(), r3);
    }
    this.ab.Fm(size({ width: l2 ? h2 : 0, height: a2 }), l2 ? n : 0, l2 ? s : 0), this.$i.S_(h2), this.Gm !== n && (this.Gm = n), this.Jm !== s && (this.Jm = s);
  }
  hb(t) {
    t ? this.Xd.addEventListener("wheel", this.eb, { passive: false }) : this.Xd.removeEventListener("wheel", this.eb);
  }
  Rb(t) {
    switch (t.deltaMode) {
      case t.DOM_DELTA_PAGE:
        return 120;
      case t.DOM_DELTA_LINE:
        return 32;
    }
    return Es ? 1 / window.devicePixelRatio : 1;
  }
  rb(t) {
    if (!(0 !== t.deltaX && this.cn.handleScroll.mouseWheel || 0 !== t.deltaY && this.cn.handleScale.mouseWheel)) return;
    const i = this.Rb(t), n = i * t.deltaX / 100, s = -i * t.deltaY / 100;
    if (t.cancelable && t.preventDefault(), 0 !== s && this.cn.handleScale.mouseWheel) {
      const i2 = Math.sign(s) * Math.min(1, Math.abs(s)), n2 = t.clientX - this.Xd.getBoundingClientRect().left;
      this.$t().Qc(n2, i2);
    }
    0 !== n && this.cn.handleScroll.mouseWheel && this.$t().td(-80 * n);
  }
  mb(t, i) {
    var n;
    const s = t.jn();
    3 === s && this.Db(), 3 !== s && 2 !== s || (this.Vb(t), this.Ob(t, i), this.ab.bt(), this.Xm.forEach((t2) => {
      t2.Xp();
    }), 3 === (null === (n = this.Qm) || void 0 === n ? void 0 : n.jn()) && (this.Qm.ts(t), this.Db(), this.Vb(this.Qm), this.Ob(this.Qm, i), t = this.Qm, this.Qm = null)), this.vp(t);
  }
  Ob(t, i) {
    for (const n of t.Qn()) this.ns(n, i);
  }
  Vb(t) {
    const i = this.$i.qc();
    for (let n = 0; n < i.length; n++) t.Hn(n).Wn && i[n].N_();
  }
  ns(t, i) {
    const n = this.$i.St();
    switch (t.qn) {
      case 0:
        n.hc();
        break;
      case 1:
        n.lc(t.Vt);
        break;
      case 2:
        n.Gn(t.Vt);
        break;
      case 3:
        n.Jn(t.Vt);
        break;
      case 4:
        n.qu();
        break;
      case 5:
        t.Vt.Qu(i) || n.Jn(t.Vt.tc(i));
    }
  }
  Vc(t) {
    null !== this.Qm ? this.Qm.ts(t) : this.Qm = t, this.tb || (this.tb = true, this.Km = window.requestAnimationFrame((t2) => {
      if (this.tb = false, this.Km = 0, null !== this.Qm) {
        const i = this.Qm;
        this.Qm = null, this.mb(i, t2);
        for (const n of i.Qn()) if (5 === n.qn && !n.Vt.Qu(t2)) {
          this.$t().Zn(n.Vt);
          break;
        }
      }
    }));
  }
  Db() {
    this.ub();
  }
  ub() {
    const t = this.$i.qc(), i = t.length, n = this.Xm.length;
    for (let t2 = i; t2 < n; t2++) {
      const t3 = m(this.Xm.pop());
      this.sb.removeChild(t3.lp()), t3.lm().p(this), t3.am().p(this), t3.S();
    }
    for (let s = n; s < i; s++) {
      const i2 = new Vs(this, t[s]);
      i2.lm().l(this.Bb.bind(this), this), i2.am().l(this.Ab.bind(this), this), this.Xm.push(i2), this.sb.insertBefore(i2.lp(), this.ab.lp());
    }
    for (let n2 = 0; n2 < i; n2++) {
      const i2 = t[n2], s = this.Xm[n2];
      s.fp() !== i2 ? s.qp(i2) : s.Up();
    }
    this.cb(), this.Pb();
  }
  Ib(t, i, n) {
    var s;
    const e2 = /* @__PURE__ */ new Map();
    if (null !== t) {
      this.$i.wt().forEach((i2) => {
        const n2 = i2.In().ll(t);
        null !== n2 && e2.set(i2, n2);
      });
    }
    let r2;
    if (null !== t) {
      const i2 = null === (s = this.$i.St().Ui(t)) || void 0 === s ? void 0 : s.originalTime;
      void 0 !== i2 && (r2 = i2);
    }
    const h2 = this.$t().Wc(), l2 = null !== h2 && h2.Hc instanceof Gi ? h2.Hc : void 0, a2 = null !== h2 && void 0 !== h2.Iv ? h2.Iv.gr : void 0;
    return { zb: r2, ee: null != t ? t : void 0, Lb: null != i ? i : void 0, Eb: l2, Nb: e2, Fb: a2, Wb: null != n ? n : void 0 };
  }
  Bb(t, i, n) {
    this.Vp.m(() => this.Ib(t, i, n));
  }
  Ab(t, i, n) {
    this.Op.m(() => this.Ib(t, i, n));
  }
  lb(t, i, n) {
    this.Rc.m(() => this.Ib(t, i, n));
  }
  cb() {
    const t = this.cn.timeScale.visible ? "" : "none";
    this.ab.lp().style.display = t;
  }
  xb() {
    return this.Xm[0].fp().R_().W().visible;
  }
  Sb() {
    return this.Xm[0].fp().D_().W().visible;
  }
  ob() {
    return "ResizeObserver" in window && (this.ib = new ResizeObserver((t) => {
      const i = t.find((t2) => t2.target === this.Jd);
      i && this._b(i.contentRect.width, i.contentRect.height);
    }), this.ib.observe(this.Jd, { box: "border-box" }), true);
  }
  pb() {
    null !== this.ib && this.ib.disconnect(), this.ib = null;
  }
}
function Ws(t) {
  return Boolean(t.handleScroll.mouseWheel || t.handleScale.mouseWheel);
}
function js(t) {
  return function(t2) {
    return void 0 !== t2.open;
  }(t) || function(t2) {
    return void 0 !== t2.value;
  }(t);
}
function Hs(t, i) {
  var n = {};
  for (var s in t) Object.prototype.hasOwnProperty.call(t, s) && i.indexOf(s) < 0 && (n[s] = t[s]);
  if (null != t && "function" == typeof Object.getOwnPropertySymbols) {
    var e2 = 0;
    for (s = Object.getOwnPropertySymbols(t); e2 < s.length; e2++) i.indexOf(s[e2]) < 0 && Object.prototype.propertyIsEnumerable.call(t, s[e2]) && (n[s[e2]] = t[s[e2]]);
  }
  return n;
}
function $s(t, i, n, s) {
  const e2 = n.value, r2 = { ee: i, ot: t, Vt: [e2, e2, e2, e2], zb: s };
  return void 0 !== n.color && (r2.V = n.color), r2;
}
function Us(t, i, n, s) {
  const e2 = n.value, r2 = { ee: i, ot: t, Vt: [e2, e2, e2, e2], zb: s };
  return void 0 !== n.lineColor && (r2.lt = n.lineColor), void 0 !== n.topColor && (r2.Ps = n.topColor), void 0 !== n.bottomColor && (r2.Rs = n.bottomColor), r2;
}
function qs(t, i, n, s) {
  const e2 = n.value, r2 = { ee: i, ot: t, Vt: [e2, e2, e2, e2], zb: s };
  return void 0 !== n.topLineColor && (r2.Re = n.topLineColor), void 0 !== n.bottomLineColor && (r2.De = n.bottomLineColor), void 0 !== n.topFillColor1 && (r2.ke = n.topFillColor1), void 0 !== n.topFillColor2 && (r2.ye = n.topFillColor2), void 0 !== n.bottomFillColor1 && (r2.Ce = n.bottomFillColor1), void 0 !== n.bottomFillColor2 && (r2.Te = n.bottomFillColor2), r2;
}
function Ys(t, i, n, s) {
  const e2 = { ee: i, ot: t, Vt: [n.open, n.high, n.low, n.close], zb: s };
  return void 0 !== n.color && (e2.V = n.color), e2;
}
function Zs(t, i, n, s) {
  const e2 = { ee: i, ot: t, Vt: [n.open, n.high, n.low, n.close], zb: s };
  return void 0 !== n.color && (e2.V = n.color), void 0 !== n.borderColor && (e2.Ot = n.borderColor), void 0 !== n.wickColor && (e2.Xh = n.wickColor), e2;
}
function Xs(t, i, n, s, e2) {
  const r2 = m(e2)(n), h2 = Math.max(...r2), l2 = Math.min(...r2), a2 = r2[r2.length - 1], o2 = [a2, h2, l2, a2], _2 = n, { time: u2, color: c2 } = _2;
  return { ee: i, ot: t, Vt: o2, zb: s, $e: Hs(_2, ["time", "color"]), V: c2 };
}
function Ks(t) {
  return void 0 !== t.Vt;
}
function Gs(t, i) {
  return void 0 !== i.customValues && (t.jb = i.customValues), t;
}
function Js(t) {
  return (i, n, s, e2, r2, h2) => function(t2, i2) {
    return i2 ? i2(t2) : void 0 === (n2 = t2).open && void 0 === n2.value;
    var n2;
  }(s, h2) ? Gs({ ot: i, ee: n, zb: e2 }, s) : Gs(t(i, n, s, e2, r2), s);
}
function Qs(t) {
  return { Candlestick: Js(Zs), Bar: Js(Ys), Area: Js(Us), Baseline: Js(qs), Histogram: Js($s), Line: Js($s), Custom: Js(Xs) }[t];
}
function te(t) {
  return { ee: 0, Hb: /* @__PURE__ */ new Map(), la: t };
}
function ie(t, i) {
  if (void 0 !== t && 0 !== t.length) return { $b: i.key(t[0].ot), Ub: i.key(t[t.length - 1].ot) };
}
function ne(t) {
  let i;
  return t.forEach((t2) => {
    void 0 === i && (i = t2.zb);
  }), m(i);
}
class se {
  constructor(t) {
    this.qb = /* @__PURE__ */ new Map(), this.Yb = /* @__PURE__ */ new Map(), this.Zb = /* @__PURE__ */ new Map(), this.Xb = [], this.q_ = t;
  }
  S() {
    this.qb.clear(), this.Yb.clear(), this.Zb.clear(), this.Xb = [];
  }
  Kb(t, i) {
    let n = 0 !== this.qb.size, s = false;
    const e2 = this.Yb.get(t);
    if (void 0 !== e2) if (1 === this.Yb.size) n = false, s = true, this.qb.clear();
    else for (const i2 of this.Xb) i2.pointData.Hb.delete(t) && (s = true);
    let r2 = [];
    if (0 !== i.length) {
      const n2 = i.map((t2) => t2.time), e3 = this.q_.createConverterToInternalObj(i), h3 = Qs(t.Qh()), l2 = t.Ca(), a2 = t.Ta();
      r2 = i.map((i2, r3) => {
        const o2 = e3(i2.time), _2 = this.q_.key(o2);
        let u2 = this.qb.get(_2);
        void 0 === u2 && (u2 = te(o2), this.qb.set(_2, u2), s = true);
        const c2 = h3(o2, u2.ee, i2, n2[r3], l2, a2);
        return u2.Hb.set(t, c2), c2;
      });
    }
    n && this.Gb(), this.Jb(t, r2);
    let h2 = -1;
    if (s) {
      const t2 = [];
      this.qb.forEach((i2) => {
        t2.push({ timeWeight: 0, time: i2.la, pointData: i2, originalTime: ne(i2.Hb) });
      }), t2.sort((t3, i2) => this.q_.key(t3.time) - this.q_.key(i2.time)), h2 = this.Qb(t2);
    }
    return this.tw(t, h2, function(t2, i2, n2) {
      const s2 = ie(t2, n2), e3 = ie(i2, n2);
      if (void 0 !== s2 && void 0 !== e3) return { ta: s2.Ub >= e3.Ub && s2.$b >= e3.$b };
    }(this.Yb.get(t), e2, this.q_));
  }
  vd(t) {
    return this.Kb(t, []);
  }
  iw(t, i) {
    const n = i;
    !function(t2) {
      void 0 === t2.zb && (t2.zb = t2.time);
    }(n), this.q_.preprocessData(i);
    const s = this.q_.createConverterToInternalObj([i])(i.time), e2 = this.Zb.get(t);
    if (void 0 !== e2 && this.q_.key(s) < this.q_.key(e2)) throw new Error(`Cannot update oldest data, last time=${e2}, new time=${s}`);
    let r2 = this.qb.get(this.q_.key(s));
    const h2 = void 0 === r2;
    void 0 === r2 && (r2 = te(s), this.qb.set(this.q_.key(s), r2));
    const l2 = Qs(t.Qh()), a2 = t.Ca(), o2 = t.Ta(), _2 = l2(s, r2.ee, i, n.zb, a2, o2);
    r2.Hb.set(t, _2), this.nw(t, _2);
    const u2 = { ta: Ks(_2) };
    if (!h2) return this.tw(t, -1, u2);
    const c2 = { timeWeight: 0, time: r2.la, pointData: r2, originalTime: ne(r2.Hb) }, d2 = Bt(this.Xb, this.q_.key(c2.time), (t2, i2) => this.q_.key(t2.time) < i2);
    this.Xb.splice(d2, 0, c2);
    for (let t2 = d2; t2 < this.Xb.length; ++t2) ee(this.Xb[t2].pointData, t2);
    return this.q_.fillWeightsForPoints(this.Xb, d2), this.tw(t, d2, u2);
  }
  nw(t, i) {
    let n = this.Yb.get(t);
    void 0 === n && (n = [], this.Yb.set(t, n));
    const s = 0 !== n.length ? n[n.length - 1] : null;
    null === s || this.q_.key(i.ot) > this.q_.key(s.ot) ? Ks(i) && n.push(i) : Ks(i) ? n[n.length - 1] = i : n.splice(-1, 1), this.Zb.set(t, i.ot);
  }
  Jb(t, i) {
    0 !== i.length ? (this.Yb.set(t, i.filter(Ks)), this.Zb.set(t, i[i.length - 1].ot)) : (this.Yb.delete(t), this.Zb.delete(t));
  }
  Gb() {
    for (const t of this.Xb) 0 === t.pointData.Hb.size && this.qb.delete(this.q_.key(t.time));
  }
  Qb(t) {
    let i = -1;
    for (let n = 0; n < this.Xb.length && n < t.length; ++n) {
      const s = this.Xb[n], e2 = t[n];
      if (this.q_.key(s.time) !== this.q_.key(e2.time)) {
        i = n;
        break;
      }
      e2.timeWeight = s.timeWeight, ee(e2.pointData, n);
    }
    if (-1 === i && this.Xb.length !== t.length && (i = Math.min(this.Xb.length, t.length)), -1 === i) return -1;
    for (let n = i; n < t.length; ++n) ee(t[n].pointData, n);
    return this.q_.fillWeightsForPoints(t, i), this.Xb = t, i;
  }
  sw() {
    if (0 === this.Yb.size) return null;
    let t = 0;
    return this.Yb.forEach((i) => {
      0 !== i.length && (t = Math.max(t, i[i.length - 1].ee));
    }), t;
  }
  tw(t, i, n) {
    const s = { ew: /* @__PURE__ */ new Map(), St: { Eu: this.sw() } };
    if (-1 !== i) this.Yb.forEach((i2, e2) => {
      s.ew.set(e2, { $e: i2, rw: e2 === t ? n : void 0 });
    }), this.Yb.has(t) || s.ew.set(t, { $e: [], rw: n }), s.St.hw = this.Xb, s.St.lw = i;
    else {
      const i2 = this.Yb.get(t);
      s.ew.set(t, { $e: i2 || [], rw: n });
    }
    return s;
  }
}
function ee(t, i) {
  t.ee = i, t.Hb.forEach((t2) => {
    t2.ee = i;
  });
}
function re(t) {
  const i = { value: t.Vt[3], time: t.zb };
  return void 0 !== t.jb && (i.customValues = t.jb), i;
}
function he(t) {
  const i = re(t);
  return void 0 !== t.V && (i.color = t.V), i;
}
function le(t) {
  const i = re(t);
  return void 0 !== t.lt && (i.lineColor = t.lt), void 0 !== t.Ps && (i.topColor = t.Ps), void 0 !== t.Rs && (i.bottomColor = t.Rs), i;
}
function ae(t) {
  const i = re(t);
  return void 0 !== t.Re && (i.topLineColor = t.Re), void 0 !== t.De && (i.bottomLineColor = t.De), void 0 !== t.ke && (i.topFillColor1 = t.ke), void 0 !== t.ye && (i.topFillColor2 = t.ye), void 0 !== t.Ce && (i.bottomFillColor1 = t.Ce), void 0 !== t.Te && (i.bottomFillColor2 = t.Te), i;
}
function oe(t) {
  const i = { open: t.Vt[0], high: t.Vt[1], low: t.Vt[2], close: t.Vt[3], time: t.zb };
  return void 0 !== t.jb && (i.customValues = t.jb), i;
}
function _e(t) {
  const i = oe(t);
  return void 0 !== t.V && (i.color = t.V), i;
}
function ue(t) {
  const i = oe(t), { V: n, Ot: s, Xh: e2 } = t;
  return void 0 !== n && (i.color = n), void 0 !== s && (i.borderColor = s), void 0 !== e2 && (i.wickColor = e2), i;
}
function ce(t) {
  return { Area: le, Line: he, Baseline: ae, Histogram: he, Bar: _e, Candlestick: ue, Custom: de }[t];
}
function de(t) {
  const i = t.zb;
  return Object.assign(Object.assign({}, t.$e), { time: i });
}
const fe = { vertLine: { color: "#9598A1", width: 1, style: 3, visible: true, labelVisible: true, labelBackgroundColor: "#131722" }, horzLine: { color: "#9598A1", width: 1, style: 3, visible: true, labelVisible: true, labelBackgroundColor: "#131722" }, mode: 1 }, ve = { vertLines: { color: "#D6DCDE", style: 0, visible: true }, horzLines: { color: "#D6DCDE", style: 0, visible: true } }, pe = { background: { type: "solid", color: "#FFFFFF" }, textColor: "#191919", fontSize: 12, fontFamily: N, attributionLogo: true }, me = { autoScale: true, mode: 0, invertScale: false, alignLabels: true, borderVisible: true, borderColor: "#2B2B43", entireTextOnly: false, visible: false, ticksVisible: false, scaleMargins: { bottom: 0.1, top: 0.2 }, minimumWidth: 0 }, be = { rightOffset: 0, barSpacing: 6, minBarSpacing: 0.5, fixLeftEdge: false, fixRightEdge: false, lockVisibleTimeRangeOnResize: false, rightBarStaysOnScroll: false, borderVisible: true, borderColor: "#2B2B43", visible: true, timeVisible: false, secondsVisible: true, shiftVisibleRangeOnNewBar: true, allowShiftVisibleRangeOnWhitespaceReplacement: false, ticksVisible: false, uniformDistribution: false, minimumHeight: 0, allowBoldLabels: true }, we = { color: "rgba(0, 0, 0, 0)", visible: false, fontSize: 48, fontFamily: N, fontStyle: "", text: "", horzAlign: "center", vertAlign: "center" };
function ge() {
  return { width: 0, height: 0, autoSize: false, layout: pe, crosshair: fe, grid: ve, overlayPriceScales: Object.assign({}, me), leftPriceScale: Object.assign(Object.assign({}, me), { visible: false }), rightPriceScale: Object.assign(Object.assign({}, me), { visible: true }), timeScale: be, watermark: we, localization: { locale: ns ? navigator.language : "", dateFormat: "dd MMM 'yy" }, handleScroll: { mouseWheel: true, pressedMouseMove: true, horzTouchDrag: true, vertTouchDrag: true }, handleScale: { axisPressedMouseMove: { time: true, price: true }, axisDoubleClickReset: { time: true, price: true }, mouseWheel: true, pinch: true }, kineticScroll: { mouse: false, touch: true }, trackingMode: { exitMode: 1 } };
}
class Me {
  constructor(t, i) {
    this.aw = t, this.ow = i;
  }
  applyOptions(t) {
    this.aw.$t().$c(this.ow, t);
  }
  options() {
    return this.Li().W();
  }
  width() {
    return _t(this.ow) ? this.aw.Mb(this.ow) : 0;
  }
  Li() {
    return b(this.aw.$t().Uc(this.ow)).Dt;
  }
}
function xe(t, i, n) {
  const s = Hs(t, ["time", "originalTime"]), e2 = Object.assign({ time: i }, s);
  return void 0 !== n && (e2.originalTime = n), e2;
}
const Se = { color: "#FF0000", price: 0, lineStyle: 2, lineWidth: 1, lineVisible: true, axisLabelVisible: true, title: "", axisLabelColor: "", axisLabelTextColor: "" };
class ke {
  constructor(t) {
    this.Nh = t;
  }
  applyOptions(t) {
    this.Nh.$h(t);
  }
  options() {
    return this.Nh.W();
  }
  _w() {
    return this.Nh;
  }
}
class ye {
  constructor(t, i, n, s, e2) {
    this.uw = new D(), this.Es = t, this.cw = i, this.dw = n, this.q_ = e2, this.fw = s;
  }
  S() {
    this.uw.S();
  }
  priceFormatter() {
    return this.Es.ba();
  }
  priceToCoordinate(t) {
    const i = this.Es.Ct();
    return null === i ? null : this.Es.Dt().Rt(t, i.Vt);
  }
  coordinateToPrice(t) {
    const i = this.Es.Ct();
    return null === i ? null : this.Es.Dt().pn(t, i.Vt);
  }
  barsInLogicalRange(t) {
    if (null === t) return null;
    const i = new yn(new xn(t.from, t.to)).lu(), n = this.Es.In();
    if (n.Ni()) return null;
    const s = n.ll(i.Os(), 1), e2 = n.ll(i.ui(), -1), r2 = b(n.el()), h2 = b(n.An());
    if (null !== s && null !== e2 && s.ee > e2.ee) return { barsBefore: t.from - r2, barsAfter: h2 - t.to };
    const l2 = { barsBefore: null === s || s.ee === r2 ? t.from - r2 : s.ee - r2, barsAfter: null === e2 || e2.ee === h2 ? h2 - t.to : h2 - e2.ee };
    return null !== s && null !== e2 && (l2.from = s.zb, l2.to = e2.zb), l2;
  }
  setData(t) {
    this.q_, this.Es.Qh(), this.cw.pw(this.Es, t), this.mw("full");
  }
  update(t) {
    this.Es.Qh(), this.cw.bw(this.Es, t), this.mw("update");
  }
  dataByIndex(t, i) {
    const n = this.Es.In().ll(t, i);
    if (null === n) return null;
    return ce(this.seriesType())(n);
  }
  data() {
    const t = ce(this.seriesType());
    return this.Es.In().ne().map((i) => t(i));
  }
  subscribeDataChanged(t) {
    this.uw.l(t);
  }
  unsubscribeDataChanged(t) {
    this.uw.v(t);
  }
  setMarkers(t) {
    this.q_;
    const i = t.map((t2) => xe(t2, this.q_.convertHorzItemToInternal(t2.time), t2.time));
    this.Es.na(i);
  }
  markers() {
    return this.Es.sa().map((t) => xe(t, t.originalTime, void 0));
  }
  applyOptions(t) {
    this.Es.$h(t);
  }
  options() {
    return z(this.Es.W());
  }
  priceScale() {
    return this.dw.priceScale(this.Es.Dt().Pa());
  }
  createPriceLine(t) {
    const i = V(z(Se), t), n = this.Es.ea(i);
    return new ke(n);
  }
  removePriceLine(t) {
    this.Es.ra(t._w());
  }
  seriesType() {
    return this.Es.Qh();
  }
  attachPrimitive(t) {
    this.Es.ka(t), t.attached && t.attached({ chart: this.fw, series: this, requestUpdate: () => this.Es.$t().Kl() });
  }
  detachPrimitive(t) {
    this.Es.ya(t), t.detached && t.detached();
  }
  mw(t) {
    this.uw.M() && this.uw.m(t);
  }
}
class Ce {
  constructor(t, i, n) {
    this.ww = new D(), this.mu = new D(), this.Om = new D(), this.$i = t, this.yl = t.St(), this.ab = i, this.yl.nc().l(this.gw.bind(this)), this.yl.sc().l(this.Mw.bind(this)), this.ab.Nm().l(this.xw.bind(this)), this.q_ = n;
  }
  S() {
    this.yl.nc().p(this), this.yl.sc().p(this), this.ab.Nm().p(this), this.ww.S(), this.mu.S(), this.Om.S();
  }
  scrollPosition() {
    return this.yl.Hu();
  }
  scrollToPosition(t, i) {
    i ? this.yl.Ju(t, 1e3) : this.$i.Jn(t);
  }
  scrollToRealTime() {
    this.yl.Gu();
  }
  getVisibleRange() {
    const t = this.yl.Vu();
    return null === t ? null : { from: t.from.originalTime, to: t.to.originalTime };
  }
  setVisibleRange(t) {
    const i = { from: this.q_.convertHorzItemToInternal(t.from), to: this.q_.convertHorzItemToInternal(t.to) }, n = this.yl.Iu(i);
    this.$i.pd(n);
  }
  getVisibleLogicalRange() {
    const t = this.yl.Du();
    return null === t ? null : { from: t.Os(), to: t.ui() };
  }
  setVisibleLogicalRange(t) {
    p(t.from <= t.to, "The from index cannot be after the to index."), this.$i.pd(t);
  }
  resetTimeScale() {
    this.$i.Kn();
  }
  fitContent() {
    this.$i.hc();
  }
  logicalToCoordinate(t) {
    const i = this.$i.St();
    return i.Ni() ? null : i.It(t);
  }
  coordinateToLogical(t) {
    return this.yl.Ni() ? null : this.yl.Nu(t);
  }
  timeToCoordinate(t) {
    const i = this.q_.convertHorzItemToInternal(t), n = this.yl.Va(i, false);
    return null === n ? null : this.yl.It(n);
  }
  coordinateToTime(t) {
    const i = this.$i.St(), n = i.Nu(t), s = i.Ui(n);
    return null === s ? null : s.originalTime;
  }
  width() {
    return this.ab.um().width;
  }
  height() {
    return this.ab.um().height;
  }
  subscribeVisibleTimeRangeChange(t) {
    this.ww.l(t);
  }
  unsubscribeVisibleTimeRangeChange(t) {
    this.ww.v(t);
  }
  subscribeVisibleLogicalRangeChange(t) {
    this.mu.l(t);
  }
  unsubscribeVisibleLogicalRangeChange(t) {
    this.mu.v(t);
  }
  subscribeSizeChange(t) {
    this.Om.l(t);
  }
  unsubscribeSizeChange(t) {
    this.Om.v(t);
  }
  applyOptions(t) {
    this.yl.$h(t);
  }
  options() {
    return Object.assign(Object.assign({}, z(this.yl.W())), { barSpacing: this.yl.le() });
  }
  gw() {
    this.ww.M() && this.ww.m(this.getVisibleRange());
  }
  Mw() {
    this.mu.M() && this.mu.m(this.getVisibleLogicalRange());
  }
  xw(t) {
    this.Om.m(t.width, t.height);
  }
}
function Te(t) {
  if (void 0 === t || "custom" === t.type) return;
  const i = t;
  void 0 !== i.minMove && void 0 === i.precision && (i.precision = function(t2) {
    if (t2 >= 1) return 0;
    let i2 = 0;
    for (; i2 < 8; i2++) {
      const n = Math.round(t2);
      if (Math.abs(n - t2) < 1e-8) return i2;
      t2 *= 10;
    }
    return i2;
  }(i.minMove));
}
function Pe(t) {
  return function(t2) {
    if (I(t2.handleScale)) {
      const i2 = t2.handleScale;
      t2.handleScale = { axisDoubleClickReset: { time: i2, price: i2 }, axisPressedMouseMove: { time: i2, price: i2 }, mouseWheel: i2, pinch: i2 };
    } else if (void 0 !== t2.handleScale) {
      const { axisPressedMouseMove: i2, axisDoubleClickReset: n } = t2.handleScale;
      I(i2) && (t2.handleScale.axisPressedMouseMove = { time: i2, price: i2 }), I(n) && (t2.handleScale.axisDoubleClickReset = { time: n, price: n });
    }
    const i = t2.handleScroll;
    I(i) && (t2.handleScroll = { horzTouchDrag: i, vertTouchDrag: i, mouseWheel: i, pressedMouseMove: i });
  }(t), t;
}
class Re {
  constructor(t, i, n) {
    this.Sw = /* @__PURE__ */ new Map(), this.kw = /* @__PURE__ */ new Map(), this.yw = new D(), this.Cw = new D(), this.Tw = new D(), this.Pw = new se(i);
    const s = void 0 === n ? z(ge()) : V(z(ge()), Pe(n));
    this.q_ = i, this.aw = new Fs(t, s, i), this.aw.lm().l((t2) => {
      this.yw.M() && this.yw.m(this.Rw(t2()));
    }, this), this.aw.am().l((t2) => {
      this.Cw.M() && this.Cw.m(this.Rw(t2()));
    }, this), this.aw.Xc().l((t2) => {
      this.Tw.M() && this.Tw.m(this.Rw(t2()));
    }, this);
    const e2 = this.aw.$t();
    this.Dw = new Ce(e2, this.aw.fb(), this.q_);
  }
  remove() {
    this.aw.lm().p(this), this.aw.am().p(this), this.aw.Xc().p(this), this.Dw.S(), this.aw.S(), this.Sw.clear(), this.kw.clear(), this.yw.S(), this.Cw.S(), this.Tw.S(), this.Pw.S();
  }
  resize(t, i, n) {
    this.autoSizeActive() || this.aw._b(t, i, n);
  }
  addCustomSeries(t, i) {
    const n = w(t), s = Object.assign(Object.assign({}, _), n.defaultOptions());
    return this.Vw("Custom", s, i, n);
  }
  addAreaSeries(t) {
    return this.Vw("Area", l, t);
  }
  addBaselineSeries(t) {
    return this.Vw("Baseline", a, t);
  }
  addBarSeries(t) {
    return this.Vw("Bar", r, t);
  }
  addCandlestickSeries(t = {}) {
    return function(t2) {
      void 0 !== t2.borderColor && (t2.borderUpColor = t2.borderColor, t2.borderDownColor = t2.borderColor), void 0 !== t2.wickColor && (t2.wickUpColor = t2.wickColor, t2.wickDownColor = t2.wickColor);
    }(t), this.Vw("Candlestick", e, t);
  }
  addHistogramSeries(t) {
    return this.Vw("Histogram", o, t);
  }
  addLineSeries(t) {
    return this.Vw("Line", h, t);
  }
  removeSeries(t) {
    const i = m(this.Sw.get(t)), n = this.Pw.vd(i);
    this.aw.$t().vd(i), this.Ow(n), this.Sw.delete(t), this.kw.delete(i);
  }
  pw(t, i) {
    this.Ow(this.Pw.Kb(t, i));
  }
  bw(t, i) {
    this.Ow(this.Pw.iw(t, i));
  }
  subscribeClick(t) {
    this.yw.l(t);
  }
  unsubscribeClick(t) {
    this.yw.v(t);
  }
  subscribeCrosshairMove(t) {
    this.Tw.l(t);
  }
  unsubscribeCrosshairMove(t) {
    this.Tw.v(t);
  }
  subscribeDblClick(t) {
    this.Cw.l(t);
  }
  unsubscribeDblClick(t) {
    this.Cw.v(t);
  }
  priceScale(t) {
    return new Me(this.aw, t);
  }
  timeScale() {
    return this.Dw;
  }
  applyOptions(t) {
    this.aw.$h(Pe(t));
  }
  options() {
    return this.aw.W();
  }
  takeScreenshot() {
    return this.aw.wb();
  }
  autoSizeActive() {
    return this.aw.kb();
  }
  chartElement() {
    return this.aw.yb();
  }
  paneSize() {
    const t = this.aw.Tb();
    return { height: t.height, width: t.width };
  }
  setCrosshairPosition(t, i, n) {
    const s = this.Sw.get(n);
    if (void 0 === s) return;
    const e2 = this.aw.$t().dr(s);
    null !== e2 && this.aw.$t().ad(t, i, e2);
  }
  clearCrosshairPosition() {
    this.aw.$t().od(true);
  }
  Vw(t, i, n = {}, s) {
    Te(n.priceFormat);
    const e2 = V(z(u), z(i), n), r2 = this.aw.$t().dd(t, e2, s), h2 = new ye(r2, this, this, this, this.q_);
    return this.Sw.set(h2, r2), this.kw.set(r2, h2), h2;
  }
  Ow(t) {
    const i = this.aw.$t();
    i._d(t.St.Eu, t.St.hw, t.St.lw), t.ew.forEach((t2, i2) => i2.J(t2.$e, t2.rw)), i.Wu();
  }
  Bw(t) {
    return m(this.kw.get(t));
  }
  Rw(t) {
    const i = /* @__PURE__ */ new Map();
    t.Nb.forEach((t2, n2) => {
      const s = n2.Qh(), e2 = ce(s)(t2);
      if ("Custom" !== s) p(js(e2));
      else {
        const t3 = n2.Ta();
        p(!t3 || false === t3(e2));
      }
      i.set(this.Bw(n2), e2);
    });
    const n = void 0 !== t.Eb && this.kw.has(t.Eb) ? this.Bw(t.Eb) : void 0;
    return { time: t.zb, logical: t.ee, point: t.Lb, hoveredSeries: n, hoveredObjectId: t.Fb, seriesData: i, sourceEvent: t.Wb };
  }
}
function De(t, i, n) {
  let s;
  if (A(t)) {
    const i2 = document.getElementById(t);
    p(null !== i2, `Cannot find element in DOM with id=${t}`), s = i2;
  } else s = t;
  const e2 = new Re(s, i, n);
  return i.setOptions(e2.options()), e2;
}
function Ve(t, i) {
  return De(t, new is(), is.Id(i));
}
Object.assign(Object.assign({}, u), _);
function CoinChartWidget({ coinId, open }) {
  const [timeframe, setTimeframe] = reactExports.useState("7d");
  const [kind, setKind] = reactExports.useState("line");
  const containerRef = reactExports.useRef(null);
  const chartRef = reactExports.useRef(null);
  const seriesRef = reactExports.useRef(null);
  const days = timeframeToDays(timeframe);
  const { data, isLoading, isError } = useCoinChart(coinId, days, kind, open);
  reactExports.useEffect(() => {
    if (!open) return;
    const container = containerRef.current;
    if (!container) return;
    const chart = Ve(container, {
      width: container.clientWidth,
      height: 320,
      layout: {
        background: { type: In.Solid, color: "transparent" },
        textColor: "rgba(220, 220, 230, 0.75)",
        fontFamily: "DM Sans, system-ui, sans-serif",
        fontSize: 11
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.05)", style: d.Dotted },
        horzLines: { color: "rgba(255,255,255,0.05)", style: d.Dotted }
      },
      rightPriceScale: { borderVisible: false, scaleMargins: { top: 0.1, bottom: 0.1 } },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      crosshair: {
        mode: at.Magnet,
        vertLine: { color: "rgba(120,255,180,0.4)", width: 1, style: d.Solid, labelBackgroundColor: "#0f1418" },
        horzLine: { color: "rgba(120,255,180,0.4)", width: 1, style: d.Solid, labelBackgroundColor: "#0f1418" }
      },
      handleScroll: true,
      handleScale: true
    });
    chartRef.current = chart;
    const ro = new ResizeObserver((entries) => {
      for (const e2 of entries) {
        chart.applyOptions({ width: e2.contentRect.width });
      }
    });
    ro.observe(container);
    return () => {
      ro.disconnect();
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
    };
  }, [open]);
  reactExports.useEffect(() => {
    const chart = chartRef.current;
    if (!chart) return;
    if (seriesRef.current) {
      chart.removeSeries(seriesRef.current);
      seriesRef.current = null;
    }
    if (kind === "line") {
      seriesRef.current = chart.addLineSeries({
        color: "oklch(0.72 0.22 145)",
        lineWidth: 2,
        priceFormat: { type: "price", precision: 4, minMove: 1e-4 }
      });
    } else {
      seriesRef.current = chart.addCandlestickSeries({
        upColor: "oklch(0.72 0.22 145)",
        downColor: "oklch(0.62 0.24 25)",
        wickUpColor: "oklch(0.72 0.22 145)",
        wickDownColor: "oklch(0.62 0.24 25)",
        borderVisible: false
      });
    }
  }, [kind]);
  reactExports.useEffect(() => {
    var _a2;
    if (!data || !seriesRef.current) return;
    if (kind === "line") {
      const points = data.line.map((p2) => ({
        time: Math.floor(p2.timestamp / 1e3),
        value: p2.price
      }));
      seriesRef.current.setData(points);
    } else {
      const candles = data.candles.map((c2) => ({
        time: Math.floor(c2.timestamp / 1e3),
        open: c2.open,
        high: c2.high,
        low: c2.low,
        close: c2.close
      }));
      seriesRef.current.setData(candles);
    }
    (_a2 = chartRef.current) == null ? void 0 : _a2.timeScale().fitContent();
  }, [data, kind]);
  const summary = reactExports.useMemo(() => {
    if (!data) return null;
    if (kind === "line" && data.line.length > 0) {
      const first = data.line[0].price;
      const last = data.line[data.line.length - 1].price;
      const change = (last - first) / first * 100;
      return { price: last, change };
    }
    if (kind === "candle" && data.candles.length > 0) {
      const first = data.candles[0].open;
      const last = data.candles[data.candles.length - 1].close;
      const change = (last - first) / first * 100;
      return { price: last, change };
    }
    return null;
  }, [data, kind]);
  return /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "space-y-3", "data-ocid": "coinChart.container", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-baseline justify-between gap-3", children: [
      /* @__PURE__ */ jsxRuntimeExports.jsx("div", { children: summary ? /* @__PURE__ */ jsxRuntimeExports.jsxs(jsxRuntimeExports.Fragment, { children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-2xl font-display font-bold text-foreground tabular-nums", children: formatPrice(summary.price) }),
        /* @__PURE__ */ jsxRuntimeExports.jsxs(
          "p",
          {
            className: `text-xs font-semibold tabular-nums ${summary.change >= 0 ? "text-price-up" : "text-price-down"}`,
            children: [
              summary.change >= 0 ? "▲" : "▼",
              " ",
              Math.abs(summary.change).toFixed(2),
              " % über ",
              timeframeLabel(timeframe)
            ]
          }
        )
      ] }) : /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-xs text-muted-foreground", children: "Lade Chart..." }) }),
      /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center rounded-lg border border-border/60 bg-card p-0.5 shrink-0", children: [
        /* @__PURE__ */ jsxRuntimeExports.jsxs(
          "button",
          {
            type: "button",
            onClick: () => setKind("line"),
            className: `px-2 py-1 rounded-md flex items-center gap-1 text-[11px] font-semibold transition-colors ${kind === "line" ? "bg-primary/15 text-primary" : "text-muted-foreground hover:text-foreground"}`,
            "aria-label": "Linien-Chart",
            "data-ocid": "coinChart.kind_line",
            children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(ChartLine, { className: "w-3.5 h-3.5" }),
              "Linie"
            ]
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsxs(
          "button",
          {
            type: "button",
            onClick: () => setKind("candle"),
            className: `px-2 py-1 rounded-md flex items-center gap-1 text-[11px] font-semibold transition-colors ${kind === "candle" ? "bg-primary/15 text-primary" : "text-muted-foreground hover:text-foreground"}`,
            "aria-label": "Candlestick-Chart",
            "data-ocid": "coinChart.kind_candle",
            children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(ChartColumn, { className: "w-3.5 h-3.5" }),
              "Kerzen"
            ]
          }
        )
      ] })
    ] }),
    /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "relative rounded-lg border border-border/50 bg-background/40 p-2", children: [
      /* @__PURE__ */ jsxRuntimeExports.jsx(
        "div",
        {
          ref: containerRef,
          className: "w-full",
          style: { height: 320 },
          "data-ocid": "coinChart.canvas_container"
        }
      ),
      (isLoading || isError) && /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "absolute inset-0 flex items-center justify-center text-xs text-muted-foreground pointer-events-none", children: isError ? "Chart konnte nicht geladen werden" : "Lade Daten..." })
    ] }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex flex-wrap gap-1", children: CHART_TIMEFRAMES.map((tf) => /* @__PURE__ */ jsxRuntimeExports.jsx(
      "button",
      {
        type: "button",
        onClick: () => setTimeframe(tf),
        className: `px-2.5 py-1 rounded-md text-[11px] font-semibold uppercase tracking-wider transition-colors ${timeframe === tf ? "bg-primary/15 text-primary border border-primary/30" : "bg-card border border-border/60 text-muted-foreground hover:text-foreground"}`,
        "data-ocid": `coinChart.tf_${tf}`,
        children: timeframeLabel(tf)
      },
      tf
    )) })
  ] });
}
function composeEventHandlers(originalEventHandler, ourEventHandler, { checkForDefaultPrevented = true } = {}) {
  return function handleEvent(event) {
    originalEventHandler == null ? void 0 : originalEventHandler(event);
    if (checkForDefaultPrevented === false || !event.defaultPrevented) {
      return ourEventHandler == null ? void 0 : ourEventHandler(event);
    }
  };
}
function setRef(ref, value) {
  if (typeof ref === "function") {
    return ref(value);
  } else if (ref !== null && ref !== void 0) {
    ref.current = value;
  }
}
function composeRefs(...refs) {
  return (node) => {
    let hasCleanup = false;
    const cleanups = refs.map((ref) => {
      const cleanup = setRef(ref, node);
      if (!hasCleanup && typeof cleanup == "function") {
        hasCleanup = true;
      }
      return cleanup;
    });
    if (hasCleanup) {
      return () => {
        for (let i = 0; i < cleanups.length; i++) {
          const cleanup = cleanups[i];
          if (typeof cleanup == "function") {
            cleanup();
          } else {
            setRef(refs[i], null);
          }
        }
      };
    }
  };
}
function useComposedRefs(...refs) {
  return reactExports.useCallback(composeRefs(...refs), refs);
}
function createContext2(rootComponentName, defaultContext) {
  const Context = reactExports.createContext(defaultContext);
  const Provider = (props) => {
    const { children, ...context } = props;
    const value = reactExports.useMemo(() => context, Object.values(context));
    return /* @__PURE__ */ jsxRuntimeExports.jsx(Context.Provider, { value, children });
  };
  Provider.displayName = rootComponentName + "Provider";
  function useContext2(consumerName) {
    const context = reactExports.useContext(Context);
    if (context) return context;
    if (defaultContext !== void 0) return defaultContext;
    throw new Error(`\`${consumerName}\` must be used within \`${rootComponentName}\``);
  }
  return [Provider, useContext2];
}
function createContextScope(scopeName, createContextScopeDeps = []) {
  let defaultContexts = [];
  function createContext3(rootComponentName, defaultContext) {
    const BaseContext = reactExports.createContext(defaultContext);
    const index = defaultContexts.length;
    defaultContexts = [...defaultContexts, defaultContext];
    const Provider = (props) => {
      var _a2;
      const { scope, children, ...context } = props;
      const Context = ((_a2 = scope == null ? void 0 : scope[scopeName]) == null ? void 0 : _a2[index]) || BaseContext;
      const value = reactExports.useMemo(() => context, Object.values(context));
      return /* @__PURE__ */ jsxRuntimeExports.jsx(Context.Provider, { value, children });
    };
    Provider.displayName = rootComponentName + "Provider";
    function useContext2(consumerName, scope) {
      var _a2;
      const Context = ((_a2 = scope == null ? void 0 : scope[scopeName]) == null ? void 0 : _a2[index]) || BaseContext;
      const context = reactExports.useContext(Context);
      if (context) return context;
      if (defaultContext !== void 0) return defaultContext;
      throw new Error(`\`${consumerName}\` must be used within \`${rootComponentName}\``);
    }
    return [Provider, useContext2];
  }
  const createScope = () => {
    const scopeContexts = defaultContexts.map((defaultContext) => {
      return reactExports.createContext(defaultContext);
    });
    return function useScope(scope) {
      const contexts = (scope == null ? void 0 : scope[scopeName]) || scopeContexts;
      return reactExports.useMemo(
        () => ({ [`__scope${scopeName}`]: { ...scope, [scopeName]: contexts } }),
        [scope, contexts]
      );
    };
  };
  createScope.scopeName = scopeName;
  return [createContext3, composeContextScopes(createScope, ...createContextScopeDeps)];
}
function composeContextScopes(...scopes) {
  const baseScope = scopes[0];
  if (scopes.length === 1) return baseScope;
  const createScope = () => {
    const scopeHooks = scopes.map((createScope2) => ({
      useScope: createScope2(),
      scopeName: createScope2.scopeName
    }));
    return function useComposedScopes(overrideScopes) {
      const nextScopes = scopeHooks.reduce((nextScopes2, { useScope, scopeName }) => {
        const scopeProps = useScope(overrideScopes);
        const currentScope = scopeProps[`__scope${scopeName}`];
        return { ...nextScopes2, ...currentScope };
      }, {});
      return reactExports.useMemo(() => ({ [`__scope${baseScope.scopeName}`]: nextScopes }), [nextScopes]);
    };
  };
  createScope.scopeName = baseScope.scopeName;
  return createScope;
}
var useLayoutEffect2 = (globalThis == null ? void 0 : globalThis.document) ? reactExports.useLayoutEffect : () => {
};
var useReactId = React[" useId ".trim().toString()] || (() => void 0);
var count$1 = 0;
function useId(deterministicId) {
  const [id, setId] = reactExports.useState(useReactId());
  useLayoutEffect2(() => {
    setId((reactId) => reactId ?? String(count$1++));
  }, [deterministicId]);
  return deterministicId || (id ? `radix-${id}` : "");
}
var useInsertionEffect = React[" useInsertionEffect ".trim().toString()] || useLayoutEffect2;
function useControllableState({
  prop,
  defaultProp,
  onChange = () => {
  },
  caller
}) {
  const [uncontrolledProp, setUncontrolledProp, onChangeRef] = useUncontrolledState({
    defaultProp,
    onChange
  });
  const isControlled = prop !== void 0;
  const value = isControlled ? prop : uncontrolledProp;
  {
    const isControlledRef = reactExports.useRef(prop !== void 0);
    reactExports.useEffect(() => {
      const wasControlled = isControlledRef.current;
      if (wasControlled !== isControlled) {
        const from = wasControlled ? "controlled" : "uncontrolled";
        const to = isControlled ? "controlled" : "uncontrolled";
        console.warn(
          `${caller} is changing from ${from} to ${to}. Components should not switch from controlled to uncontrolled (or vice versa). Decide between using a controlled or uncontrolled value for the lifetime of the component.`
        );
      }
      isControlledRef.current = isControlled;
    }, [isControlled, caller]);
  }
  const setValue = reactExports.useCallback(
    (nextValue) => {
      var _a2;
      if (isControlled) {
        const value2 = isFunction(nextValue) ? nextValue(prop) : nextValue;
        if (value2 !== prop) {
          (_a2 = onChangeRef.current) == null ? void 0 : _a2.call(onChangeRef, value2);
        }
      } else {
        setUncontrolledProp(nextValue);
      }
    },
    [isControlled, prop, setUncontrolledProp, onChangeRef]
  );
  return [value, setValue];
}
function useUncontrolledState({
  defaultProp,
  onChange
}) {
  const [value, setValue] = reactExports.useState(defaultProp);
  const prevValueRef = reactExports.useRef(value);
  const onChangeRef = reactExports.useRef(onChange);
  useInsertionEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);
  reactExports.useEffect(() => {
    var _a2;
    if (prevValueRef.current !== value) {
      (_a2 = onChangeRef.current) == null ? void 0 : _a2.call(onChangeRef, value);
      prevValueRef.current = value;
    }
  }, [value, prevValueRef]);
  return [value, setValue, onChangeRef];
}
function isFunction(value) {
  return typeof value === "function";
}
// @__NO_SIDE_EFFECTS__
function createSlot$1(ownerName) {
  const SlotClone = /* @__PURE__ */ createSlotClone$1(ownerName);
  const Slot2 = reactExports.forwardRef((props, forwardedRef) => {
    const { children, ...slotProps } = props;
    const childrenArray = reactExports.Children.toArray(children);
    const slottable = childrenArray.find(isSlottable$1);
    if (slottable) {
      const newElement = slottable.props.children;
      const newChildren = childrenArray.map((child) => {
        if (child === slottable) {
          if (reactExports.Children.count(newElement) > 1) return reactExports.Children.only(null);
          return reactExports.isValidElement(newElement) ? newElement.props.children : null;
        } else {
          return child;
        }
      });
      return /* @__PURE__ */ jsxRuntimeExports.jsx(SlotClone, { ...slotProps, ref: forwardedRef, children: reactExports.isValidElement(newElement) ? reactExports.cloneElement(newElement, void 0, newChildren) : null });
    }
    return /* @__PURE__ */ jsxRuntimeExports.jsx(SlotClone, { ...slotProps, ref: forwardedRef, children });
  });
  Slot2.displayName = `${ownerName}.Slot`;
  return Slot2;
}
// @__NO_SIDE_EFFECTS__
function createSlotClone$1(ownerName) {
  const SlotClone = reactExports.forwardRef((props, forwardedRef) => {
    const { children, ...slotProps } = props;
    if (reactExports.isValidElement(children)) {
      const childrenRef = getElementRef$2(children);
      const props2 = mergeProps$1(slotProps, children.props);
      if (children.type !== reactExports.Fragment) {
        props2.ref = forwardedRef ? composeRefs(forwardedRef, childrenRef) : childrenRef;
      }
      return reactExports.cloneElement(children, props2);
    }
    return reactExports.Children.count(children) > 1 ? reactExports.Children.only(null) : null;
  });
  SlotClone.displayName = `${ownerName}.SlotClone`;
  return SlotClone;
}
var SLOTTABLE_IDENTIFIER$1 = Symbol("radix.slottable");
function isSlottable$1(child) {
  return reactExports.isValidElement(child) && typeof child.type === "function" && "__radixId" in child.type && child.type.__radixId === SLOTTABLE_IDENTIFIER$1;
}
function mergeProps$1(slotProps, childProps) {
  const overrideProps = { ...childProps };
  for (const propName in childProps) {
    const slotPropValue = slotProps[propName];
    const childPropValue = childProps[propName];
    const isHandler = /^on[A-Z]/.test(propName);
    if (isHandler) {
      if (slotPropValue && childPropValue) {
        overrideProps[propName] = (...args) => {
          const result = childPropValue(...args);
          slotPropValue(...args);
          return result;
        };
      } else if (slotPropValue) {
        overrideProps[propName] = slotPropValue;
      }
    } else if (propName === "style") {
      overrideProps[propName] = { ...slotPropValue, ...childPropValue };
    } else if (propName === "className") {
      overrideProps[propName] = [slotPropValue, childPropValue].filter(Boolean).join(" ");
    }
  }
  return { ...slotProps, ...overrideProps };
}
function getElementRef$2(element) {
  var _a2, _b;
  let getter = (_a2 = Object.getOwnPropertyDescriptor(element.props, "ref")) == null ? void 0 : _a2.get;
  let mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.ref;
  }
  getter = (_b = Object.getOwnPropertyDescriptor(element, "ref")) == null ? void 0 : _b.get;
  mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.props.ref;
  }
  return element.props.ref || element.ref;
}
var NODES = [
  "a",
  "button",
  "div",
  "form",
  "h2",
  "h3",
  "img",
  "input",
  "label",
  "li",
  "nav",
  "ol",
  "p",
  "select",
  "span",
  "svg",
  "ul"
];
var Primitive = NODES.reduce((primitive, node) => {
  const Slot2 = /* @__PURE__ */ createSlot$1(`Primitive.${node}`);
  const Node2 = reactExports.forwardRef((props, forwardedRef) => {
    const { asChild, ...primitiveProps } = props;
    const Comp = asChild ? Slot2 : node;
    if (typeof window !== "undefined") {
      window[Symbol.for("radix-ui")] = true;
    }
    return /* @__PURE__ */ jsxRuntimeExports.jsx(Comp, { ...primitiveProps, ref: forwardedRef });
  });
  Node2.displayName = `Primitive.${node}`;
  return { ...primitive, [node]: Node2 };
}, {});
function dispatchDiscreteCustomEvent(target, event) {
  if (target) reactDomExports.flushSync(() => target.dispatchEvent(event));
}
function useCallbackRef$1(callback) {
  const callbackRef = reactExports.useRef(callback);
  reactExports.useEffect(() => {
    callbackRef.current = callback;
  });
  return reactExports.useMemo(() => (...args) => {
    var _a2;
    return (_a2 = callbackRef.current) == null ? void 0 : _a2.call(callbackRef, ...args);
  }, []);
}
function useEscapeKeydown(onEscapeKeyDownProp, ownerDocument = globalThis == null ? void 0 : globalThis.document) {
  const onEscapeKeyDown = useCallbackRef$1(onEscapeKeyDownProp);
  reactExports.useEffect(() => {
    const handleKeyDown = (event) => {
      if (event.key === "Escape") {
        onEscapeKeyDown(event);
      }
    };
    ownerDocument.addEventListener("keydown", handleKeyDown, { capture: true });
    return () => ownerDocument.removeEventListener("keydown", handleKeyDown, { capture: true });
  }, [onEscapeKeyDown, ownerDocument]);
}
var DISMISSABLE_LAYER_NAME = "DismissableLayer";
var CONTEXT_UPDATE = "dismissableLayer.update";
var POINTER_DOWN_OUTSIDE = "dismissableLayer.pointerDownOutside";
var FOCUS_OUTSIDE = "dismissableLayer.focusOutside";
var originalBodyPointerEvents;
var DismissableLayerContext = reactExports.createContext({
  layers: /* @__PURE__ */ new Set(),
  layersWithOutsidePointerEventsDisabled: /* @__PURE__ */ new Set(),
  branches: /* @__PURE__ */ new Set()
});
var DismissableLayer = reactExports.forwardRef(
  (props, forwardedRef) => {
    const {
      disableOutsidePointerEvents = false,
      onEscapeKeyDown,
      onPointerDownOutside,
      onFocusOutside,
      onInteractOutside,
      onDismiss,
      ...layerProps
    } = props;
    const context = reactExports.useContext(DismissableLayerContext);
    const [node, setNode] = reactExports.useState(null);
    const ownerDocument = (node == null ? void 0 : node.ownerDocument) ?? (globalThis == null ? void 0 : globalThis.document);
    const [, force] = reactExports.useState({});
    const composedRefs = useComposedRefs(forwardedRef, (node2) => setNode(node2));
    const layers = Array.from(context.layers);
    const [highestLayerWithOutsidePointerEventsDisabled] = [...context.layersWithOutsidePointerEventsDisabled].slice(-1);
    const highestLayerWithOutsidePointerEventsDisabledIndex = layers.indexOf(highestLayerWithOutsidePointerEventsDisabled);
    const index = node ? layers.indexOf(node) : -1;
    const isBodyPointerEventsDisabled = context.layersWithOutsidePointerEventsDisabled.size > 0;
    const isPointerEventsEnabled = index >= highestLayerWithOutsidePointerEventsDisabledIndex;
    const pointerDownOutside = usePointerDownOutside((event) => {
      const target = event.target;
      const isPointerDownOnBranch = [...context.branches].some((branch) => branch.contains(target));
      if (!isPointerEventsEnabled || isPointerDownOnBranch) return;
      onPointerDownOutside == null ? void 0 : onPointerDownOutside(event);
      onInteractOutside == null ? void 0 : onInteractOutside(event);
      if (!event.defaultPrevented) onDismiss == null ? void 0 : onDismiss();
    }, ownerDocument);
    const focusOutside = useFocusOutside((event) => {
      const target = event.target;
      const isFocusInBranch = [...context.branches].some((branch) => branch.contains(target));
      if (isFocusInBranch) return;
      onFocusOutside == null ? void 0 : onFocusOutside(event);
      onInteractOutside == null ? void 0 : onInteractOutside(event);
      if (!event.defaultPrevented) onDismiss == null ? void 0 : onDismiss();
    }, ownerDocument);
    useEscapeKeydown((event) => {
      const isHighestLayer = index === context.layers.size - 1;
      if (!isHighestLayer) return;
      onEscapeKeyDown == null ? void 0 : onEscapeKeyDown(event);
      if (!event.defaultPrevented && onDismiss) {
        event.preventDefault();
        onDismiss();
      }
    }, ownerDocument);
    reactExports.useEffect(() => {
      if (!node) return;
      if (disableOutsidePointerEvents) {
        if (context.layersWithOutsidePointerEventsDisabled.size === 0) {
          originalBodyPointerEvents = ownerDocument.body.style.pointerEvents;
          ownerDocument.body.style.pointerEvents = "none";
        }
        context.layersWithOutsidePointerEventsDisabled.add(node);
      }
      context.layers.add(node);
      dispatchUpdate();
      return () => {
        if (disableOutsidePointerEvents && context.layersWithOutsidePointerEventsDisabled.size === 1) {
          ownerDocument.body.style.pointerEvents = originalBodyPointerEvents;
        }
      };
    }, [node, ownerDocument, disableOutsidePointerEvents, context]);
    reactExports.useEffect(() => {
      return () => {
        if (!node) return;
        context.layers.delete(node);
        context.layersWithOutsidePointerEventsDisabled.delete(node);
        dispatchUpdate();
      };
    }, [node, context]);
    reactExports.useEffect(() => {
      const handleUpdate = () => force({});
      document.addEventListener(CONTEXT_UPDATE, handleUpdate);
      return () => document.removeEventListener(CONTEXT_UPDATE, handleUpdate);
    }, []);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      Primitive.div,
      {
        ...layerProps,
        ref: composedRefs,
        style: {
          pointerEvents: isBodyPointerEventsDisabled ? isPointerEventsEnabled ? "auto" : "none" : void 0,
          ...props.style
        },
        onFocusCapture: composeEventHandlers(props.onFocusCapture, focusOutside.onFocusCapture),
        onBlurCapture: composeEventHandlers(props.onBlurCapture, focusOutside.onBlurCapture),
        onPointerDownCapture: composeEventHandlers(
          props.onPointerDownCapture,
          pointerDownOutside.onPointerDownCapture
        )
      }
    );
  }
);
DismissableLayer.displayName = DISMISSABLE_LAYER_NAME;
var BRANCH_NAME = "DismissableLayerBranch";
var DismissableLayerBranch = reactExports.forwardRef((props, forwardedRef) => {
  const context = reactExports.useContext(DismissableLayerContext);
  const ref = reactExports.useRef(null);
  const composedRefs = useComposedRefs(forwardedRef, ref);
  reactExports.useEffect(() => {
    const node = ref.current;
    if (node) {
      context.branches.add(node);
      return () => {
        context.branches.delete(node);
      };
    }
  }, [context.branches]);
  return /* @__PURE__ */ jsxRuntimeExports.jsx(Primitive.div, { ...props, ref: composedRefs });
});
DismissableLayerBranch.displayName = BRANCH_NAME;
function usePointerDownOutside(onPointerDownOutside, ownerDocument = globalThis == null ? void 0 : globalThis.document) {
  const handlePointerDownOutside = useCallbackRef$1(onPointerDownOutside);
  const isPointerInsideReactTreeRef = reactExports.useRef(false);
  const handleClickRef = reactExports.useRef(() => {
  });
  reactExports.useEffect(() => {
    const handlePointerDown = (event) => {
      if (event.target && !isPointerInsideReactTreeRef.current) {
        let handleAndDispatchPointerDownOutsideEvent2 = function() {
          handleAndDispatchCustomEvent(
            POINTER_DOWN_OUTSIDE,
            handlePointerDownOutside,
            eventDetail,
            { discrete: true }
          );
        };
        const eventDetail = { originalEvent: event };
        if (event.pointerType === "touch") {
          ownerDocument.removeEventListener("click", handleClickRef.current);
          handleClickRef.current = handleAndDispatchPointerDownOutsideEvent2;
          ownerDocument.addEventListener("click", handleClickRef.current, { once: true });
        } else {
          handleAndDispatchPointerDownOutsideEvent2();
        }
      } else {
        ownerDocument.removeEventListener("click", handleClickRef.current);
      }
      isPointerInsideReactTreeRef.current = false;
    };
    const timerId = window.setTimeout(() => {
      ownerDocument.addEventListener("pointerdown", handlePointerDown);
    }, 0);
    return () => {
      window.clearTimeout(timerId);
      ownerDocument.removeEventListener("pointerdown", handlePointerDown);
      ownerDocument.removeEventListener("click", handleClickRef.current);
    };
  }, [ownerDocument, handlePointerDownOutside]);
  return {
    // ensures we check React component tree (not just DOM tree)
    onPointerDownCapture: () => isPointerInsideReactTreeRef.current = true
  };
}
function useFocusOutside(onFocusOutside, ownerDocument = globalThis == null ? void 0 : globalThis.document) {
  const handleFocusOutside = useCallbackRef$1(onFocusOutside);
  const isFocusInsideReactTreeRef = reactExports.useRef(false);
  reactExports.useEffect(() => {
    const handleFocus = (event) => {
      if (event.target && !isFocusInsideReactTreeRef.current) {
        const eventDetail = { originalEvent: event };
        handleAndDispatchCustomEvent(FOCUS_OUTSIDE, handleFocusOutside, eventDetail, {
          discrete: false
        });
      }
    };
    ownerDocument.addEventListener("focusin", handleFocus);
    return () => ownerDocument.removeEventListener("focusin", handleFocus);
  }, [ownerDocument, handleFocusOutside]);
  return {
    onFocusCapture: () => isFocusInsideReactTreeRef.current = true,
    onBlurCapture: () => isFocusInsideReactTreeRef.current = false
  };
}
function dispatchUpdate() {
  const event = new CustomEvent(CONTEXT_UPDATE);
  document.dispatchEvent(event);
}
function handleAndDispatchCustomEvent(name, handler, detail, { discrete }) {
  const target = detail.originalEvent.target;
  const event = new CustomEvent(name, { bubbles: false, cancelable: true, detail });
  if (handler) target.addEventListener(name, handler, { once: true });
  if (discrete) {
    dispatchDiscreteCustomEvent(target, event);
  } else {
    target.dispatchEvent(event);
  }
}
var AUTOFOCUS_ON_MOUNT = "focusScope.autoFocusOnMount";
var AUTOFOCUS_ON_UNMOUNT = "focusScope.autoFocusOnUnmount";
var EVENT_OPTIONS = { bubbles: false, cancelable: true };
var FOCUS_SCOPE_NAME = "FocusScope";
var FocusScope = reactExports.forwardRef((props, forwardedRef) => {
  const {
    loop = false,
    trapped = false,
    onMountAutoFocus: onMountAutoFocusProp,
    onUnmountAutoFocus: onUnmountAutoFocusProp,
    ...scopeProps
  } = props;
  const [container, setContainer] = reactExports.useState(null);
  const onMountAutoFocus = useCallbackRef$1(onMountAutoFocusProp);
  const onUnmountAutoFocus = useCallbackRef$1(onUnmountAutoFocusProp);
  const lastFocusedElementRef = reactExports.useRef(null);
  const composedRefs = useComposedRefs(forwardedRef, (node) => setContainer(node));
  const focusScope = reactExports.useRef({
    paused: false,
    pause() {
      this.paused = true;
    },
    resume() {
      this.paused = false;
    }
  }).current;
  reactExports.useEffect(() => {
    if (trapped) {
      let handleFocusIn2 = function(event) {
        if (focusScope.paused || !container) return;
        const target = event.target;
        if (container.contains(target)) {
          lastFocusedElementRef.current = target;
        } else {
          focus(lastFocusedElementRef.current, { select: true });
        }
      }, handleFocusOut2 = function(event) {
        if (focusScope.paused || !container) return;
        const relatedTarget = event.relatedTarget;
        if (relatedTarget === null) return;
        if (!container.contains(relatedTarget)) {
          focus(lastFocusedElementRef.current, { select: true });
        }
      }, handleMutations2 = function(mutations) {
        const focusedElement = document.activeElement;
        if (focusedElement !== document.body) return;
        for (const mutation of mutations) {
          if (mutation.removedNodes.length > 0) focus(container);
        }
      };
      document.addEventListener("focusin", handleFocusIn2);
      document.addEventListener("focusout", handleFocusOut2);
      const mutationObserver = new MutationObserver(handleMutations2);
      if (container) mutationObserver.observe(container, { childList: true, subtree: true });
      return () => {
        document.removeEventListener("focusin", handleFocusIn2);
        document.removeEventListener("focusout", handleFocusOut2);
        mutationObserver.disconnect();
      };
    }
  }, [trapped, container, focusScope.paused]);
  reactExports.useEffect(() => {
    if (container) {
      focusScopesStack.add(focusScope);
      const previouslyFocusedElement = document.activeElement;
      const hasFocusedCandidate = container.contains(previouslyFocusedElement);
      if (!hasFocusedCandidate) {
        const mountEvent = new CustomEvent(AUTOFOCUS_ON_MOUNT, EVENT_OPTIONS);
        container.addEventListener(AUTOFOCUS_ON_MOUNT, onMountAutoFocus);
        container.dispatchEvent(mountEvent);
        if (!mountEvent.defaultPrevented) {
          focusFirst(removeLinks(getTabbableCandidates(container)), { select: true });
          if (document.activeElement === previouslyFocusedElement) {
            focus(container);
          }
        }
      }
      return () => {
        container.removeEventListener(AUTOFOCUS_ON_MOUNT, onMountAutoFocus);
        setTimeout(() => {
          const unmountEvent = new CustomEvent(AUTOFOCUS_ON_UNMOUNT, EVENT_OPTIONS);
          container.addEventListener(AUTOFOCUS_ON_UNMOUNT, onUnmountAutoFocus);
          container.dispatchEvent(unmountEvent);
          if (!unmountEvent.defaultPrevented) {
            focus(previouslyFocusedElement ?? document.body, { select: true });
          }
          container.removeEventListener(AUTOFOCUS_ON_UNMOUNT, onUnmountAutoFocus);
          focusScopesStack.remove(focusScope);
        }, 0);
      };
    }
  }, [container, onMountAutoFocus, onUnmountAutoFocus, focusScope]);
  const handleKeyDown = reactExports.useCallback(
    (event) => {
      if (!loop && !trapped) return;
      if (focusScope.paused) return;
      const isTabKey = event.key === "Tab" && !event.altKey && !event.ctrlKey && !event.metaKey;
      const focusedElement = document.activeElement;
      if (isTabKey && focusedElement) {
        const container2 = event.currentTarget;
        const [first, last] = getTabbableEdges(container2);
        const hasTabbableElementsInside = first && last;
        if (!hasTabbableElementsInside) {
          if (focusedElement === container2) event.preventDefault();
        } else {
          if (!event.shiftKey && focusedElement === last) {
            event.preventDefault();
            if (loop) focus(first, { select: true });
          } else if (event.shiftKey && focusedElement === first) {
            event.preventDefault();
            if (loop) focus(last, { select: true });
          }
        }
      }
    },
    [loop, trapped, focusScope.paused]
  );
  return /* @__PURE__ */ jsxRuntimeExports.jsx(Primitive.div, { tabIndex: -1, ...scopeProps, ref: composedRefs, onKeyDown: handleKeyDown });
});
FocusScope.displayName = FOCUS_SCOPE_NAME;
function focusFirst(candidates, { select = false } = {}) {
  const previouslyFocusedElement = document.activeElement;
  for (const candidate of candidates) {
    focus(candidate, { select });
    if (document.activeElement !== previouslyFocusedElement) return;
  }
}
function getTabbableEdges(container) {
  const candidates = getTabbableCandidates(container);
  const first = findVisible(candidates, container);
  const last = findVisible(candidates.reverse(), container);
  return [first, last];
}
function getTabbableCandidates(container) {
  const nodes = [];
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_ELEMENT, {
    acceptNode: (node) => {
      const isHiddenInput = node.tagName === "INPUT" && node.type === "hidden";
      if (node.disabled || node.hidden || isHiddenInput) return NodeFilter.FILTER_SKIP;
      return node.tabIndex >= 0 ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_SKIP;
    }
  });
  while (walker.nextNode()) nodes.push(walker.currentNode);
  return nodes;
}
function findVisible(elements, container) {
  for (const element of elements) {
    if (!isHidden(element, { upTo: container })) return element;
  }
}
function isHidden(node, { upTo }) {
  if (getComputedStyle(node).visibility === "hidden") return true;
  while (node) {
    if (upTo !== void 0 && node === upTo) return false;
    if (getComputedStyle(node).display === "none") return true;
    node = node.parentElement;
  }
  return false;
}
function isSelectableInput(element) {
  return element instanceof HTMLInputElement && "select" in element;
}
function focus(element, { select = false } = {}) {
  if (element && element.focus) {
    const previouslyFocusedElement = document.activeElement;
    element.focus({ preventScroll: true });
    if (element !== previouslyFocusedElement && isSelectableInput(element) && select)
      element.select();
  }
}
var focusScopesStack = createFocusScopesStack();
function createFocusScopesStack() {
  let stack = [];
  return {
    add(focusScope) {
      const activeFocusScope = stack[0];
      if (focusScope !== activeFocusScope) {
        activeFocusScope == null ? void 0 : activeFocusScope.pause();
      }
      stack = arrayRemove(stack, focusScope);
      stack.unshift(focusScope);
    },
    remove(focusScope) {
      var _a2;
      stack = arrayRemove(stack, focusScope);
      (_a2 = stack[0]) == null ? void 0 : _a2.resume();
    }
  };
}
function arrayRemove(array, item) {
  const updatedArray = [...array];
  const index = updatedArray.indexOf(item);
  if (index !== -1) {
    updatedArray.splice(index, 1);
  }
  return updatedArray;
}
function removeLinks(items) {
  return items.filter((item) => item.tagName !== "A");
}
var PORTAL_NAME$1 = "Portal";
var Portal$1 = reactExports.forwardRef((props, forwardedRef) => {
  var _a2;
  const { container: containerProp, ...portalProps } = props;
  const [mounted, setMounted] = reactExports.useState(false);
  useLayoutEffect2(() => setMounted(true), []);
  const container = containerProp || mounted && ((_a2 = globalThis == null ? void 0 : globalThis.document) == null ? void 0 : _a2.body);
  return container ? ReactDOM.createPortal(/* @__PURE__ */ jsxRuntimeExports.jsx(Primitive.div, { ...portalProps, ref: forwardedRef }), container) : null;
});
Portal$1.displayName = PORTAL_NAME$1;
function useStateMachine(initialState, machine) {
  return reactExports.useReducer((state, event) => {
    const nextState = machine[state][event];
    return nextState ?? state;
  }, initialState);
}
var Presence = (props) => {
  const { present, children } = props;
  const presence = usePresence(present);
  const child = typeof children === "function" ? children({ present: presence.isPresent }) : reactExports.Children.only(children);
  const ref = useComposedRefs(presence.ref, getElementRef$1(child));
  const forceMount = typeof children === "function";
  return forceMount || presence.isPresent ? reactExports.cloneElement(child, { ref }) : null;
};
Presence.displayName = "Presence";
function usePresence(present) {
  const [node, setNode] = reactExports.useState();
  const stylesRef = reactExports.useRef(null);
  const prevPresentRef = reactExports.useRef(present);
  const prevAnimationNameRef = reactExports.useRef("none");
  const initialState = present ? "mounted" : "unmounted";
  const [state, send] = useStateMachine(initialState, {
    mounted: {
      UNMOUNT: "unmounted",
      ANIMATION_OUT: "unmountSuspended"
    },
    unmountSuspended: {
      MOUNT: "mounted",
      ANIMATION_END: "unmounted"
    },
    unmounted: {
      MOUNT: "mounted"
    }
  });
  reactExports.useEffect(() => {
    const currentAnimationName = getAnimationName(stylesRef.current);
    prevAnimationNameRef.current = state === "mounted" ? currentAnimationName : "none";
  }, [state]);
  useLayoutEffect2(() => {
    const styles = stylesRef.current;
    const wasPresent = prevPresentRef.current;
    const hasPresentChanged = wasPresent !== present;
    if (hasPresentChanged) {
      const prevAnimationName = prevAnimationNameRef.current;
      const currentAnimationName = getAnimationName(styles);
      if (present) {
        send("MOUNT");
      } else if (currentAnimationName === "none" || (styles == null ? void 0 : styles.display) === "none") {
        send("UNMOUNT");
      } else {
        const isAnimating = prevAnimationName !== currentAnimationName;
        if (wasPresent && isAnimating) {
          send("ANIMATION_OUT");
        } else {
          send("UNMOUNT");
        }
      }
      prevPresentRef.current = present;
    }
  }, [present, send]);
  useLayoutEffect2(() => {
    if (node) {
      let timeoutId;
      const ownerWindow = node.ownerDocument.defaultView ?? window;
      const handleAnimationEnd = (event) => {
        const currentAnimationName = getAnimationName(stylesRef.current);
        const isCurrentAnimation = currentAnimationName.includes(CSS.escape(event.animationName));
        if (event.target === node && isCurrentAnimation) {
          send("ANIMATION_END");
          if (!prevPresentRef.current) {
            const currentFillMode = node.style.animationFillMode;
            node.style.animationFillMode = "forwards";
            timeoutId = ownerWindow.setTimeout(() => {
              if (node.style.animationFillMode === "forwards") {
                node.style.animationFillMode = currentFillMode;
              }
            });
          }
        }
      };
      const handleAnimationStart = (event) => {
        if (event.target === node) {
          prevAnimationNameRef.current = getAnimationName(stylesRef.current);
        }
      };
      node.addEventListener("animationstart", handleAnimationStart);
      node.addEventListener("animationcancel", handleAnimationEnd);
      node.addEventListener("animationend", handleAnimationEnd);
      return () => {
        ownerWindow.clearTimeout(timeoutId);
        node.removeEventListener("animationstart", handleAnimationStart);
        node.removeEventListener("animationcancel", handleAnimationEnd);
        node.removeEventListener("animationend", handleAnimationEnd);
      };
    } else {
      send("ANIMATION_END");
    }
  }, [node, send]);
  return {
    isPresent: ["mounted", "unmountSuspended"].includes(state),
    ref: reactExports.useCallback((node2) => {
      stylesRef.current = node2 ? getComputedStyle(node2) : null;
      setNode(node2);
    }, [])
  };
}
function getAnimationName(styles) {
  return (styles == null ? void 0 : styles.animationName) || "none";
}
function getElementRef$1(element) {
  var _a2, _b;
  let getter = (_a2 = Object.getOwnPropertyDescriptor(element.props, "ref")) == null ? void 0 : _a2.get;
  let mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.ref;
  }
  getter = (_b = Object.getOwnPropertyDescriptor(element, "ref")) == null ? void 0 : _b.get;
  mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.props.ref;
  }
  return element.props.ref || element.ref;
}
var count = 0;
function useFocusGuards() {
  reactExports.useEffect(() => {
    const edgeGuards = document.querySelectorAll("[data-radix-focus-guard]");
    document.body.insertAdjacentElement("afterbegin", edgeGuards[0] ?? createFocusGuard());
    document.body.insertAdjacentElement("beforeend", edgeGuards[1] ?? createFocusGuard());
    count++;
    return () => {
      if (count === 1) {
        document.querySelectorAll("[data-radix-focus-guard]").forEach((node) => node.remove());
      }
      count--;
    };
  }, []);
}
function createFocusGuard() {
  const element = document.createElement("span");
  element.setAttribute("data-radix-focus-guard", "");
  element.tabIndex = 0;
  element.style.outline = "none";
  element.style.opacity = "0";
  element.style.position = "fixed";
  element.style.pointerEvents = "none";
  return element;
}
var __assign = function() {
  __assign = Object.assign || function __assign2(t) {
    for (var s, i = 1, n = arguments.length; i < n; i++) {
      s = arguments[i];
      for (var p2 in s) if (Object.prototype.hasOwnProperty.call(s, p2)) t[p2] = s[p2];
    }
    return t;
  };
  return __assign.apply(this, arguments);
};
function __rest(s, e2) {
  var t = {};
  for (var p2 in s) if (Object.prototype.hasOwnProperty.call(s, p2) && e2.indexOf(p2) < 0)
    t[p2] = s[p2];
  if (s != null && typeof Object.getOwnPropertySymbols === "function")
    for (var i = 0, p2 = Object.getOwnPropertySymbols(s); i < p2.length; i++) {
      if (e2.indexOf(p2[i]) < 0 && Object.prototype.propertyIsEnumerable.call(s, p2[i]))
        t[p2[i]] = s[p2[i]];
    }
  return t;
}
function __spreadArray(to, from, pack) {
  if (pack || arguments.length === 2) for (var i = 0, l2 = from.length, ar; i < l2; i++) {
    if (ar || !(i in from)) {
      if (!ar) ar = Array.prototype.slice.call(from, 0, i);
      ar[i] = from[i];
    }
  }
  return to.concat(ar || Array.prototype.slice.call(from));
}
typeof SuppressedError === "function" ? SuppressedError : function(error, suppressed, message) {
  var e2 = new Error(message);
  return e2.name = "SuppressedError", e2.error = error, e2.suppressed = suppressed, e2;
};
var zeroRightClassName = "right-scroll-bar-position";
var fullWidthClassName = "width-before-scroll-bar";
var noScrollbarsClassName = "with-scroll-bars-hidden";
var removedBarSizeVariable = "--removed-body-scroll-bar-size";
function assignRef(ref, value) {
  if (typeof ref === "function") {
    ref(value);
  } else if (ref) {
    ref.current = value;
  }
  return ref;
}
function useCallbackRef(initialValue, callback) {
  var ref = reactExports.useState(function() {
    return {
      // value
      value: initialValue,
      // last callback
      callback,
      // "memoized" public interface
      facade: {
        get current() {
          return ref.value;
        },
        set current(value) {
          var last = ref.value;
          if (last !== value) {
            ref.value = value;
            ref.callback(value, last);
          }
        }
      }
    };
  })[0];
  ref.callback = callback;
  return ref.facade;
}
var useIsomorphicLayoutEffect$1 = typeof window !== "undefined" ? reactExports.useLayoutEffect : reactExports.useEffect;
var currentValues = /* @__PURE__ */ new WeakMap();
function useMergeRefs(refs, defaultValue) {
  var callbackRef = useCallbackRef(null, function(newValue) {
    return refs.forEach(function(ref) {
      return assignRef(ref, newValue);
    });
  });
  useIsomorphicLayoutEffect$1(function() {
    var oldValue = currentValues.get(callbackRef);
    if (oldValue) {
      var prevRefs_1 = new Set(oldValue);
      var nextRefs_1 = new Set(refs);
      var current_1 = callbackRef.current;
      prevRefs_1.forEach(function(ref) {
        if (!nextRefs_1.has(ref)) {
          assignRef(ref, null);
        }
      });
      nextRefs_1.forEach(function(ref) {
        if (!prevRefs_1.has(ref)) {
          assignRef(ref, current_1);
        }
      });
    }
    currentValues.set(callbackRef, refs);
  }, [refs]);
  return callbackRef;
}
function ItoI(a2) {
  return a2;
}
function innerCreateMedium(defaults, middleware) {
  if (middleware === void 0) {
    middleware = ItoI;
  }
  var buffer = [];
  var assigned = false;
  var medium = {
    read: function() {
      if (assigned) {
        throw new Error("Sidecar: could not `read` from an `assigned` medium. `read` could be used only with `useMedium`.");
      }
      if (buffer.length) {
        return buffer[buffer.length - 1];
      }
      return defaults;
    },
    useMedium: function(data) {
      var item = middleware(data, assigned);
      buffer.push(item);
      return function() {
        buffer = buffer.filter(function(x2) {
          return x2 !== item;
        });
      };
    },
    assignSyncMedium: function(cb) {
      assigned = true;
      while (buffer.length) {
        var cbs = buffer;
        buffer = [];
        cbs.forEach(cb);
      }
      buffer = {
        push: function(x2) {
          return cb(x2);
        },
        filter: function() {
          return buffer;
        }
      };
    },
    assignMedium: function(cb) {
      assigned = true;
      var pendingQueue = [];
      if (buffer.length) {
        var cbs = buffer;
        buffer = [];
        cbs.forEach(cb);
        pendingQueue = buffer;
      }
      var executeQueue = function() {
        var cbs2 = pendingQueue;
        pendingQueue = [];
        cbs2.forEach(cb);
      };
      var cycle = function() {
        return Promise.resolve().then(executeQueue);
      };
      cycle();
      buffer = {
        push: function(x2) {
          pendingQueue.push(x2);
          cycle();
        },
        filter: function(filter) {
          pendingQueue = pendingQueue.filter(filter);
          return buffer;
        }
      };
    }
  };
  return medium;
}
function createSidecarMedium(options) {
  if (options === void 0) {
    options = {};
  }
  var medium = innerCreateMedium(null);
  medium.options = __assign({ async: true, ssr: false }, options);
  return medium;
}
var SideCar$1 = function(_a2) {
  var sideCar = _a2.sideCar, rest = __rest(_a2, ["sideCar"]);
  if (!sideCar) {
    throw new Error("Sidecar: please provide `sideCar` property to import the right car");
  }
  var Target = sideCar.read();
  if (!Target) {
    throw new Error("Sidecar medium not found");
  }
  return reactExports.createElement(Target, __assign({}, rest));
};
SideCar$1.isSideCarExport = true;
function exportSidecar(medium, exported) {
  medium.useMedium(exported);
  return SideCar$1;
}
var effectCar = createSidecarMedium();
var nothing = function() {
  return;
};
var RemoveScroll = reactExports.forwardRef(function(props, parentRef) {
  var ref = reactExports.useRef(null);
  var _a2 = reactExports.useState({
    onScrollCapture: nothing,
    onWheelCapture: nothing,
    onTouchMoveCapture: nothing
  }), callbacks = _a2[0], setCallbacks = _a2[1];
  var forwardProps = props.forwardProps, children = props.children, className = props.className, removeScrollBar = props.removeScrollBar, enabled = props.enabled, shards = props.shards, sideCar = props.sideCar, noRelative = props.noRelative, noIsolation = props.noIsolation, inert = props.inert, allowPinchZoom = props.allowPinchZoom, _b = props.as, Container = _b === void 0 ? "div" : _b, gapMode = props.gapMode, rest = __rest(props, ["forwardProps", "children", "className", "removeScrollBar", "enabled", "shards", "sideCar", "noRelative", "noIsolation", "inert", "allowPinchZoom", "as", "gapMode"]);
  var SideCar2 = sideCar;
  var containerRef = useMergeRefs([ref, parentRef]);
  var containerProps = __assign(__assign({}, rest), callbacks);
  return reactExports.createElement(
    reactExports.Fragment,
    null,
    enabled && reactExports.createElement(SideCar2, { sideCar: effectCar, removeScrollBar, shards, noRelative, noIsolation, inert, setCallbacks, allowPinchZoom: !!allowPinchZoom, lockRef: ref, gapMode }),
    forwardProps ? reactExports.cloneElement(reactExports.Children.only(children), __assign(__assign({}, containerProps), { ref: containerRef })) : reactExports.createElement(Container, __assign({}, containerProps, { className, ref: containerRef }), children)
  );
});
RemoveScroll.defaultProps = {
  enabled: true,
  removeScrollBar: true,
  inert: false
};
RemoveScroll.classNames = {
  fullWidth: fullWidthClassName,
  zeroRight: zeroRightClassName
};
var getNonce = function() {
  if (typeof __webpack_nonce__ !== "undefined") {
    return __webpack_nonce__;
  }
  return void 0;
};
function makeStyleTag() {
  if (!document)
    return null;
  var tag = document.createElement("style");
  tag.type = "text/css";
  var nonce = getNonce();
  if (nonce) {
    tag.setAttribute("nonce", nonce);
  }
  return tag;
}
function injectStyles(tag, css) {
  if (tag.styleSheet) {
    tag.styleSheet.cssText = css;
  } else {
    tag.appendChild(document.createTextNode(css));
  }
}
function insertStyleTag(tag) {
  var head = document.head || document.getElementsByTagName("head")[0];
  head.appendChild(tag);
}
var stylesheetSingleton = function() {
  var counter = 0;
  var stylesheet = null;
  return {
    add: function(style) {
      if (counter == 0) {
        if (stylesheet = makeStyleTag()) {
          injectStyles(stylesheet, style);
          insertStyleTag(stylesheet);
        }
      }
      counter++;
    },
    remove: function() {
      counter--;
      if (!counter && stylesheet) {
        stylesheet.parentNode && stylesheet.parentNode.removeChild(stylesheet);
        stylesheet = null;
      }
    }
  };
};
var styleHookSingleton = function() {
  var sheet = stylesheetSingleton();
  return function(styles, isDynamic) {
    reactExports.useEffect(function() {
      sheet.add(styles);
      return function() {
        sheet.remove();
      };
    }, [styles && isDynamic]);
  };
};
var styleSingleton = function() {
  var useStyle = styleHookSingleton();
  var Sheet2 = function(_a2) {
    var styles = _a2.styles, dynamic = _a2.dynamic;
    useStyle(styles, dynamic);
    return null;
  };
  return Sheet2;
};
var zeroGap = {
  left: 0,
  top: 0,
  right: 0,
  gap: 0
};
var parse = function(x2) {
  return parseInt(x2 || "", 10) || 0;
};
var getOffset = function(gapMode) {
  var cs2 = window.getComputedStyle(document.body);
  var left = cs2[gapMode === "padding" ? "paddingLeft" : "marginLeft"];
  var top = cs2[gapMode === "padding" ? "paddingTop" : "marginTop"];
  var right = cs2[gapMode === "padding" ? "paddingRight" : "marginRight"];
  return [parse(left), parse(top), parse(right)];
};
var getGapWidth = function(gapMode) {
  if (gapMode === void 0) {
    gapMode = "margin";
  }
  if (typeof window === "undefined") {
    return zeroGap;
  }
  var offsets = getOffset(gapMode);
  var documentWidth = document.documentElement.clientWidth;
  var windowWidth = window.innerWidth;
  return {
    left: offsets[0],
    top: offsets[1],
    right: offsets[2],
    gap: Math.max(0, windowWidth - documentWidth + offsets[2] - offsets[0])
  };
};
var Style = styleSingleton();
var lockAttribute = "data-scroll-locked";
var getStyles = function(_a2, allowRelative, gapMode, important) {
  var left = _a2.left, top = _a2.top, right = _a2.right, gap = _a2.gap;
  if (gapMode === void 0) {
    gapMode = "margin";
  }
  return "\n  .".concat(noScrollbarsClassName, " {\n   overflow: hidden ").concat(important, ";\n   padding-right: ").concat(gap, "px ").concat(important, ";\n  }\n  body[").concat(lockAttribute, "] {\n    overflow: hidden ").concat(important, ";\n    overscroll-behavior: contain;\n    ").concat([
    allowRelative && "position: relative ".concat(important, ";"),
    gapMode === "margin" && "\n    padding-left: ".concat(left, "px;\n    padding-top: ").concat(top, "px;\n    padding-right: ").concat(right, "px;\n    margin-left:0;\n    margin-top:0;\n    margin-right: ").concat(gap, "px ").concat(important, ";\n    "),
    gapMode === "padding" && "padding-right: ".concat(gap, "px ").concat(important, ";")
  ].filter(Boolean).join(""), "\n  }\n  \n  .").concat(zeroRightClassName, " {\n    right: ").concat(gap, "px ").concat(important, ";\n  }\n  \n  .").concat(fullWidthClassName, " {\n    margin-right: ").concat(gap, "px ").concat(important, ";\n  }\n  \n  .").concat(zeroRightClassName, " .").concat(zeroRightClassName, " {\n    right: 0 ").concat(important, ";\n  }\n  \n  .").concat(fullWidthClassName, " .").concat(fullWidthClassName, " {\n    margin-right: 0 ").concat(important, ";\n  }\n  \n  body[").concat(lockAttribute, "] {\n    ").concat(removedBarSizeVariable, ": ").concat(gap, "px;\n  }\n");
};
var getCurrentUseCounter = function() {
  var counter = parseInt(document.body.getAttribute(lockAttribute) || "0", 10);
  return isFinite(counter) ? counter : 0;
};
var useLockAttribute = function() {
  reactExports.useEffect(function() {
    document.body.setAttribute(lockAttribute, (getCurrentUseCounter() + 1).toString());
    return function() {
      var newCounter = getCurrentUseCounter() - 1;
      if (newCounter <= 0) {
        document.body.removeAttribute(lockAttribute);
      } else {
        document.body.setAttribute(lockAttribute, newCounter.toString());
      }
    };
  }, []);
};
var RemoveScrollBar = function(_a2) {
  var noRelative = _a2.noRelative, noImportant = _a2.noImportant, _b = _a2.gapMode, gapMode = _b === void 0 ? "margin" : _b;
  useLockAttribute();
  var gap = reactExports.useMemo(function() {
    return getGapWidth(gapMode);
  }, [gapMode]);
  return reactExports.createElement(Style, { styles: getStyles(gap, !noRelative, gapMode, !noImportant ? "!important" : "") });
};
var passiveSupported = false;
if (typeof window !== "undefined") {
  try {
    var options = Object.defineProperty({}, "passive", {
      get: function() {
        passiveSupported = true;
        return true;
      }
    });
    window.addEventListener("test", options, options);
    window.removeEventListener("test", options, options);
  } catch (err) {
    passiveSupported = false;
  }
}
var nonPassive = passiveSupported ? { passive: false } : false;
var alwaysContainsScroll = function(node) {
  return node.tagName === "TEXTAREA";
};
var elementCanBeScrolled = function(node, overflow) {
  if (!(node instanceof Element)) {
    return false;
  }
  var styles = window.getComputedStyle(node);
  return (
    // not-not-scrollable
    styles[overflow] !== "hidden" && // contains scroll inside self
    !(styles.overflowY === styles.overflowX && !alwaysContainsScroll(node) && styles[overflow] === "visible")
  );
};
var elementCouldBeVScrolled = function(node) {
  return elementCanBeScrolled(node, "overflowY");
};
var elementCouldBeHScrolled = function(node) {
  return elementCanBeScrolled(node, "overflowX");
};
var locationCouldBeScrolled = function(axis, node) {
  var ownerDocument = node.ownerDocument;
  var current = node;
  do {
    if (typeof ShadowRoot !== "undefined" && current instanceof ShadowRoot) {
      current = current.host;
    }
    var isScrollable = elementCouldBeScrolled(axis, current);
    if (isScrollable) {
      var _a2 = getScrollVariables(axis, current), scrollHeight = _a2[1], clientHeight = _a2[2];
      if (scrollHeight > clientHeight) {
        return true;
      }
    }
    current = current.parentNode;
  } while (current && current !== ownerDocument.body);
  return false;
};
var getVScrollVariables = function(_a2) {
  var scrollTop = _a2.scrollTop, scrollHeight = _a2.scrollHeight, clientHeight = _a2.clientHeight;
  return [
    scrollTop,
    scrollHeight,
    clientHeight
  ];
};
var getHScrollVariables = function(_a2) {
  var scrollLeft = _a2.scrollLeft, scrollWidth = _a2.scrollWidth, clientWidth = _a2.clientWidth;
  return [
    scrollLeft,
    scrollWidth,
    clientWidth
  ];
};
var elementCouldBeScrolled = function(axis, node) {
  return axis === "v" ? elementCouldBeVScrolled(node) : elementCouldBeHScrolled(node);
};
var getScrollVariables = function(axis, node) {
  return axis === "v" ? getVScrollVariables(node) : getHScrollVariables(node);
};
var getDirectionFactor = function(axis, direction) {
  return axis === "h" && direction === "rtl" ? -1 : 1;
};
var handleScroll = function(axis, endTarget, event, sourceDelta, noOverscroll) {
  var directionFactor = getDirectionFactor(axis, window.getComputedStyle(endTarget).direction);
  var delta = directionFactor * sourceDelta;
  var target = event.target;
  var targetInLock = endTarget.contains(target);
  var shouldCancelScroll = false;
  var isDeltaPositive = delta > 0;
  var availableScroll = 0;
  var availableScrollTop = 0;
  do {
    if (!target) {
      break;
    }
    var _a2 = getScrollVariables(axis, target), position = _a2[0], scroll_1 = _a2[1], capacity = _a2[2];
    var elementScroll2 = scroll_1 - capacity - directionFactor * position;
    if (position || elementScroll2) {
      if (elementCouldBeScrolled(axis, target)) {
        availableScroll += elementScroll2;
        availableScrollTop += position;
      }
    }
    var parent_1 = target.parentNode;
    target = parent_1 && parent_1.nodeType === Node.DOCUMENT_FRAGMENT_NODE ? parent_1.host : parent_1;
  } while (
    // portaled content
    !targetInLock && target !== document.body || // self content
    targetInLock && (endTarget.contains(target) || endTarget === target)
  );
  if (isDeltaPositive && (Math.abs(availableScroll) < 1 || false)) {
    shouldCancelScroll = true;
  } else if (!isDeltaPositive && (Math.abs(availableScrollTop) < 1 || false)) {
    shouldCancelScroll = true;
  }
  return shouldCancelScroll;
};
var getTouchXY = function(event) {
  return "changedTouches" in event ? [event.changedTouches[0].clientX, event.changedTouches[0].clientY] : [0, 0];
};
var getDeltaXY = function(event) {
  return [event.deltaX, event.deltaY];
};
var extractRef = function(ref) {
  return ref && "current" in ref ? ref.current : ref;
};
var deltaCompare = function(x2, y2) {
  return x2[0] === y2[0] && x2[1] === y2[1];
};
var generateStyle = function(id) {
  return "\n  .block-interactivity-".concat(id, " {pointer-events: none;}\n  .allow-interactivity-").concat(id, " {pointer-events: all;}\n");
};
var idCounter = 0;
var lockStack = [];
function RemoveScrollSideCar(props) {
  var shouldPreventQueue = reactExports.useRef([]);
  var touchStartRef = reactExports.useRef([0, 0]);
  var activeAxis = reactExports.useRef();
  var id = reactExports.useState(idCounter++)[0];
  var Style2 = reactExports.useState(styleSingleton)[0];
  var lastProps = reactExports.useRef(props);
  reactExports.useEffect(function() {
    lastProps.current = props;
  }, [props]);
  reactExports.useEffect(function() {
    if (props.inert) {
      document.body.classList.add("block-interactivity-".concat(id));
      var allow_1 = __spreadArray([props.lockRef.current], (props.shards || []).map(extractRef), true).filter(Boolean);
      allow_1.forEach(function(el) {
        return el.classList.add("allow-interactivity-".concat(id));
      });
      return function() {
        document.body.classList.remove("block-interactivity-".concat(id));
        allow_1.forEach(function(el) {
          return el.classList.remove("allow-interactivity-".concat(id));
        });
      };
    }
    return;
  }, [props.inert, props.lockRef.current, props.shards]);
  var shouldCancelEvent = reactExports.useCallback(function(event, parent) {
    if ("touches" in event && event.touches.length === 2 || event.type === "wheel" && event.ctrlKey) {
      return !lastProps.current.allowPinchZoom;
    }
    var touch = getTouchXY(event);
    var touchStart = touchStartRef.current;
    var deltaX = "deltaX" in event ? event.deltaX : touchStart[0] - touch[0];
    var deltaY = "deltaY" in event ? event.deltaY : touchStart[1] - touch[1];
    var currentAxis;
    var target = event.target;
    var moveDirection = Math.abs(deltaX) > Math.abs(deltaY) ? "h" : "v";
    if ("touches" in event && moveDirection === "h" && target.type === "range") {
      return false;
    }
    var selection = window.getSelection();
    var anchorNode = selection && selection.anchorNode;
    var isTouchingSelection = anchorNode ? anchorNode === target || anchorNode.contains(target) : false;
    if (isTouchingSelection) {
      return false;
    }
    var canBeScrolledInMainDirection = locationCouldBeScrolled(moveDirection, target);
    if (!canBeScrolledInMainDirection) {
      return true;
    }
    if (canBeScrolledInMainDirection) {
      currentAxis = moveDirection;
    } else {
      currentAxis = moveDirection === "v" ? "h" : "v";
      canBeScrolledInMainDirection = locationCouldBeScrolled(moveDirection, target);
    }
    if (!canBeScrolledInMainDirection) {
      return false;
    }
    if (!activeAxis.current && "changedTouches" in event && (deltaX || deltaY)) {
      activeAxis.current = currentAxis;
    }
    if (!currentAxis) {
      return true;
    }
    var cancelingAxis = activeAxis.current || currentAxis;
    return handleScroll(cancelingAxis, parent, event, cancelingAxis === "h" ? deltaX : deltaY);
  }, []);
  var shouldPrevent = reactExports.useCallback(function(_event) {
    var event = _event;
    if (!lockStack.length || lockStack[lockStack.length - 1] !== Style2) {
      return;
    }
    var delta = "deltaY" in event ? getDeltaXY(event) : getTouchXY(event);
    var sourceEvent = shouldPreventQueue.current.filter(function(e2) {
      return e2.name === event.type && (e2.target === event.target || event.target === e2.shadowParent) && deltaCompare(e2.delta, delta);
    })[0];
    if (sourceEvent && sourceEvent.should) {
      if (event.cancelable) {
        event.preventDefault();
      }
      return;
    }
    if (!sourceEvent) {
      var shardNodes = (lastProps.current.shards || []).map(extractRef).filter(Boolean).filter(function(node) {
        return node.contains(event.target);
      });
      var shouldStop = shardNodes.length > 0 ? shouldCancelEvent(event, shardNodes[0]) : !lastProps.current.noIsolation;
      if (shouldStop) {
        if (event.cancelable) {
          event.preventDefault();
        }
      }
    }
  }, []);
  var shouldCancel = reactExports.useCallback(function(name, delta, target, should) {
    var event = { name, delta, target, should, shadowParent: getOutermostShadowParent(target) };
    shouldPreventQueue.current.push(event);
    setTimeout(function() {
      shouldPreventQueue.current = shouldPreventQueue.current.filter(function(e2) {
        return e2 !== event;
      });
    }, 1);
  }, []);
  var scrollTouchStart = reactExports.useCallback(function(event) {
    touchStartRef.current = getTouchXY(event);
    activeAxis.current = void 0;
  }, []);
  var scrollWheel = reactExports.useCallback(function(event) {
    shouldCancel(event.type, getDeltaXY(event), event.target, shouldCancelEvent(event, props.lockRef.current));
  }, []);
  var scrollTouchMove = reactExports.useCallback(function(event) {
    shouldCancel(event.type, getTouchXY(event), event.target, shouldCancelEvent(event, props.lockRef.current));
  }, []);
  reactExports.useEffect(function() {
    lockStack.push(Style2);
    props.setCallbacks({
      onScrollCapture: scrollWheel,
      onWheelCapture: scrollWheel,
      onTouchMoveCapture: scrollTouchMove
    });
    document.addEventListener("wheel", shouldPrevent, nonPassive);
    document.addEventListener("touchmove", shouldPrevent, nonPassive);
    document.addEventListener("touchstart", scrollTouchStart, nonPassive);
    return function() {
      lockStack = lockStack.filter(function(inst) {
        return inst !== Style2;
      });
      document.removeEventListener("wheel", shouldPrevent, nonPassive);
      document.removeEventListener("touchmove", shouldPrevent, nonPassive);
      document.removeEventListener("touchstart", scrollTouchStart, nonPassive);
    };
  }, []);
  var removeScrollBar = props.removeScrollBar, inert = props.inert;
  return reactExports.createElement(
    reactExports.Fragment,
    null,
    inert ? reactExports.createElement(Style2, { styles: generateStyle(id) }) : null,
    removeScrollBar ? reactExports.createElement(RemoveScrollBar, { noRelative: props.noRelative, gapMode: props.gapMode }) : null
  );
}
function getOutermostShadowParent(node) {
  var shadowParent = null;
  while (node !== null) {
    if (node instanceof ShadowRoot) {
      shadowParent = node.host;
      node = node.host;
    }
    node = node.parentNode;
  }
  return shadowParent;
}
const SideCar = exportSidecar(effectCar, RemoveScrollSideCar);
var ReactRemoveScroll = reactExports.forwardRef(function(props, ref) {
  return reactExports.createElement(RemoveScroll, __assign({}, props, { ref, sideCar: SideCar }));
});
ReactRemoveScroll.classNames = RemoveScroll.classNames;
var getDefaultParent = function(originalTarget) {
  if (typeof document === "undefined") {
    return null;
  }
  var sampleTarget = Array.isArray(originalTarget) ? originalTarget[0] : originalTarget;
  return sampleTarget.ownerDocument.body;
};
var counterMap = /* @__PURE__ */ new WeakMap();
var uncontrolledNodes = /* @__PURE__ */ new WeakMap();
var markerMap = {};
var lockCount = 0;
var unwrapHost = function(node) {
  return node && (node.host || unwrapHost(node.parentNode));
};
var correctTargets = function(parent, targets) {
  return targets.map(function(target) {
    if (parent.contains(target)) {
      return target;
    }
    var correctedTarget = unwrapHost(target);
    if (correctedTarget && parent.contains(correctedTarget)) {
      return correctedTarget;
    }
    console.error("aria-hidden", target, "in not contained inside", parent, ". Doing nothing");
    return null;
  }).filter(function(x2) {
    return Boolean(x2);
  });
};
var applyAttributeToOthers = function(originalTarget, parentNode, markerName, controlAttribute) {
  var targets = correctTargets(parentNode, Array.isArray(originalTarget) ? originalTarget : [originalTarget]);
  if (!markerMap[markerName]) {
    markerMap[markerName] = /* @__PURE__ */ new WeakMap();
  }
  var markerCounter = markerMap[markerName];
  var hiddenNodes = [];
  var elementsToKeep = /* @__PURE__ */ new Set();
  var elementsToStop = new Set(targets);
  var keep = function(el) {
    if (!el || elementsToKeep.has(el)) {
      return;
    }
    elementsToKeep.add(el);
    keep(el.parentNode);
  };
  targets.forEach(keep);
  var deep = function(parent) {
    if (!parent || elementsToStop.has(parent)) {
      return;
    }
    Array.prototype.forEach.call(parent.children, function(node) {
      if (elementsToKeep.has(node)) {
        deep(node);
      } else {
        try {
          var attr = node.getAttribute(controlAttribute);
          var alreadyHidden = attr !== null && attr !== "false";
          var counterValue = (counterMap.get(node) || 0) + 1;
          var markerValue = (markerCounter.get(node) || 0) + 1;
          counterMap.set(node, counterValue);
          markerCounter.set(node, markerValue);
          hiddenNodes.push(node);
          if (counterValue === 1 && alreadyHidden) {
            uncontrolledNodes.set(node, true);
          }
          if (markerValue === 1) {
            node.setAttribute(markerName, "true");
          }
          if (!alreadyHidden) {
            node.setAttribute(controlAttribute, "true");
          }
        } catch (e2) {
          console.error("aria-hidden: cannot operate on ", node, e2);
        }
      }
    });
  };
  deep(parentNode);
  elementsToKeep.clear();
  lockCount++;
  return function() {
    hiddenNodes.forEach(function(node) {
      var counterValue = counterMap.get(node) - 1;
      var markerValue = markerCounter.get(node) - 1;
      counterMap.set(node, counterValue);
      markerCounter.set(node, markerValue);
      if (!counterValue) {
        if (!uncontrolledNodes.has(node)) {
          node.removeAttribute(controlAttribute);
        }
        uncontrolledNodes.delete(node);
      }
      if (!markerValue) {
        node.removeAttribute(markerName);
      }
    });
    lockCount--;
    if (!lockCount) {
      counterMap = /* @__PURE__ */ new WeakMap();
      counterMap = /* @__PURE__ */ new WeakMap();
      uncontrolledNodes = /* @__PURE__ */ new WeakMap();
      markerMap = {};
    }
  };
};
var hideOthers = function(originalTarget, parentNode, markerName) {
  if (markerName === void 0) {
    markerName = "data-aria-hidden";
  }
  var targets = Array.from(Array.isArray(originalTarget) ? originalTarget : [originalTarget]);
  var activeParentNode = getDefaultParent(originalTarget);
  if (!activeParentNode) {
    return function() {
      return null;
    };
  }
  targets.push.apply(targets, Array.from(activeParentNode.querySelectorAll("[aria-live], script")));
  return applyAttributeToOthers(targets, activeParentNode, markerName, "aria-hidden");
};
var DIALOG_NAME = "Dialog";
var [createDialogContext] = createContextScope(DIALOG_NAME);
var [DialogProvider, useDialogContext] = createDialogContext(DIALOG_NAME);
var Dialog = (props) => {
  const {
    __scopeDialog,
    children,
    open: openProp,
    defaultOpen,
    onOpenChange,
    modal = true
  } = props;
  const triggerRef = reactExports.useRef(null);
  const contentRef = reactExports.useRef(null);
  const [open, setOpen] = useControllableState({
    prop: openProp,
    defaultProp: defaultOpen ?? false,
    onChange: onOpenChange,
    caller: DIALOG_NAME
  });
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    DialogProvider,
    {
      scope: __scopeDialog,
      triggerRef,
      contentRef,
      contentId: useId(),
      titleId: useId(),
      descriptionId: useId(),
      open,
      onOpenChange: setOpen,
      onOpenToggle: reactExports.useCallback(() => setOpen((prevOpen) => !prevOpen), [setOpen]),
      modal,
      children
    }
  );
};
Dialog.displayName = DIALOG_NAME;
var TRIGGER_NAME = "DialogTrigger";
var DialogTrigger = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, ...triggerProps } = props;
    const context = useDialogContext(TRIGGER_NAME, __scopeDialog);
    const composedTriggerRef = useComposedRefs(forwardedRef, context.triggerRef);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      Primitive.button,
      {
        type: "button",
        "aria-haspopup": "dialog",
        "aria-expanded": context.open,
        "aria-controls": context.contentId,
        "data-state": getState(context.open),
        ...triggerProps,
        ref: composedTriggerRef,
        onClick: composeEventHandlers(props.onClick, context.onOpenToggle)
      }
    );
  }
);
DialogTrigger.displayName = TRIGGER_NAME;
var PORTAL_NAME = "DialogPortal";
var [PortalProvider, usePortalContext] = createDialogContext(PORTAL_NAME, {
  forceMount: void 0
});
var DialogPortal = (props) => {
  const { __scopeDialog, forceMount, children, container } = props;
  const context = useDialogContext(PORTAL_NAME, __scopeDialog);
  return /* @__PURE__ */ jsxRuntimeExports.jsx(PortalProvider, { scope: __scopeDialog, forceMount, children: reactExports.Children.map(children, (child) => /* @__PURE__ */ jsxRuntimeExports.jsx(Presence, { present: forceMount || context.open, children: /* @__PURE__ */ jsxRuntimeExports.jsx(Portal$1, { asChild: true, container, children: child }) })) });
};
DialogPortal.displayName = PORTAL_NAME;
var OVERLAY_NAME = "DialogOverlay";
var DialogOverlay = reactExports.forwardRef(
  (props, forwardedRef) => {
    const portalContext = usePortalContext(OVERLAY_NAME, props.__scopeDialog);
    const { forceMount = portalContext.forceMount, ...overlayProps } = props;
    const context = useDialogContext(OVERLAY_NAME, props.__scopeDialog);
    return context.modal ? /* @__PURE__ */ jsxRuntimeExports.jsx(Presence, { present: forceMount || context.open, children: /* @__PURE__ */ jsxRuntimeExports.jsx(DialogOverlayImpl, { ...overlayProps, ref: forwardedRef }) }) : null;
  }
);
DialogOverlay.displayName = OVERLAY_NAME;
var Slot$1 = /* @__PURE__ */ createSlot$1("DialogOverlay.RemoveScroll");
var DialogOverlayImpl = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, ...overlayProps } = props;
    const context = useDialogContext(OVERLAY_NAME, __scopeDialog);
    return (
      // Make sure `Content` is scrollable even when it doesn't live inside `RemoveScroll`
      // ie. when `Overlay` and `Content` are siblings
      /* @__PURE__ */ jsxRuntimeExports.jsx(ReactRemoveScroll, { as: Slot$1, allowPinchZoom: true, shards: [context.contentRef], children: /* @__PURE__ */ jsxRuntimeExports.jsx(
        Primitive.div,
        {
          "data-state": getState(context.open),
          ...overlayProps,
          ref: forwardedRef,
          style: { pointerEvents: "auto", ...overlayProps.style }
        }
      ) })
    );
  }
);
var CONTENT_NAME = "DialogContent";
var DialogContent = reactExports.forwardRef(
  (props, forwardedRef) => {
    const portalContext = usePortalContext(CONTENT_NAME, props.__scopeDialog);
    const { forceMount = portalContext.forceMount, ...contentProps } = props;
    const context = useDialogContext(CONTENT_NAME, props.__scopeDialog);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(Presence, { present: forceMount || context.open, children: context.modal ? /* @__PURE__ */ jsxRuntimeExports.jsx(DialogContentModal, { ...contentProps, ref: forwardedRef }) : /* @__PURE__ */ jsxRuntimeExports.jsx(DialogContentNonModal, { ...contentProps, ref: forwardedRef }) });
  }
);
DialogContent.displayName = CONTENT_NAME;
var DialogContentModal = reactExports.forwardRef(
  (props, forwardedRef) => {
    const context = useDialogContext(CONTENT_NAME, props.__scopeDialog);
    const contentRef = reactExports.useRef(null);
    const composedRefs = useComposedRefs(forwardedRef, context.contentRef, contentRef);
    reactExports.useEffect(() => {
      const content = contentRef.current;
      if (content) return hideOthers(content);
    }, []);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      DialogContentImpl,
      {
        ...props,
        ref: composedRefs,
        trapFocus: context.open,
        disableOutsidePointerEvents: true,
        onCloseAutoFocus: composeEventHandlers(props.onCloseAutoFocus, (event) => {
          var _a2;
          event.preventDefault();
          (_a2 = context.triggerRef.current) == null ? void 0 : _a2.focus();
        }),
        onPointerDownOutside: composeEventHandlers(props.onPointerDownOutside, (event) => {
          const originalEvent = event.detail.originalEvent;
          const ctrlLeftClick = originalEvent.button === 0 && originalEvent.ctrlKey === true;
          const isRightClick = originalEvent.button === 2 || ctrlLeftClick;
          if (isRightClick) event.preventDefault();
        }),
        onFocusOutside: composeEventHandlers(
          props.onFocusOutside,
          (event) => event.preventDefault()
        )
      }
    );
  }
);
var DialogContentNonModal = reactExports.forwardRef(
  (props, forwardedRef) => {
    const context = useDialogContext(CONTENT_NAME, props.__scopeDialog);
    const hasInteractedOutsideRef = reactExports.useRef(false);
    const hasPointerDownOutsideRef = reactExports.useRef(false);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      DialogContentImpl,
      {
        ...props,
        ref: forwardedRef,
        trapFocus: false,
        disableOutsidePointerEvents: false,
        onCloseAutoFocus: (event) => {
          var _a2, _b;
          (_a2 = props.onCloseAutoFocus) == null ? void 0 : _a2.call(props, event);
          if (!event.defaultPrevented) {
            if (!hasInteractedOutsideRef.current) (_b = context.triggerRef.current) == null ? void 0 : _b.focus();
            event.preventDefault();
          }
          hasInteractedOutsideRef.current = false;
          hasPointerDownOutsideRef.current = false;
        },
        onInteractOutside: (event) => {
          var _a2, _b;
          (_a2 = props.onInteractOutside) == null ? void 0 : _a2.call(props, event);
          if (!event.defaultPrevented) {
            hasInteractedOutsideRef.current = true;
            if (event.detail.originalEvent.type === "pointerdown") {
              hasPointerDownOutsideRef.current = true;
            }
          }
          const target = event.target;
          const targetIsTrigger = (_b = context.triggerRef.current) == null ? void 0 : _b.contains(target);
          if (targetIsTrigger) event.preventDefault();
          if (event.detail.originalEvent.type === "focusin" && hasPointerDownOutsideRef.current) {
            event.preventDefault();
          }
        }
      }
    );
  }
);
var DialogContentImpl = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, trapFocus, onOpenAutoFocus, onCloseAutoFocus, ...contentProps } = props;
    const context = useDialogContext(CONTENT_NAME, __scopeDialog);
    const contentRef = reactExports.useRef(null);
    const composedRefs = useComposedRefs(forwardedRef, contentRef);
    useFocusGuards();
    return /* @__PURE__ */ jsxRuntimeExports.jsxs(jsxRuntimeExports.Fragment, { children: [
      /* @__PURE__ */ jsxRuntimeExports.jsx(
        FocusScope,
        {
          asChild: true,
          loop: true,
          trapped: trapFocus,
          onMountAutoFocus: onOpenAutoFocus,
          onUnmountAutoFocus: onCloseAutoFocus,
          children: /* @__PURE__ */ jsxRuntimeExports.jsx(
            DismissableLayer,
            {
              role: "dialog",
              id: context.contentId,
              "aria-describedby": context.descriptionId,
              "aria-labelledby": context.titleId,
              "data-state": getState(context.open),
              ...contentProps,
              ref: composedRefs,
              onDismiss: () => context.onOpenChange(false)
            }
          )
        }
      ),
      /* @__PURE__ */ jsxRuntimeExports.jsxs(jsxRuntimeExports.Fragment, { children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(TitleWarning, { titleId: context.titleId }),
        /* @__PURE__ */ jsxRuntimeExports.jsx(DescriptionWarning, { contentRef, descriptionId: context.descriptionId })
      ] })
    ] });
  }
);
var TITLE_NAME = "DialogTitle";
var DialogTitle = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, ...titleProps } = props;
    const context = useDialogContext(TITLE_NAME, __scopeDialog);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(Primitive.h2, { id: context.titleId, ...titleProps, ref: forwardedRef });
  }
);
DialogTitle.displayName = TITLE_NAME;
var DESCRIPTION_NAME = "DialogDescription";
var DialogDescription = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, ...descriptionProps } = props;
    const context = useDialogContext(DESCRIPTION_NAME, __scopeDialog);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(Primitive.p, { id: context.descriptionId, ...descriptionProps, ref: forwardedRef });
  }
);
DialogDescription.displayName = DESCRIPTION_NAME;
var CLOSE_NAME = "DialogClose";
var DialogClose = reactExports.forwardRef(
  (props, forwardedRef) => {
    const { __scopeDialog, ...closeProps } = props;
    const context = useDialogContext(CLOSE_NAME, __scopeDialog);
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      Primitive.button,
      {
        type: "button",
        ...closeProps,
        ref: forwardedRef,
        onClick: composeEventHandlers(props.onClick, () => context.onOpenChange(false))
      }
    );
  }
);
DialogClose.displayName = CLOSE_NAME;
function getState(open) {
  return open ? "open" : "closed";
}
var TITLE_WARNING_NAME = "DialogTitleWarning";
var [WarningProvider, useWarningContext] = createContext2(TITLE_WARNING_NAME, {
  contentName: CONTENT_NAME,
  titleName: TITLE_NAME,
  docsSlug: "dialog"
});
var TitleWarning = ({ titleId }) => {
  const titleWarningContext = useWarningContext(TITLE_WARNING_NAME);
  const MESSAGE = `\`${titleWarningContext.contentName}\` requires a \`${titleWarningContext.titleName}\` for the component to be accessible for screen reader users.

If you want to hide the \`${titleWarningContext.titleName}\`, you can wrap it with our VisuallyHidden component.

For more information, see https://radix-ui.com/primitives/docs/components/${titleWarningContext.docsSlug}`;
  reactExports.useEffect(() => {
    if (titleId) {
      const hasTitle = document.getElementById(titleId);
      if (!hasTitle) console.error(MESSAGE);
    }
  }, [MESSAGE, titleId]);
  return null;
};
var DESCRIPTION_WARNING_NAME = "DialogDescriptionWarning";
var DescriptionWarning = ({ contentRef, descriptionId }) => {
  const descriptionWarningContext = useWarningContext(DESCRIPTION_WARNING_NAME);
  const MESSAGE = `Warning: Missing \`Description\` or \`aria-describedby={undefined}\` for {${descriptionWarningContext.contentName}}.`;
  reactExports.useEffect(() => {
    var _a2;
    const describedById = (_a2 = contentRef.current) == null ? void 0 : _a2.getAttribute("aria-describedby");
    if (descriptionId && describedById) {
      const hasDescription = document.getElementById(descriptionId);
      if (!hasDescription) console.warn(MESSAGE);
    }
  }, [MESSAGE, contentRef, descriptionId]);
  return null;
};
var Root = Dialog;
var Portal = DialogPortal;
var Overlay = DialogOverlay;
var Content = DialogContent;
var Title = DialogTitle;
var Description = DialogDescription;
var Close = DialogClose;
function Sheet({ ...props }) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(Root, { "data-slot": "sheet", ...props });
}
function SheetPortal({
  ...props
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(Portal, { "data-slot": "sheet-portal", ...props });
}
function SheetOverlay({
  className,
  ...props
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    Overlay,
    {
      "data-slot": "sheet-overlay",
      className: cn$1(
        "data-[state=open]:animate-in data-[state=closed]:animate-out data-[state=closed]:fade-out-0 data-[state=open]:fade-in-0 fixed inset-0 z-50 bg-black/50",
        className
      ),
      ...props
    }
  );
}
function SheetContent({
  className,
  children,
  side = "right",
  ...props
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(SheetPortal, { children: [
    /* @__PURE__ */ jsxRuntimeExports.jsx(SheetOverlay, {}),
    /* @__PURE__ */ jsxRuntimeExports.jsxs(
      Content,
      {
        "data-slot": "sheet-content",
        className: cn$1(
          "bg-background data-[state=open]:animate-in data-[state=closed]:animate-out fixed z-50 flex flex-col gap-4 shadow-lg transition ease-in-out data-[state=closed]:duration-300 data-[state=open]:duration-500",
          side === "right" && "data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right inset-y-0 right-0 h-full w-3/4 border-l sm:max-w-sm",
          side === "left" && "data-[state=closed]:slide-out-to-left data-[state=open]:slide-in-from-left inset-y-0 left-0 h-full w-3/4 border-r sm:max-w-sm",
          side === "top" && "data-[state=closed]:slide-out-to-top data-[state=open]:slide-in-from-top inset-x-0 top-0 h-auto border-b",
          side === "bottom" && "data-[state=closed]:slide-out-to-bottom data-[state=open]:slide-in-from-bottom inset-x-0 bottom-0 h-auto border-t",
          className
        ),
        ...props,
        children: [
          children,
          /* @__PURE__ */ jsxRuntimeExports.jsxs(Close, { className: "ring-offset-background focus:ring-ring data-[state=open]:bg-secondary absolute top-4 right-4 rounded-xs opacity-70 transition-opacity hover:opacity-100 focus:ring-2 focus:ring-offset-2 focus:outline-hidden disabled:pointer-events-none", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(X$1, { className: "size-4" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "sr-only", children: "Close" })
          ] })
        ]
      }
    )
  ] });
}
function SheetHeader({ className, ...props }) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    "div",
    {
      "data-slot": "sheet-header",
      className: cn$1("flex flex-col gap-1.5 p-4", className),
      ...props
    }
  );
}
function SheetTitle({
  className,
  ...props
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    Title,
    {
      "data-slot": "sheet-title",
      className: cn$1("text-foreground font-semibold", className),
      ...props
    }
  );
}
function SheetDescription({
  className,
  ...props
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    Description,
    {
      "data-slot": "sheet-description",
      className: cn$1("text-muted-foreground text-sm", className),
      ...props
    }
  );
}
function StatRow({ label, value, trend }) {
  const trendColor = trend === void 0 ? "text-foreground" : trend >= 0 ? "text-price-up" : "text-price-down";
  return /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center justify-between py-2.5 border-b border-border/40 last:border-b-0", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-xs text-muted-foreground", children: label }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: `text-sm font-mono font-semibold tabular-nums ${trendColor}`, children: value })
  ] });
}
function CoinDetailDrawer({
  coin,
  open,
  onOpenChange,
  isFavorite,
  onToggleFavorite
}) {
  if (!coin) return null;
  const positive24h = coin.priceChangePercentage24h >= 0;
  return /* @__PURE__ */ jsxRuntimeExports.jsx(Sheet, { open, onOpenChange, children: /* @__PURE__ */ jsxRuntimeExports.jsxs(
    SheetContent,
    {
      side: "right",
      className: "w-full sm:max-w-xl bg-card border-border/60 overflow-y-auto p-0",
      "data-ocid": "coinDetail.drawer",
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(SheetHeader, { className: "px-5 pt-5 pb-3 border-b border-border/40", children: /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-3", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            "img",
            {
              src: coin.image,
              alt: coin.name,
              className: "w-12 h-12 rounded-full shrink-0",
              onError: (e2) => {
                e2.target.style.visibility = "hidden";
              }
            }
          ),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "min-w-0 flex-1", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-2", children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(SheetTitle, { className: "text-lg font-display font-bold text-foreground truncate", children: coin.name }),
              /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-xs text-muted-foreground uppercase tracking-wider shrink-0", children: coin.symbol }),
              /* @__PURE__ */ jsxRuntimeExports.jsxs("span", { className: "text-[10px] text-muted-foreground font-mono px-1.5 py-0.5 rounded bg-muted/40 shrink-0", children: [
                "#",
                coin.marketCapRank
              ] })
            ] }),
            /* @__PURE__ */ jsxRuntimeExports.jsxs(SheetDescription, { className: "text-xs flex items-baseline gap-2", children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "font-mono text-foreground tabular-nums", children: formatPrice(coin.currentPrice) }),
              /* @__PURE__ */ jsxRuntimeExports.jsxs("span", { className: `font-semibold tabular-nums ${positive24h ? "text-price-up" : "text-price-down"}`, children: [
                formatPercent(coin.priceChangePercentage24h),
                " (24h)"
              ] })
            ] })
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            "button",
            {
              type: "button",
              onClick: () => onToggleFavorite(coin.id),
              className: `shrink-0 w-9 h-9 rounded-full flex items-center justify-center border transition-colors ${isFavorite ? "bg-primary/15 border-primary/30 text-primary" : "bg-card border-border/60 text-muted-foreground hover:text-foreground"}`,
              "aria-label": isFavorite ? "Aus Watchlist entfernen" : "Zur Watchlist hinzufügen",
              "data-ocid": "coinDetail.favorite_button",
              children: /* @__PURE__ */ jsxRuntimeExports.jsx(Star, { className: "w-4 h-4", fill: isFavorite ? "currentColor" : "none" })
            }
          )
        ] }) }),
        /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "px-5 py-4 space-y-5", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx(CoinChartWidget, { coinId: coin.id, open }),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-2", children: "Performance" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "grid grid-cols-3 gap-2", children: [
              { label: "1h", value: coin.priceChangePercentage1h },
              { label: "24h", value: coin.priceChangePercentage24h },
              { label: "7d", value: coin.priceChangePercentage7d }
            ].map((p2) => {
              const pos = p2.value >= 0;
              return /* @__PURE__ */ jsxRuntimeExports.jsxs(
                "div",
                {
                  className: "rounded-lg border border-border/50 bg-background/40 px-3 py-2.5 text-center",
                  children: [
                    /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-[10px] text-muted-foreground uppercase tracking-wider", children: p2.label }),
                    /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: `text-sm font-mono font-semibold tabular-nums mt-0.5 ${pos ? "text-price-up" : "text-price-down"}`, children: formatPercent(p2.value) })
                  ]
                },
                p2.label
              );
            }) })
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-[10px] font-medium text-muted-foreground uppercase tracking-wider mb-1", children: "Marktdaten" }),
            /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "rounded-lg border border-border/50 bg-background/40 px-3", children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "Marktkapitalisierung", value: `${formatCompactNumber(coin.marketCap)} €` }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "24h Volumen", value: `${formatCompactNumber(coin.totalVolume)} €` }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "24h Hoch", value: formatPrice(coin.high24h) }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "24h Tief", value: formatPrice(coin.low24h) }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "All-Time-High", value: formatPrice(coin.ath), trend: coin.athChangePercentage }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "Abstand zum ATH", value: formatPercent(coin.athChangePercentage), trend: coin.athChangePercentage }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "Umlaufmenge", value: formatSupply(coin.circulatingSupply, coin.symbol) }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(StatRow, { label: "Gesamtmenge", value: formatSupply(coin.totalSupply, coin.symbol) })
            ] })
          ] })
        ] })
      ]
    }
  ) });
}
function StatItem({ label, value, trend, testId }) {
  const trendColor = trend === void 0 ? "text-muted-foreground" : trend >= 0 ? "text-price-up" : "text-price-down";
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    "div",
    {
      className: "flex flex-col gap-0.5 px-3 sm:px-4 py-2.5 border-r border-border/40 last:border-r-0 min-w-[120px]",
      "data-ocid": testId,
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-[10px] font-medium text-muted-foreground uppercase tracking-wider", children: label }),
        /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-baseline gap-1.5", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-sm font-mono font-semibold text-foreground tabular-nums", children: value }),
          trend !== void 0 && /* @__PURE__ */ jsxRuntimeExports.jsxs("span", { className: `text-[11px] font-semibold ${trendColor}`, children: [
            trend >= 0 ? "▲" : "▼",
            formatPercentPlain(Math.abs(trend))
          ] })
        ] })
      ]
    }
  );
}
function GlobalStatsBar({
  data,
  isLoading,
  coinCount
}) {
  if (isLoading) {
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      "div",
      {
        className: "rounded-xl border border-border/60 bg-card overflow-hidden mb-4 sm:mb-6",
        "data-ocid": "globalStats.loading",
        children: /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex flex-wrap", children: ["a", "b", "c", "d", "e"].map((k2) => /* @__PURE__ */ jsxRuntimeExports.jsxs(
          "div",
          {
            className: "flex flex-col gap-1 px-3 sm:px-4 py-2.5 border-r border-border/40 last:border-r-0 min-w-[120px]",
            children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-3 w-16 rounded" }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-4 w-24 rounded" })
            ]
          },
          k2
        )) })
      }
    );
  }
  if (!data) {
    return null;
  }
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    "div",
    {
      className: "rounded-xl border border-border/60 bg-card overflow-hidden mb-4 sm:mb-6",
      "data-ocid": "globalStats.container",
      children: /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex flex-wrap", children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          StatItem,
          {
            label: "Marktkapital.",
            value: `${formatCompactNumber(data.totalMarketCap)} €`,
            trend: data.marketCapChangePercentage24h,
            testId: "globalStats.marketCap"
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          StatItem,
          {
            label: "24h Volumen",
            value: `${formatCompactNumber(data.totalVolume24h)} €`,
            testId: "globalStats.volume"
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          StatItem,
          {
            label: "BTC Dominanz",
            value: formatPercentPlain(data.btcDominance),
            testId: "globalStats.btcDominance"
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          StatItem,
          {
            label: "ETH Dominanz",
            value: formatPercentPlain(data.ethDominance),
            testId: "globalStats.ethDominance"
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          StatItem,
          {
            label: "Coins",
            value: `${data.activeCryptocurrencies > 0 ? data.activeCryptocurrencies.toLocaleString("de-DE") : coinCount}`,
            testId: "globalStats.activeCoins"
          }
        )
      ] })
    }
  );
}
var REACT_LAZY_TYPE = Symbol.for("react.lazy");
var use = React[" use ".trim().toString()];
function isPromiseLike(value) {
  return typeof value === "object" && value !== null && "then" in value;
}
function isLazyComponent(element) {
  return element != null && typeof element === "object" && "$$typeof" in element && element.$$typeof === REACT_LAZY_TYPE && "_payload" in element && isPromiseLike(element._payload);
}
// @__NO_SIDE_EFFECTS__
function createSlot(ownerName) {
  const SlotClone = /* @__PURE__ */ createSlotClone(ownerName);
  const Slot2 = reactExports.forwardRef((props, forwardedRef) => {
    let { children, ...slotProps } = props;
    if (isLazyComponent(children) && typeof use === "function") {
      children = use(children._payload);
    }
    const childrenArray = reactExports.Children.toArray(children);
    const slottable = childrenArray.find(isSlottable);
    if (slottable) {
      const newElement = slottable.props.children;
      const newChildren = childrenArray.map((child) => {
        if (child === slottable) {
          if (reactExports.Children.count(newElement) > 1) return reactExports.Children.only(null);
          return reactExports.isValidElement(newElement) ? newElement.props.children : null;
        } else {
          return child;
        }
      });
      return /* @__PURE__ */ jsxRuntimeExports.jsx(SlotClone, { ...slotProps, ref: forwardedRef, children: reactExports.isValidElement(newElement) ? reactExports.cloneElement(newElement, void 0, newChildren) : null });
    }
    return /* @__PURE__ */ jsxRuntimeExports.jsx(SlotClone, { ...slotProps, ref: forwardedRef, children });
  });
  Slot2.displayName = `${ownerName}.Slot`;
  return Slot2;
}
var Slot = /* @__PURE__ */ createSlot("Slot");
// @__NO_SIDE_EFFECTS__
function createSlotClone(ownerName) {
  const SlotClone = reactExports.forwardRef((props, forwardedRef) => {
    let { children, ...slotProps } = props;
    if (isLazyComponent(children) && typeof use === "function") {
      children = use(children._payload);
    }
    if (reactExports.isValidElement(children)) {
      const childrenRef = getElementRef(children);
      const props2 = mergeProps(slotProps, children.props);
      if (children.type !== reactExports.Fragment) {
        props2.ref = forwardedRef ? composeRefs(forwardedRef, childrenRef) : childrenRef;
      }
      return reactExports.cloneElement(children, props2);
    }
    return reactExports.Children.count(children) > 1 ? reactExports.Children.only(null) : null;
  });
  SlotClone.displayName = `${ownerName}.SlotClone`;
  return SlotClone;
}
var SLOTTABLE_IDENTIFIER = Symbol("radix.slottable");
function isSlottable(child) {
  return reactExports.isValidElement(child) && typeof child.type === "function" && "__radixId" in child.type && child.type.__radixId === SLOTTABLE_IDENTIFIER;
}
function mergeProps(slotProps, childProps) {
  const overrideProps = { ...childProps };
  for (const propName in childProps) {
    const slotPropValue = slotProps[propName];
    const childPropValue = childProps[propName];
    const isHandler = /^on[A-Z]/.test(propName);
    if (isHandler) {
      if (slotPropValue && childPropValue) {
        overrideProps[propName] = (...args) => {
          const result = childPropValue(...args);
          slotPropValue(...args);
          return result;
        };
      } else if (slotPropValue) {
        overrideProps[propName] = slotPropValue;
      }
    } else if (propName === "style") {
      overrideProps[propName] = { ...slotPropValue, ...childPropValue };
    } else if (propName === "className") {
      overrideProps[propName] = [slotPropValue, childPropValue].filter(Boolean).join(" ");
    }
  }
  return { ...slotProps, ...overrideProps };
}
function getElementRef(element) {
  var _a2, _b;
  let getter = (_a2 = Object.getOwnPropertyDescriptor(element.props, "ref")) == null ? void 0 : _a2.get;
  let mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.ref;
  }
  getter = (_b = Object.getOwnPropertyDescriptor(element, "ref")) == null ? void 0 : _b.get;
  mayWarn = getter && "isReactWarning" in getter && getter.isReactWarning;
  if (mayWarn) {
    return element.props.ref;
  }
  return element.props.ref || element.ref;
}
const falsyToString = (value) => typeof value === "boolean" ? `${value}` : value === 0 ? "0" : value;
const cx = clsx;
const cva = (base, config) => (props) => {
  var _config_compoundVariants;
  if ((config === null || config === void 0 ? void 0 : config.variants) == null) return cx(base, props === null || props === void 0 ? void 0 : props.class, props === null || props === void 0 ? void 0 : props.className);
  const { variants, defaultVariants } = config;
  const getVariantClassNames = Object.keys(variants).map((variant) => {
    const variantProp = props === null || props === void 0 ? void 0 : props[variant];
    const defaultVariantProp = defaultVariants === null || defaultVariants === void 0 ? void 0 : defaultVariants[variant];
    if (variantProp === null) return null;
    const variantKey = falsyToString(variantProp) || falsyToString(defaultVariantProp);
    return variants[variant][variantKey];
  });
  const propsWithoutUndefined = props && Object.entries(props).reduce((acc, param) => {
    let [key, value] = param;
    if (value === void 0) {
      return acc;
    }
    acc[key] = value;
    return acc;
  }, {});
  const getCompoundVariantClassNames = config === null || config === void 0 ? void 0 : (_config_compoundVariants = config.compoundVariants) === null || _config_compoundVariants === void 0 ? void 0 : _config_compoundVariants.reduce((acc, param) => {
    let { class: cvClass, className: cvClassName, ...compoundVariantOptions } = param;
    return Object.entries(compoundVariantOptions).every((param2) => {
      let [key, value] = param2;
      return Array.isArray(value) ? value.includes({
        ...defaultVariants,
        ...propsWithoutUndefined
      }[key]) : {
        ...defaultVariants,
        ...propsWithoutUndefined
      }[key] === value;
    }) ? [
      ...acc,
      cvClass,
      cvClassName
    ] : acc;
  }, []);
  return cx(base, getVariantClassNames, getCompoundVariantClassNames, props === null || props === void 0 ? void 0 : props.class, props === null || props === void 0 ? void 0 : props.className);
};
const badgeVariants = cva(
  "inline-flex items-center justify-center rounded-md border px-2 py-0.5 text-xs font-medium w-fit whitespace-nowrap shrink-0 [&>svg]:size-3 gap-1 [&>svg]:pointer-events-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive transition-[color,box-shadow] overflow-hidden",
  {
    variants: {
      variant: {
        default: "border-transparent bg-primary text-primary-foreground [a&]:hover:bg-primary/90",
        secondary: "border-transparent bg-secondary text-secondary-foreground [a&]:hover:bg-secondary/90",
        destructive: "border-transparent bg-destructive text-destructive-foreground [a&]:hover:bg-destructive/90 focus-visible:ring-destructive/20 dark:focus-visible:ring-destructive/40 dark:bg-destructive/60",
        outline: "text-foreground [a&]:hover:bg-accent [a&]:hover:text-accent-foreground"
      }
    },
    defaultVariants: {
      variant: "default"
    }
  }
);
function Badge({
  className,
  variant,
  asChild = false,
  ...props
}) {
  const Comp = asChild ? Slot : "span";
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    Comp,
    {
      "data-slot": "badge",
      className: cn$1(badgeVariants({ variant }), className),
      ...props
    }
  );
}
function Header({ lastUpdated, isLive, isLoading }) {
  const timeStr = lastUpdated ? lastUpdated.toLocaleTimeString("de-DE", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit"
  }) : null;
  return /* @__PURE__ */ jsxRuntimeExports.jsx("header", { className: "bg-card border-b border-border/60 sticky top-0 z-40 backdrop-blur-sm supports-[backdrop-filter]:bg-card/80", children: /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "max-w-screen-xl mx-auto px-4 sm:px-6 h-14 flex items-center justify-between gap-3", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-2.5 min-w-0", children: [
      /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "w-8 h-8 rounded-lg bg-primary/15 border border-primary/30 flex items-center justify-center shrink-0", children: /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-[13px] font-display font-bold text-primary", children: "₿" }) }),
      /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "min-w-0", children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "font-display font-bold text-[17px] tracking-tight text-foreground truncate block", children: "CryptoMarket" }),
        /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-[10px] text-muted-foreground uppercase tracking-wider hidden sm:block", children: "Live · On-Chain · ICP" })
      ] })
    ] }),
    /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-2 shrink-0", children: [
      isLoading ? /* @__PURE__ */ jsxRuntimeExports.jsxs(
        Badge,
        {
          variant: "outline",
          className: "gap-1.5 text-[11px] border-muted-foreground/30 text-muted-foreground",
          "data-ocid": "header.loading_state",
          children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "w-1.5 h-1.5 rounded-full bg-muted-foreground animate-pulse" }),
            "Lädt..."
          ]
        }
      ) : isLive ? /* @__PURE__ */ jsxRuntimeExports.jsxs(
        Badge,
        {
          variant: "outline",
          className: "gap-1.5 text-[11px] border-price-up/40 text-price-up",
          "data-ocid": "header.live_badge",
          children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(Activity, { className: "w-3 h-3" }),
            "LIVE"
          ]
        }
      ) : null,
      timeStr && !isLoading && /* @__PURE__ */ jsxRuntimeExports.jsx(
        "span",
        {
          className: "text-[11px] text-muted-foreground font-mono hidden sm:block",
          "data-ocid": "header.last_updated",
          children: timeStr
        }
      )
    ] })
  ] }) });
}
function Layout({
  children,
  lastUpdated,
  isLive,
  isLoading
}) {
  return /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "min-h-screen bg-background flex flex-col", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsx(Header, { lastUpdated, isLive, isLoading }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("main", { className: "flex-1 w-full max-w-screen-xl mx-auto px-4 sm:px-6 py-4 sm:py-6", children }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("footer", { className: "bg-card border-t border-border/60 py-4", children: /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "max-w-screen-xl mx-auto px-4 sm:px-6 flex flex-col sm:flex-row items-center justify-between gap-2", children: [
      /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "text-xs text-muted-foreground", children: [
        "© ",
        (/* @__PURE__ */ new Date()).getFullYear(),
        ". Built with love using",
        " ",
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          "a",
          {
            href: `https://caffeine.ai?utm_source=caffeine-footer&utm_medium=referral&utm_content=${encodeURIComponent(typeof window !== "undefined" ? window.location.hostname : "")}`,
            target: "_blank",
            rel: "noopener noreferrer",
            className: "text-primary hover:underline transition-colors",
            children: "caffeine.ai"
          }
        )
      ] }),
      /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-xs text-muted-foreground", children: "Preise via CoinGecko • Kein Finanzberater" })
    ] }) })
  ] });
}
function pctFor$2(c2, tf) {
  if (tf === "1h") return c2.priceChangePercentage1h;
  if (tf === "7d") return c2.priceChangePercentage7d;
  return c2.priceChangePercentage24h;
}
const MoverCard = reactExports.memo(function MoverCard2({
  coin,
  pct,
  onSelect
}) {
  const positive = pct >= 0;
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    "button",
    {
      type: "button",
      onClick: () => onSelect == null ? void 0 : onSelect(coin),
      className: "flex items-center gap-2.5 px-3 py-2.5 rounded-lg border border-border/50 bg-card/60 hover:bg-card hover:border-border transition-colors text-left min-w-[180px] shrink-0 cursor-pointer",
      "data-ocid": `topMovers.item.${coin.id}`,
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          "img",
          {
            src: coin.image,
            alt: coin.name,
            className: "w-7 h-7 rounded-full shrink-0",
            loading: "lazy",
            onError: (e2) => {
              e2.target.style.visibility = "hidden";
            }
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "min-w-0 flex-1", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-xs font-display font-semibold text-foreground truncate leading-tight", children: coin.symbol }),
          /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-[10px] text-muted-foreground font-mono tabular-nums", children: formatPrice(coin.currentPrice) })
        ] }),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          "span",
          {
            className: `text-xs font-semibold tabular-nums shrink-0 ${positive ? "text-price-up" : "text-price-down"}`,
            children: formatPercent(pct)
          }
        )
      ]
    }
  );
});
function Row({ label, icon, items, color, onSelect, isLoading }) {
  return /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex flex-col gap-2", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-1.5 px-1", children: [
      /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: color, children: icon }),
      /* @__PURE__ */ jsxRuntimeExports.jsx("h3", { className: "text-[11px] font-semibold uppercase tracking-wider text-muted-foreground", children: label })
    ] }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex gap-2 overflow-x-auto pb-1 -mx-1 px-1 scrollbar-thin snap-x", children: isLoading ? ["a", "b", "c", "d", "e"].map((k2) => /* @__PURE__ */ jsxRuntimeExports.jsx(
      Skeleton,
      {
        className: "h-[52px] w-[180px] rounded-lg shrink-0"
      },
      k2
    )) : items.map(({ coin, pct }) => /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "snap-start", children: /* @__PURE__ */ jsxRuntimeExports.jsx(MoverCard, { coin, pct, onSelect }) }, coin.id)) })
  ] });
}
function TopMovers({
  coins,
  isLoading,
  timeframe,
  onSelect
}) {
  const { gainers, losers } = reactExports.useMemo(() => {
    if (!coins || coins.length === 0)
      return { gainers: [], losers: [] };
    const withPct = coins.map((c2) => ({ coin: c2, pct: pctFor$2(c2, timeframe) }));
    const valid = withPct.filter((x2) => Number.isFinite(x2.pct) && x2.pct !== 0);
    const sorted = [...valid].sort((a2, b2) => b2.pct - a2.pct);
    return {
      gainers: sorted.slice(0, 5),
      losers: sorted.slice(-5).reverse()
    };
  }, [coins, timeframe]);
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    "div",
    {
      className: "grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4 sm:mb-6",
      "data-ocid": "topMovers.container",
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          Row,
          {
            label: `Top Gewinner (${timeframe})`,
            icon: /* @__PURE__ */ jsxRuntimeExports.jsx(TrendingUp, { className: "w-3.5 h-3.5" }),
            items: gainers,
            color: "text-price-up",
            onSelect,
            isLoading
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          Row,
          {
            label: `Top Verlierer (${timeframe})`,
            icon: /* @__PURE__ */ jsxRuntimeExports.jsx(TrendingDown, { className: "w-3.5 h-3.5" }),
            items: losers,
            color: "text-price-down",
            onSelect,
            isLoading
          }
        )
      ]
    }
  );
}
function SparklineImpl({
  data,
  positive,
  width = 88,
  height = 32,
  strokeWidth = 1.5,
  showFill = true
}) {
  const gradientId = reactExports.useId();
  if (!data || data.length < 2) {
    return /* @__PURE__ */ jsxRuntimeExports.jsx(
      "svg",
      {
        width,
        height,
        viewBox: `0 0 ${width} ${height}`,
        role: "img",
        "aria-label": "Keine Sparkline-Daten",
        children: /* @__PURE__ */ jsxRuntimeExports.jsx(
          "line",
          {
            x1: 0,
            y1: height / 2,
            x2: width,
            y2: height / 2,
            stroke: "oklch(var(--muted-foreground) / 0.4)",
            strokeWidth: "1",
            strokeDasharray: "2 2"
          }
        )
      }
    );
  }
  let min = data[0];
  let max = data[0];
  for (const v2 of data) {
    if (v2 < min) min = v2;
    if (v2 > max) max = v2;
  }
  const range = max - min || 1;
  let line = "";
  let area = "";
  for (let i = 0; i < data.length; i++) {
    const x2 = i / (data.length - 1) * width;
    const y2 = height - (data[i] - min) / range * (height - 2) - 1;
    line += i === 0 ? `M${x2.toFixed(2)},${y2.toFixed(2)}` : ` L${x2.toFixed(2)},${y2.toFixed(2)}`;
  }
  area = `${line} L${width},${height} L0,${height} Z`;
  const stroke = positive ? "oklch(var(--price-up))" : "oklch(var(--price-down))";
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    "svg",
    {
      width,
      height,
      viewBox: `0 0 ${width} ${height}`,
      className: "overflow-visible",
      role: "img",
      "aria-label": positive ? "Aufwärtstrend" : "Abwärtstrend",
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx("defs", { children: /* @__PURE__ */ jsxRuntimeExports.jsxs("linearGradient", { id: gradientId, x1: "0", y1: "0", x2: "0", y2: "1", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx("stop", { offset: "0%", stopColor: stroke, stopOpacity: "0.35" }),
          /* @__PURE__ */ jsxRuntimeExports.jsx("stop", { offset: "100%", stopColor: stroke, stopOpacity: "0" })
        ] }) }),
        showFill && /* @__PURE__ */ jsxRuntimeExports.jsx("path", { d: area, fill: `url(#${gradientId})` }),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          "path",
          {
            d: line,
            fill: "none",
            stroke,
            strokeWidth,
            strokeLinecap: "round",
            strokeLinejoin: "round"
          }
        )
      ]
    }
  );
}
const Sparkline = reactExports.memo(SparklineImpl);
function pctFor$1(c2, tf) {
  if (tf === "1h") return c2.priceChangePercentage1h;
  if (tf === "7d") return c2.priceChangePercentage7d;
  return c2.priceChangePercentage24h;
}
function CoinRowImpl({
  coin,
  rank,
  timeframe,
  isFavorite,
  flash,
  onToggleFavorite,
  onSelect
}) {
  const pct = pctFor$1(coin, timeframe);
  const positive = pct >= 0;
  const sparkPositive = coin.priceChangePercentage7d >= 0;
  const handleClick = reactExports.useCallback(() => {
    onSelect(coin);
  }, [coin, onSelect]);
  const handleFavClick = reactExports.useCallback(
    (e2) => {
      e2.stopPropagation();
      onToggleFavorite(coin.id);
    },
    [coin.id, onToggleFavorite]
  );
  const handleKey = reactExports.useCallback(
    (e2) => {
      if (e2.key === "Enter" || e2.key === " ") {
        e2.preventDefault();
        onSelect(coin);
      }
    },
    [coin, onSelect]
  );
  const flashClass = flash === "up" ? "bg-price-up/[0.06]" : flash === "down" ? "bg-price-down/[0.06]" : "";
  return (
    // biome-ignore lint/a11y/useSemanticElements: row contains a nested favorite <button>; using a native <button> for the row would nest interactive elements. The div carries role+tabIndex+keydown so it is keyboard-accessible.
    /* @__PURE__ */ jsxRuntimeExports.jsxs(
      "div",
      {
        role: "button",
        tabIndex: 0,
        onClick: handleClick,
        onKeyDown: handleKey,
        "aria-label": `${coin.name} Details öffnen`,
        className: `grid grid-cols-[auto_auto_1fr_auto_auto] sm:grid-cols-[auto_auto_1fr_auto_auto_auto_auto] md:grid-cols-[auto_auto_1fr_auto_auto_auto_auto_auto] items-center gap-2 sm:gap-4 px-3 sm:px-4 py-3 sm:py-3.5 border-b border-border/40 hover:bg-card/60 focus:outline-none focus:ring-1 focus:ring-primary/40 focus-visible:ring-2 cursor-pointer transition-colors ${flashClass}`,
        "data-ocid": `market.item.${coin.id}`,
        children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            "button",
            {
              type: "button",
              onClick: handleFavClick,
              className: `w-6 h-6 flex items-center justify-center rounded transition-colors ${isFavorite ? "text-primary" : "text-muted-foreground/40 hover:text-foreground"}`,
              "aria-label": isFavorite ? "Aus Watchlist entfernen" : "Zur Watchlist hinzufügen",
              "data-ocid": `market.favorite.${coin.id}`,
              children: /* @__PURE__ */ jsxRuntimeExports.jsx(Star, { className: "w-3.5 h-3.5", fill: isFavorite ? "currentColor" : "none" })
            }
          ),
          /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-xs text-muted-foreground w-6 text-right tabular-nums shrink-0", children: rank }),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-2.5 min-w-0", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(
              "img",
              {
                src: coin.image,
                alt: coin.name,
                className: "w-7 h-7 rounded-full shrink-0",
                loading: "lazy",
                onError: (e2) => {
                  e2.target.style.visibility = "hidden";
                }
              }
            ),
            /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "min-w-0", children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-sm font-display font-semibold text-foreground truncate leading-tight", children: coin.name }),
              /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-[11px] text-muted-foreground uppercase tracking-wider", children: coin.symbol })
            ] })
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "hidden sm:block shrink-0", children: /* @__PURE__ */ jsxRuntimeExports.jsx(Sparkline, { data: coin.sparkline7d, positive: sparkPositive }) }),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "text-right shrink-0", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(
              "p",
              {
                className: "text-sm font-mono font-semibold text-foreground tabular-nums",
                "data-ocid": `market.price.${coin.id}`,
                children: formatPrice(coin.currentPrice)
              }
            ),
            /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "text-[10px] text-muted-foreground sm:hidden", children: [
              "MK ",
              formatCompactNumber(coin.marketCap),
              " €"
            ] })
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            "div",
            {
              className: `text-right shrink-0 px-2 py-0.5 rounded text-xs font-semibold tabular-nums ${positive ? "bg-price-up/10 text-price-up" : "bg-price-down/10 text-price-down"}`,
              "data-ocid": `market.change.${coin.id}`,
              children: formatPercent(pct)
            }
          ),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "hidden md:block text-right text-xs text-muted-foreground tabular-nums shrink-0 min-w-[88px]", children: [
            formatCompactNumber(coin.totalVolume),
            " €"
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "hidden sm:block text-right text-xs text-muted-foreground tabular-nums shrink-0 min-w-[88px]", children: [
            formatCompactNumber(coin.marketCap),
            " €"
          ] })
        ]
      }
    )
  );
}
const CoinRow = reactExports.memo(CoinRowImpl);
function createLazyMeasurementsView(count2, flat, getItemKey) {
  const cache = new Array(count2);
  return new Proxy(cache, {
    get(target, prop, receiver) {
      if (typeof prop === "string") {
        const c2 = prop.charCodeAt(0);
        if (c2 >= 48 && c2 <= 57) {
          const i = +prop;
          if (Number.isInteger(i) && i >= 0 && i < count2) {
            let v2 = target[i];
            if (!v2) {
              const s = flat[i * 2];
              v2 = target[i] = {
                index: i,
                key: getItemKey(i),
                start: s,
                size: flat[i * 2 + 1],
                end: s + flat[i * 2 + 1],
                lane: 0
              };
            }
            return v2;
          }
        }
        if (prop === "length") return count2;
      }
      return Reflect.get(target, prop, receiver);
    }
  });
}
function memo(getDeps, fn2, opts) {
  let deps = opts.initialDeps ?? [];
  let result;
  let isInitial = true;
  function memoizedFunction() {
    const newDeps = getDeps();
    const depsChanged = newDeps.length !== deps.length || newDeps.some((dep, index) => deps[index] !== dep);
    if (!depsChanged) {
      return result;
    }
    deps = newDeps;
    result = fn2(...newDeps);
    if ((opts == null ? void 0 : opts.onChange) && !(isInitial && opts.skipInitialOnChange)) {
      opts.onChange(result);
    }
    isInitial = false;
    return result;
  }
  memoizedFunction.updateDeps = (newDeps) => {
    deps = newDeps;
  };
  return memoizedFunction;
}
function notUndefined(value, msg) {
  if (value === void 0) {
    throw new Error(`Unexpected undefined${""}`);
  } else {
    return value;
  }
}
const approxEqual = (a2, b2) => Math.abs(a2 - b2) < 1.01;
const debounce = (targetWindow, fn2, ms2) => {
  let timeoutId;
  return function(...args) {
    targetWindow.clearTimeout(timeoutId);
    timeoutId = targetWindow.setTimeout(() => fn2.apply(this, args), ms2);
  };
};
let _isIOSResult;
const isIOSWebKit = () => {
  if (_isIOSResult !== void 0) return _isIOSResult;
  if (typeof navigator === "undefined") return _isIOSResult = false;
  if (/iP(hone|od|ad)/.test(navigator.userAgent)) return _isIOSResult = true;
  const mtp = navigator.maxTouchPoints;
  return _isIOSResult = navigator.platform === "MacIntel" && mtp !== void 0 && mtp > 0;
};
const getRect = (element) => {
  const { offsetWidth, offsetHeight } = element;
  return { width: offsetWidth, height: offsetHeight };
};
const defaultKeyExtractor = (index) => index;
const defaultRangeExtractor = (range) => {
  const start = Math.max(range.startIndex - range.overscan, 0);
  const end = Math.min(range.endIndex + range.overscan, range.count - 1);
  const len = end - start + 1;
  const arr = new Array(len);
  for (let i = 0; i < len; i++) {
    arr[i] = start + i;
  }
  return arr;
};
const observeElementRect = (instance, cb) => {
  const element = instance.scrollElement;
  if (!element) {
    return;
  }
  const targetWindow = instance.targetWindow;
  if (!targetWindow) {
    return;
  }
  const handler = (rect) => {
    const { width, height } = rect;
    cb({ width: Math.round(width), height: Math.round(height) });
  };
  handler(getRect(element));
  if (!targetWindow.ResizeObserver) {
    return () => {
    };
  }
  const observer = new targetWindow.ResizeObserver((entries) => {
    const run = () => {
      const entry = entries[0];
      if (entry == null ? void 0 : entry.borderBoxSize) {
        const box = entry.borderBoxSize[0];
        if (box) {
          handler({ width: box.inlineSize, height: box.blockSize });
          return;
        }
      }
      handler(getRect(element));
    };
    instance.options.useAnimationFrameWithResizeObserver ? requestAnimationFrame(run) : run();
  });
  observer.observe(element, { box: "border-box" });
  return () => {
    observer.unobserve(element);
  };
};
const addEventListenerOptions = {
  passive: true
};
const supportsScrollend = typeof window == "undefined" ? true : "onscrollend" in window;
const observeOffset = (instance, cb, readOffset) => {
  const element = instance.scrollElement;
  if (!element) {
    return;
  }
  const targetWindow = instance.targetWindow;
  if (!targetWindow) {
    return;
  }
  const registerScrollendEvent = instance.options.useScrollendEvent && supportsScrollend;
  let offset = 0;
  const fallback = registerScrollendEvent ? null : debounce(
    targetWindow,
    () => cb(offset, false),
    instance.options.isScrollingResetDelay
  );
  const createHandler = (isScrolling) => () => {
    offset = readOffset(element);
    fallback == null ? void 0 : fallback();
    cb(offset, isScrolling);
  };
  const handler = createHandler(true);
  const endHandler = createHandler(false);
  element.addEventListener("scroll", handler, addEventListenerOptions);
  if (registerScrollendEvent) {
    element.addEventListener("scrollend", endHandler, addEventListenerOptions);
  }
  return () => {
    element.removeEventListener("scroll", handler);
    if (registerScrollendEvent) {
      element.removeEventListener("scrollend", endHandler);
    }
  };
};
const observeElementOffset = (instance, cb) => observeOffset(instance, cb, (el) => {
  const { horizontal, isRtl } = instance.options;
  return horizontal ? el.scrollLeft * (isRtl && -1 || 1) : el.scrollTop;
});
const measureElement = (element, entry, instance) => {
  if (entry == null ? void 0 : entry.borderBoxSize) {
    const box = entry.borderBoxSize[0];
    if (box) {
      const size2 = Math.round(
        box[instance.options.horizontal ? "inlineSize" : "blockSize"]
      );
      return size2;
    }
  }
  return element[instance.options.horizontal ? "offsetWidth" : "offsetHeight"];
};
const scrollWithAdjustments = (offset, {
  adjustments = 0,
  behavior
}, instance) => {
  var _a2, _b;
  (_b = (_a2 = instance.scrollElement) == null ? void 0 : _a2.scrollTo) == null ? void 0 : _b.call(_a2, {
    [instance.options.horizontal ? "left" : "top"]: offset + adjustments,
    behavior
  });
};
const elementScroll = scrollWithAdjustments;
class Virtualizer {
  constructor(opts) {
    this.unsubs = [];
    this.scrollElement = null;
    this.targetWindow = null;
    this.isScrolling = false;
    this.scrollState = null;
    this.measurementsCache = [];
    this._flatMeasurements = null;
    this.itemSizeCache = /* @__PURE__ */ new Map();
    this.itemSizeCacheVersion = 0;
    this.laneAssignments = /* @__PURE__ */ new Map();
    this.pendingMin = null;
    this.prevLanes = void 0;
    this.lanesChangedFlag = false;
    this.lanesSettling = false;
    this.pendingScrollAnchor = null;
    this.scrollRect = null;
    this.scrollOffset = null;
    this.scrollDirection = null;
    this.scrollAdjustments = 0;
    this._iosDeferredAdjustment = 0;
    this._iosTouching = false;
    this._iosJustTouchEnded = false;
    this._iosTouchEndTimerId = null;
    this._intendedScrollOffset = null;
    this.elementsCache = /* @__PURE__ */ new Map();
    this.now = () => {
      var _a2, _b, _c;
      return ((_c = (_b = (_a2 = this.targetWindow) == null ? void 0 : _a2.performance) == null ? void 0 : _b.now) == null ? void 0 : _c.call(_b)) ?? Date.now();
    };
    this.observer = /* @__PURE__ */ (() => {
      let _ro = null;
      const get = () => {
        if (_ro) {
          return _ro;
        }
        if (!this.targetWindow || !this.targetWindow.ResizeObserver) {
          return null;
        }
        return _ro = new this.targetWindow.ResizeObserver((entries) => {
          entries.forEach((entry) => {
            const run = () => {
              const node = entry.target;
              const index = this.indexFromElement(node);
              if (!node.isConnected) {
                this.observer.unobserve(node);
                for (const [cacheKey, cachedNode] of this.elementsCache) {
                  if (cachedNode === node) {
                    this.elementsCache.delete(cacheKey);
                    break;
                  }
                }
                return;
              }
              if (this.shouldMeasureDuringScroll(index)) {
                this.resizeItem(
                  index,
                  this.options.measureElement(node, entry, this)
                );
              }
            };
            this.options.useAnimationFrameWithResizeObserver ? requestAnimationFrame(run) : run();
          });
        });
      };
      return {
        disconnect: () => {
          var _a2;
          (_a2 = get()) == null ? void 0 : _a2.disconnect();
          _ro = null;
        },
        observe: (target) => {
          var _a2;
          return (_a2 = get()) == null ? void 0 : _a2.observe(target, { box: "border-box" });
        },
        unobserve: (target) => {
          var _a2;
          return (_a2 = get()) == null ? void 0 : _a2.unobserve(target);
        }
      };
    })();
    this.range = null;
    this.setOptions = (opts2) => {
      var _a2, _b;
      const merged = {
        debug: false,
        initialOffset: 0,
        overscan: 1,
        paddingStart: 0,
        paddingEnd: 0,
        scrollPaddingStart: 0,
        scrollPaddingEnd: 0,
        horizontal: false,
        getItemKey: defaultKeyExtractor,
        rangeExtractor: defaultRangeExtractor,
        onChange: () => {
        },
        measureElement,
        initialRect: { width: 0, height: 0 },
        scrollMargin: 0,
        gap: 0,
        indexAttribute: "data-index",
        initialMeasurementsCache: [],
        lanes: 1,
        anchorTo: "start",
        followOnAppend: false,
        scrollEndThreshold: 1,
        isScrollingResetDelay: 150,
        enabled: true,
        isRtl: false,
        useScrollendEvent: false,
        useAnimationFrameWithResizeObserver: false,
        laneAssignmentMode: "estimate"
      };
      for (const key in opts2) {
        const v2 = opts2[key];
        if (v2 !== void 0) merged[key] = v2;
      }
      const prevOptions = this.options;
      let anchor = null;
      let followOnAppend = null;
      if (prevOptions !== void 0 && prevOptions.enabled && merged.enabled && merged.anchorTo === "end" && this.scrollElement !== null) {
        const prevCount = prevOptions.count;
        const nextCount = merged.count;
        const measurements = this.getMeasurements();
        const prevFirstKey = prevCount > 0 ? ((_a2 = measurements[0]) == null ? void 0 : _a2.key) ?? prevOptions.getItemKey(0) : null;
        const prevLastKey = prevCount > 0 ? ((_b = measurements[prevCount - 1]) == null ? void 0 : _b.key) ?? prevOptions.getItemKey(prevCount - 1) : null;
        const didCountChange = nextCount !== prevCount;
        const didEdgeKeysChange = didCountChange || prevCount > 0 && nextCount > 0 && (merged.getItemKey(0) !== prevFirstKey || merged.getItemKey(nextCount - 1) !== prevLastKey);
        if (didEdgeKeysChange) {
          const item = prevCount > 0 ? this.getVirtualItemForOffset(this.getScrollOffset()) ?? measurements[0] : null;
          if (item) {
            anchor = [item.key, this.getScrollOffset() - item.start];
          }
          const behavior = merged.followOnAppend === true ? "auto" : merged.followOnAppend || null;
          if (behavior && nextCount > prevCount && this.isAtEnd(prevOptions.scrollEndThreshold) && (prevCount === 0 || merged.getItemKey(nextCount - 1) !== prevLastKey)) {
            followOnAppend = behavior;
          }
        }
      }
      this.options = merged;
      if (anchor || followOnAppend) {
        this.pendingScrollAnchor = [
          (anchor == null ? void 0 : anchor[0]) ?? null,
          (anchor == null ? void 0 : anchor[1]) ?? 0,
          followOnAppend
        ];
      }
    };
    this.notify = (sync) => {
      var _a2, _b;
      (_b = (_a2 = this.options).onChange) == null ? void 0 : _b.call(_a2, this, sync);
    };
    this.maybeNotify = memo(
      () => {
        this.calculateRange();
        return [
          this.isScrolling,
          this.range ? this.range.startIndex : null,
          this.range ? this.range.endIndex : null
        ];
      },
      (isScrolling) => {
        this.notify(isScrolling);
      },
      {
        key: false,
        debug: () => this.options.debug,
        initialDeps: [
          this.isScrolling,
          this.range ? this.range.startIndex : null,
          this.range ? this.range.endIndex : null
        ]
      }
    );
    this.cleanup = () => {
      this.unsubs.filter(Boolean).forEach((d2) => d2());
      this.unsubs = [];
      this.observer.disconnect();
      if (this.rafId != null && this.targetWindow) {
        this.targetWindow.cancelAnimationFrame(this.rafId);
        this.rafId = null;
      }
      this.scrollState = null;
      this.scrollElement = null;
      this.targetWindow = null;
    };
    this._didMount = () => {
      return () => {
        this.cleanup();
      };
    };
    this._willUpdate = () => {
      var _a2;
      const scrollElement = this.options.enabled ? this.options.getScrollElement() : null;
      if (this.scrollElement !== scrollElement) {
        this.cleanup();
        if (!scrollElement) {
          this.maybeNotify();
          return;
        }
        this.scrollElement = scrollElement;
        if (this.scrollElement && "ownerDocument" in this.scrollElement) {
          this.targetWindow = this.scrollElement.ownerDocument.defaultView;
        } else {
          this.targetWindow = ((_a2 = this.scrollElement) == null ? void 0 : _a2.window) ?? null;
        }
        this.elementsCache.forEach((cached) => {
          this.observer.observe(cached);
        });
        this.unsubs.push(
          this.options.observeElementRect(this, (rect) => {
            this.scrollRect = rect;
            this.maybeNotify();
          })
        );
        this.unsubs.push(
          this.options.observeElementOffset(this, (offset, isScrolling) => {
            if (this._intendedScrollOffset !== null && Math.abs(offset - this._intendedScrollOffset) < 1.5) {
              offset = this._intendedScrollOffset;
            }
            this._intendedScrollOffset = null;
            this.scrollAdjustments = 0;
            this.scrollDirection = isScrolling ? this.getScrollOffset() < offset ? "forward" : "backward" : null;
            this.scrollOffset = offset;
            this.isScrolling = isScrolling;
            this._flushIosDeferredIfReady();
            if (this.scrollState) {
              this.scheduleScrollReconcile();
            }
            this.maybeNotify();
          })
        );
        if ("addEventListener" in this.scrollElement) {
          const scrollEl = this.scrollElement;
          const onTouchStart = () => {
            this._iosTouching = true;
            this._iosJustTouchEnded = false;
            if (this._iosTouchEndTimerId !== null && this.targetWindow != null) {
              this.targetWindow.clearTimeout(this._iosTouchEndTimerId);
              this._iosTouchEndTimerId = null;
            }
          };
          const onTouchEnd = () => {
            this._iosTouching = false;
            if (!isIOSWebKit() || this.targetWindow == null) {
              return;
            }
            this._iosJustTouchEnded = true;
            this._iosTouchEndTimerId = this.targetWindow.setTimeout(() => {
              this._iosJustTouchEnded = false;
              this._iosTouchEndTimerId = null;
              this._flushIosDeferredIfReady();
            }, 150);
          };
          scrollEl.addEventListener(
            "touchstart",
            onTouchStart,
            addEventListenerOptions
          );
          scrollEl.addEventListener(
            "touchend",
            onTouchEnd,
            addEventListenerOptions
          );
          this.unsubs.push(() => {
            scrollEl.removeEventListener("touchstart", onTouchStart);
            scrollEl.removeEventListener("touchend", onTouchEnd);
            if (this._iosTouchEndTimerId !== null && this.targetWindow != null) {
              this.targetWindow.clearTimeout(this._iosTouchEndTimerId);
              this._iosTouchEndTimerId = null;
            }
          });
        }
        this._scrollToOffset(this.getScrollOffset(), {
          adjustments: void 0,
          behavior: void 0
        });
      }
      const anchor = this.pendingScrollAnchor;
      this.pendingScrollAnchor = null;
      if (anchor && this.scrollElement && this.options.enabled) {
        const [key, offset, followOnAppend] = anchor;
        if (key !== null) {
          const { count: count2, getItemKey } = this.options;
          let index = 0;
          while (index < count2 && getItemKey(index) !== key) {
            index++;
          }
          const item = index < count2 ? this.getMeasurements()[index] : void 0;
          if (item) {
            const delta = item.start + offset - this.getScrollOffset();
            if (!approxEqual(delta, 0)) {
              this.applyScrollAdjustment(delta);
            }
          }
        }
        if (followOnAppend) {
          this.scrollToEnd({ behavior: followOnAppend });
        }
      }
    };
    this._flushIosDeferredIfReady = () => {
      if (this._iosDeferredAdjustment === 0) return;
      if (this.isScrolling) return;
      if (this._iosTouching) return;
      if (this._iosJustTouchEnded) return;
      const cur = this.getScrollOffset();
      const max = this.getMaxScrollOffset();
      if (cur < 0 || cur > max) return;
      const delta = this._iosDeferredAdjustment;
      this._iosDeferredAdjustment = 0;
      this._scrollToOffset(cur, {
        adjustments: this.scrollAdjustments += delta,
        behavior: void 0
      });
    };
    this.rafId = null;
    this.getSize = () => {
      if (!this.options.enabled) {
        this.scrollRect = null;
        return 0;
      }
      this.scrollRect = this.scrollRect ?? this.options.initialRect;
      return this.scrollRect[this.options.horizontal ? "width" : "height"];
    };
    this.getScrollOffset = () => {
      if (!this.options.enabled) {
        this.scrollOffset = null;
        return 0;
      }
      this.scrollOffset = this.scrollOffset ?? (typeof this.options.initialOffset === "function" ? this.options.initialOffset() : this.options.initialOffset);
      return this.scrollOffset;
    };
    this.getFurthestMeasurement = (measurements, index) => {
      const furthestMeasurementsFound = /* @__PURE__ */ new Map();
      const furthestMeasurements = /* @__PURE__ */ new Map();
      for (let m2 = index - 1; m2 >= 0; m2--) {
        const measurement = measurements[m2];
        if (furthestMeasurementsFound.has(measurement.lane)) {
          continue;
        }
        const previousFurthestMeasurement = furthestMeasurements.get(
          measurement.lane
        );
        if (previousFurthestMeasurement == null || measurement.end > previousFurthestMeasurement.end) {
          furthestMeasurements.set(measurement.lane, measurement);
        } else if (measurement.end < previousFurthestMeasurement.end) {
          furthestMeasurementsFound.set(measurement.lane, true);
        }
        if (furthestMeasurementsFound.size === this.options.lanes) {
          break;
        }
      }
      return furthestMeasurements.size === this.options.lanes ? Array.from(furthestMeasurements.values()).sort((a2, b2) => {
        if (a2.end === b2.end) {
          return a2.index - b2.index;
        }
        return a2.end - b2.end;
      })[0] : void 0;
    };
    this.getMeasurementOptions = memo(
      () => [
        this.options.count,
        this.options.paddingStart,
        this.options.scrollMargin,
        this.options.getItemKey,
        this.options.enabled,
        this.options.lanes,
        this.options.laneAssignmentMode
      ],
      (count2, paddingStart, scrollMargin, getItemKey, enabled, lanes, laneAssignmentMode) => {
        const lanesChanged = this.prevLanes !== void 0 && this.prevLanes !== lanes;
        if (lanesChanged) {
          this.lanesChangedFlag = true;
        }
        this.prevLanes = lanes;
        this.pendingMin = null;
        return {
          count: count2,
          paddingStart,
          scrollMargin,
          getItemKey,
          enabled,
          lanes,
          laneAssignmentMode
        };
      },
      {
        key: false
      }
    );
    this.getMeasurements = memo(
      () => [this.getMeasurementOptions(), this.itemSizeCacheVersion],
      ({
        count: count2,
        paddingStart,
        scrollMargin,
        getItemKey,
        enabled,
        lanes,
        laneAssignmentMode
      }, _itemSizeCacheVersion) => {
        const itemSizeCache = this.itemSizeCache;
        if (!enabled) {
          this.measurementsCache = [];
          this.itemSizeCache.clear();
          this.laneAssignments.clear();
          return [];
        }
        if (this.laneAssignments.size > count2) {
          for (const index of this.laneAssignments.keys()) {
            if (index >= count2) {
              this.laneAssignments.delete(index);
            }
          }
        }
        if (this.lanesChangedFlag) {
          this.lanesChangedFlag = false;
          this.lanesSettling = true;
          this.measurementsCache = [];
          this.itemSizeCache.clear();
          this.laneAssignments.clear();
          this.pendingMin = null;
        }
        if (this.measurementsCache.length === 0 && !this.lanesSettling) {
          this.measurementsCache = this.options.initialMeasurementsCache;
          this.measurementsCache.forEach((item) => {
            this.itemSizeCache.set(item.key, item.size);
          });
        }
        const min = this.lanesSettling ? 0 : this.pendingMin ?? 0;
        this.pendingMin = null;
        if (this.lanesSettling && this.measurementsCache.length === count2) {
          this.lanesSettling = false;
        }
        if (lanes === 1) {
          const gap = this.options.gap;
          const need = count2 * 2;
          let flat = this._flatMeasurements;
          if (!flat || flat.length < need) {
            const next = new Float64Array(need);
            if (flat && min > 0) next.set(flat.subarray(0, min * 2));
            flat = next;
            this._flatMeasurements = flat;
          }
          let runningStart;
          if (min === 0) {
            runningStart = paddingStart + scrollMargin;
          } else {
            const prevIdx = min - 1;
            runningStart = flat[prevIdx * 2] + flat[prevIdx * 2 + 1] + gap;
          }
          for (let i = min; i < count2; i++) {
            const key = getItemKey(i);
            const measuredSize = itemSizeCache.get(key);
            const size2 = typeof measuredSize === "number" ? measuredSize : this.options.estimateSize(i);
            flat[i * 2] = runningStart;
            flat[i * 2 + 1] = size2;
            runningStart += size2 + gap;
          }
          const view = createLazyMeasurementsView(count2, flat, getItemKey);
          this.measurementsCache = view;
          return view;
        }
        const measurements = this.measurementsCache.slice(0, min);
        const laneLastIndex = new Array(lanes).fill(
          void 0
        );
        for (let m2 = 0; m2 < min; m2++) {
          const item = measurements[m2];
          if (item) {
            laneLastIndex[item.lane] = m2;
          }
        }
        for (let i = min; i < count2; i++) {
          const key = getItemKey(i);
          const cachedLane = this.laneAssignments.get(i);
          let lane;
          let start;
          const shouldCacheLane = laneAssignmentMode === "estimate" || itemSizeCache.has(key);
          if (cachedLane !== void 0 && this.options.lanes > 1) {
            lane = cachedLane;
            const prevIndex = laneLastIndex[lane];
            const prevInLane = prevIndex !== void 0 ? measurements[prevIndex] : void 0;
            start = prevInLane ? prevInLane.end + this.options.gap : paddingStart + scrollMargin;
          } else {
            const furthestMeasurement = this.options.lanes === 1 ? measurements[i - 1] : this.getFurthestMeasurement(measurements, i);
            start = furthestMeasurement ? furthestMeasurement.end + this.options.gap : paddingStart + scrollMargin;
            lane = furthestMeasurement ? furthestMeasurement.lane : i % this.options.lanes;
            if (this.options.lanes > 1 && shouldCacheLane) {
              this.laneAssignments.set(i, lane);
            }
          }
          const measuredSize = itemSizeCache.get(key);
          const size2 = typeof measuredSize === "number" ? measuredSize : this.options.estimateSize(i);
          const end = start + size2;
          measurements[i] = {
            index: i,
            start,
            size: size2,
            end,
            key,
            lane
          };
          laneLastIndex[lane] = i;
        }
        this.measurementsCache = measurements;
        return measurements;
      },
      {
        key: false,
        debug: () => this.options.debug
      }
    );
    this.calculateRange = memo(
      () => [
        this.getMeasurements(),
        this.getSize(),
        this.getScrollOffset(),
        this.options.lanes
      ],
      (measurements, outerSize, scrollOffset, lanes) => {
        return this.range = measurements.length > 0 && outerSize > 0 ? calculateRange({
          measurements,
          outerSize,
          scrollOffset,
          lanes,
          // Pass the typed array so binary search + forward-walk can
          // read start/end directly from Float64Array, skipping the
          // Proxy traps that materialize a full VirtualItem per probe.
          flat: lanes === 1 && this._flatMeasurements != null ? this._flatMeasurements : null
        }) : null;
      },
      {
        key: false,
        debug: () => this.options.debug
      }
    );
    this.getVirtualIndexes = memo(
      () => {
        let startIndex = null;
        let endIndex = null;
        const range = this.calculateRange();
        if (range) {
          startIndex = range.startIndex;
          endIndex = range.endIndex;
        }
        this.maybeNotify.updateDeps([this.isScrolling, startIndex, endIndex]);
        return [
          this.options.rangeExtractor,
          this.options.overscan,
          this.options.count,
          startIndex,
          endIndex
        ];
      },
      (rangeExtractor, overscan, count2, startIndex, endIndex) => {
        return startIndex === null || endIndex === null ? [] : rangeExtractor({
          startIndex,
          endIndex,
          overscan,
          count: count2
        });
      },
      {
        key: false,
        debug: () => this.options.debug
      }
    );
    this.indexFromElement = (node) => {
      const attributeName = this.options.indexAttribute;
      const indexStr = node.getAttribute(attributeName);
      if (!indexStr) {
        console.warn(
          `Missing attribute name '${attributeName}={index}' on measured element.`
        );
        return -1;
      }
      return parseInt(indexStr, 10);
    };
    this.shouldMeasureDuringScroll = (index) => {
      var _a2;
      if (!this.scrollState || this.scrollState.behavior !== "smooth") {
        return true;
      }
      const scrollIndex = this.scrollState.index ?? ((_a2 = this.getVirtualItemForOffset(this.scrollState.lastTargetOffset)) == null ? void 0 : _a2.index);
      if (scrollIndex !== void 0 && this.range) {
        const bufferSize = Math.max(
          this.options.overscan,
          Math.ceil((this.range.endIndex - this.range.startIndex) / 2)
        );
        const minIndex = Math.max(0, scrollIndex - bufferSize);
        const maxIndex = Math.min(
          this.options.count - 1,
          scrollIndex + bufferSize
        );
        return index >= minIndex && index <= maxIndex;
      }
      return true;
    };
    this.measureElement = (node) => {
      if (!node) {
        this.elementsCache.forEach((cached, key2) => {
          if (!cached.isConnected) {
            this.observer.unobserve(cached);
            this.elementsCache.delete(key2);
          }
        });
        return;
      }
      const index = this.indexFromElement(node);
      const key = this.options.getItemKey(index);
      const prevNode = this.elementsCache.get(key);
      if (prevNode !== node) {
        if (prevNode) {
          this.observer.unobserve(prevNode);
        }
        this.observer.observe(node);
        this.elementsCache.set(key, node);
      }
      if ((!this.isScrolling || this.scrollState) && this.shouldMeasureDuringScroll(index)) {
        this.resizeItem(index, this.options.measureElement(node, void 0, this));
      }
    };
    this.resizeItem = (index, size2) => {
      var _a2, _b;
      if (index < 0 || index >= this.options.count) return;
      let cachedSize;
      let itemStart;
      let key;
      const flat = this._flatMeasurements;
      if (this.options.lanes === 1 && flat !== null) {
        key = this.options.getItemKey(index);
        itemStart = flat[index * 2];
        cachedSize = flat[index * 2 + 1];
      } else {
        const item = this.measurementsCache[index];
        if (!item) return;
        key = item.key;
        itemStart = item.start;
        cachedSize = item.size;
      }
      const itemSize = this.itemSizeCache.get(key) ?? cachedSize;
      const delta = size2 - itemSize;
      if (delta !== 0) {
        const wasAtEnd = this.options.anchorTo === "end" && ((_a2 = this.scrollState) == null ? void 0 : _a2.behavior) !== "smooth" && this.getVirtualDistanceFromEnd() <= this.options.scrollEndThreshold;
        const prevTotalSize = wasAtEnd ? this.getTotalSize() : 0;
        const shouldAdjustScroll = ((_b = this.scrollState) == null ? void 0 : _b.behavior) !== "smooth" && (this.shouldAdjustScrollPositionOnItemSizeChange !== void 0 ? this.shouldAdjustScrollPositionOnItemSizeChange(
          // The callback expects a VirtualItem; build one lazily only
          // when the consumer actually supplied a custom predicate.
          this.measurementsCache[index] ?? {
            index,
            key,
            start: itemStart,
            size: cachedSize,
            end: itemStart + cachedSize,
            lane: 0
          },
          delta,
          this
        ) : (
          // Default: adjust scrollTop only when the resize is an above-
          // viewport item AND we're not actively scrolling backward.
          // Adjusting during backward scroll fights the user's scroll
          // direction and produces the "items jump while scrolling up"
          // jank reported across many issues. Users who want the old
          // behavior can pass shouldAdjustScrollPositionOnItemSizeChange.
          itemStart < this.getScrollOffset() + this.scrollAdjustments && this.scrollDirection !== "backward"
        ));
        if (this.pendingMin === null || index < this.pendingMin) {
          this.pendingMin = index;
        }
        this.itemSizeCache.set(key, size2);
        this.itemSizeCacheVersion++;
        if (wasAtEnd) {
          this.applyScrollAdjustment(this.getTotalSize() - prevTotalSize);
        } else if (shouldAdjustScroll) {
          this.applyScrollAdjustment(delta);
        }
        this.notify(false);
      }
    };
    this.getVirtualItems = memo(
      () => [this.getVirtualIndexes(), this.getMeasurements()],
      (indexes, measurements) => {
        const virtualItems = [];
        for (let k2 = 0, len = indexes.length; k2 < len; k2++) {
          const i = indexes[k2];
          const measurement = measurements[i];
          virtualItems.push(measurement);
        }
        return virtualItems;
      },
      {
        key: false,
        debug: () => this.options.debug
      }
    );
    this.getVirtualItemForOffset = (offset) => {
      const measurements = this.getMeasurements();
      if (measurements.length === 0) {
        return void 0;
      }
      const flat = this._flatMeasurements;
      const useFlat = this.options.lanes === 1 && flat != null;
      const idx = findNearestBinarySearch(
        0,
        measurements.length - 1,
        useFlat ? (i) => flat[i * 2] : (i) => notUndefined(measurements[i]).start,
        offset
      );
      return notUndefined(measurements[idx]);
    };
    this.getMaxScrollOffset = () => {
      if (!this.scrollElement) return 0;
      if ("scrollHeight" in this.scrollElement) {
        return this.options.horizontal ? this.scrollElement.scrollWidth - this.scrollElement.clientWidth : this.scrollElement.scrollHeight - this.scrollElement.clientHeight;
      } else {
        const doc = this.scrollElement.document.documentElement;
        return this.options.horizontal ? doc.scrollWidth - this.scrollElement.innerWidth : doc.scrollHeight - this.scrollElement.innerHeight;
      }
    };
    this.getVirtualDistanceFromEnd = () => {
      return Math.max(
        this.getTotalSize() - this.getSize() - this.getScrollOffset(),
        0
      );
    };
    this.getDistanceFromEnd = () => {
      return Math.max(this.getMaxScrollOffset() - this.getScrollOffset(), 0);
    };
    this.isAtEnd = (threshold = this.options.scrollEndThreshold) => {
      return this.getDistanceFromEnd() <= threshold;
    };
    this.getOffsetForAlignment = (toOffset, align, itemSize = 0) => {
      if (!this.scrollElement) return 0;
      const size2 = this.getSize();
      const scrollOffset = this.getScrollOffset();
      if (align === "auto") {
        align = toOffset >= scrollOffset + size2 ? "end" : "start";
      }
      if (align === "center") {
        toOffset += (itemSize - size2) / 2;
      } else if (align === "end") {
        toOffset -= size2;
      }
      const maxOffset = this.getMaxScrollOffset();
      return Math.max(Math.min(maxOffset, toOffset), 0);
    };
    this.getOffsetForIndex = (index, align = "auto") => {
      index = Math.max(0, Math.min(index, this.options.count - 1));
      const size2 = this.getSize();
      const scrollOffset = this.getScrollOffset();
      const item = this.measurementsCache[index];
      if (!item) return;
      if (align === "auto") {
        if (item.end >= scrollOffset + size2 - this.options.scrollPaddingEnd) {
          align = "end";
        } else if (item.start <= scrollOffset + this.options.scrollPaddingStart) {
          align = "start";
        } else {
          return [scrollOffset, align];
        }
      }
      if (align === "end" && index === this.options.count - 1) {
        return [this.getMaxScrollOffset(), align];
      }
      const toOffset = align === "end" ? item.end + this.options.scrollPaddingEnd : item.start - this.options.scrollPaddingStart;
      return [
        this.getOffsetForAlignment(toOffset, align, item.size),
        align
      ];
    };
    this.scrollToOffset = (toOffset, { align = "start", behavior = "auto" } = {}) => {
      const offset = this.getOffsetForAlignment(toOffset, align);
      const now = this.now();
      this.scrollState = {
        index: null,
        align,
        behavior,
        startedAt: now,
        lastTargetOffset: offset,
        stableFrames: 0
      };
      this._scrollToOffset(offset, { adjustments: void 0, behavior });
      this.scheduleScrollReconcile();
    };
    this.scrollToIndex = (index, {
      align: initialAlign = "auto",
      behavior = "auto"
    } = {}) => {
      index = Math.max(0, Math.min(index, this.options.count - 1));
      const offsetInfo = this.getOffsetForIndex(index, initialAlign);
      if (!offsetInfo) {
        return;
      }
      const [offset, align] = offsetInfo;
      const now = this.now();
      this.scrollState = {
        index,
        align,
        behavior,
        startedAt: now,
        lastTargetOffset: offset,
        stableFrames: 0
      };
      this._scrollToOffset(offset, { adjustments: void 0, behavior });
      this.scheduleScrollReconcile();
    };
    this.scrollBy = (delta, { behavior = "auto" } = {}) => {
      const offset = this.getScrollOffset() + delta;
      const now = this.now();
      this.scrollState = {
        index: null,
        align: "start",
        behavior,
        startedAt: now,
        lastTargetOffset: offset,
        stableFrames: 0
      };
      this._scrollToOffset(offset, { adjustments: void 0, behavior });
      this.scheduleScrollReconcile();
    };
    this.scrollToEnd = ({ behavior = "auto" } = {}) => {
      if (this.options.count > 0) {
        this.scrollToIndex(this.options.count - 1, {
          align: "end",
          behavior
        });
        return;
      }
      this.scrollToOffset(Math.max(this.getTotalSize() - this.getSize(), 0), {
        behavior
      });
    };
    this.getTotalSize = () => {
      var _a2;
      const measurements = this.getMeasurements();
      let end;
      if (measurements.length === 0) {
        end = this.options.paddingStart;
      } else if (this.options.lanes === 1) {
        const lastIdx = measurements.length - 1;
        const flat = this._flatMeasurements;
        if (flat != null) {
          end = flat[lastIdx * 2] + flat[lastIdx * 2 + 1];
        } else {
          end = ((_a2 = measurements[lastIdx]) == null ? void 0 : _a2.end) ?? 0;
        }
      } else {
        const endByLane = Array(this.options.lanes).fill(null);
        let endIndex = measurements.length - 1;
        while (endIndex >= 0 && endByLane.some((val) => val === null)) {
          const item = measurements[endIndex];
          if (endByLane[item.lane] === null) {
            endByLane[item.lane] = item.end;
          }
          endIndex--;
        }
        end = Math.max(...endByLane.filter((val) => val !== null));
      }
      return Math.max(
        end - this.options.scrollMargin + this.options.paddingEnd,
        0
      );
    };
    this.takeSnapshot = () => {
      const snapshot = [];
      if (this.itemSizeCache.size === 0) return snapshot;
      const m2 = this.getMeasurements();
      for (const item of m2) {
        if (item && this.itemSizeCache.has(item.key)) {
          snapshot.push({
            index: item.index,
            key: item.key,
            start: item.start,
            size: item.size,
            end: item.end,
            lane: item.lane
          });
        }
      }
      return snapshot;
    };
    this._scrollToOffset = (offset, {
      adjustments,
      behavior
    }) => {
      this._intendedScrollOffset = offset + (adjustments ?? 0);
      this.options.scrollToFn(offset, { behavior, adjustments }, this);
    };
    this.measure = () => {
      this.pendingMin = null;
      this.itemSizeCache.clear();
      this.laneAssignments.clear();
      this.itemSizeCacheVersion++;
      this.notify(false);
    };
    this.setOptions(opts);
  }
  applyScrollAdjustment(delta, behavior) {
    if (delta === 0) return;
    if (isIOSWebKit() && (this.isScrolling || this._iosTouching || this._iosJustTouchEnded)) {
      this._iosDeferredAdjustment += delta;
    } else {
      this._scrollToOffset(this.getScrollOffset(), {
        adjustments: this.scrollAdjustments += delta,
        behavior
      });
    }
  }
  scheduleScrollReconcile() {
    if (!this.targetWindow) {
      this.scrollState = null;
      return;
    }
    if (this.rafId != null) return;
    this.rafId = this.targetWindow.requestAnimationFrame(() => {
      this.rafId = null;
      this.reconcileScroll();
    });
  }
  reconcileScroll() {
    if (!this.scrollState) return;
    const el = this.scrollElement;
    if (!el) return;
    const MAX_RECONCILE_MS = 5e3;
    if (this.now() - this.scrollState.startedAt > MAX_RECONCILE_MS) {
      this.scrollState = null;
      return;
    }
    const offsetInfo = this.scrollState.index != null ? this.getOffsetForIndex(this.scrollState.index, this.scrollState.align) : void 0;
    const targetOffset = offsetInfo ? offsetInfo[0] : this.scrollState.lastTargetOffset;
    const STABLE_FRAMES = 1;
    const targetChanged = targetOffset !== this.scrollState.lastTargetOffset;
    if (!targetChanged && approxEqual(targetOffset, this.getScrollOffset())) {
      this.scrollState.stableFrames++;
      if (this.scrollState.stableFrames >= STABLE_FRAMES) {
        if (this.getScrollOffset() !== targetOffset) {
          this._scrollToOffset(targetOffset, {
            adjustments: void 0,
            behavior: "auto"
          });
        }
        this.scrollState = null;
        return;
      }
    } else {
      this.scrollState.stableFrames = 0;
      if (targetChanged) {
        const viewport = this.getSize() || 600;
        const distance = Math.abs(targetOffset - this.getScrollOffset());
        const keepSmooth = this.scrollState.behavior === "smooth" && distance > viewport;
        this.scrollState.lastTargetOffset = targetOffset;
        if (!keepSmooth) {
          this.scrollState.behavior = "auto";
        }
        this._scrollToOffset(targetOffset, {
          adjustments: void 0,
          behavior: keepSmooth ? "smooth" : "auto"
        });
      }
    }
    this.scheduleScrollReconcile();
  }
}
const findNearestBinarySearch = (low, high, getCurrentValue, value) => {
  while (low <= high) {
    const middle = (low + high) / 2 | 0;
    const currentValue = getCurrentValue(middle);
    if (currentValue < value) {
      low = middle + 1;
    } else if (currentValue > value) {
      high = middle - 1;
    } else {
      return middle;
    }
  }
  if (low > 0) {
    return low - 1;
  } else {
    return 0;
  }
};
function calculateRange({
  measurements,
  outerSize,
  scrollOffset,
  lanes,
  flat
}) {
  const lastIndex = measurements.length - 1;
  const getStart = flat ? (index) => flat[index * 2] : (index) => measurements[index].start;
  const getEnd = flat ? (index) => flat[index * 2] + flat[index * 2 + 1] : (index) => measurements[index].end;
  if (measurements.length <= lanes) {
    return {
      startIndex: 0,
      endIndex: lastIndex
    };
  }
  let startIndex = findNearestBinarySearch(0, lastIndex, getStart, scrollOffset);
  let endIndex = startIndex;
  if (lanes === 1) {
    while (endIndex < lastIndex && getEnd(endIndex) < scrollOffset + outerSize) {
      endIndex++;
    }
  } else if (lanes > 1) {
    const endPerLane = Array(lanes).fill(0);
    while (endIndex < lastIndex && endPerLane.some((pos) => pos < scrollOffset + outerSize)) {
      const item = measurements[endIndex];
      endPerLane[item.lane] = item.end;
      endIndex++;
    }
    const startPerLane = Array(lanes).fill(scrollOffset + outerSize);
    while (startIndex >= 0 && startPerLane.some((pos) => pos >= scrollOffset)) {
      const item = measurements[startIndex];
      startPerLane[item.lane] = item.start;
      startIndex--;
    }
    startIndex = Math.max(0, startIndex - startIndex % lanes);
    endIndex = Math.min(lastIndex, endIndex + (lanes - 1 - endIndex % lanes));
  }
  return { startIndex, endIndex };
}
const useIsomorphicLayoutEffect = typeof document !== "undefined" ? reactExports.useLayoutEffect : reactExports.useEffect;
function useVirtualizerBase({
  useFlushSync = true,
  ...options
}) {
  const rerender = reactExports.useReducer((x2) => x2 + 1, 0)[1];
  const resolvedOptions = {
    ...options,
    onChange: (instance2, sync) => {
      var _a2;
      if (useFlushSync && sync) {
        reactDomExports.flushSync(rerender);
      } else {
        rerender();
      }
      (_a2 = options.onChange) == null ? void 0 : _a2.call(options, instance2, sync);
    }
  };
  const [instance] = reactExports.useState(
    () => new Virtualizer(resolvedOptions)
  );
  instance.setOptions(resolvedOptions);
  useIsomorphicLayoutEffect(() => {
    return instance._didMount();
  }, []);
  useIsomorphicLayoutEffect(() => {
    return instance._willUpdate();
  });
  return instance;
}
function useVirtualizer(options) {
  return useVirtualizerBase({
    observeElementRect,
    observeElementOffset,
    scrollToFn: elementScroll,
    ...options
  });
}
function VirtualCoinList({
  coins,
  timeframe,
  watchlistHas,
  flashes,
  onToggleFavorite,
  onSelect,
  estimatedRowHeight = 64,
  onEndReached
}) {
  const parentRef = reactExports.useRef(null);
  const virtualizer = useVirtualizer({
    count: coins.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimatedRowHeight,
    overscan: 6
  });
  const items = virtualizer.getVirtualItems();
  const lastItem = items[items.length - 1];
  reactExports.useEffect(() => {
    if (!onEndReached) return;
    if (!lastItem) return;
    if (lastItem.index >= coins.length - 5) {
      onEndReached();
    }
  }, [lastItem, coins.length, onEndReached]);
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    "div",
    {
      ref: parentRef,
      className: "overflow-auto max-h-[70vh]",
      "data-ocid": "market.virtual_list",
      children: /* @__PURE__ */ jsxRuntimeExports.jsx("div", { style: { height: virtualizer.getTotalSize(), position: "relative", width: "100%" }, children: items.map((vi2) => {
        const coin = coins[vi2.index];
        return /* @__PURE__ */ jsxRuntimeExports.jsx(
          "div",
          {
            "data-index": vi2.index,
            ref: virtualizer.measureElement,
            style: {
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              transform: `translateY(${vi2.start}px)`
            },
            children: /* @__PURE__ */ jsxRuntimeExports.jsx(
              CoinRow,
              {
                coin,
                rank: coin.marketCapRank,
                timeframe,
                isFavorite: watchlistHas(coin.id),
                flash: flashes[coin.id] ?? null,
                onToggleFavorite,
                onSelect
              }
            )
          },
          coin.id
        );
      }) })
    }
  );
}
function Input({ className, type, ...props }) {
  return /* @__PURE__ */ jsxRuntimeExports.jsx(
    "input",
    {
      type,
      "data-slot": "input",
      className: cn$1(
        "file:text-foreground placeholder:text-muted-foreground selection:bg-primary selection:text-primary-foreground dark:bg-input/30 border-input flex h-9 w-full min-w-0 rounded-md border bg-transparent px-3 py-1 text-base shadow-xs transition-[color,box-shadow] outline-none file:inline-flex file:h-7 file:border-0 file:bg-transparent file:text-sm file:font-medium disabled:pointer-events-none disabled:cursor-not-allowed disabled:opacity-50 md:text-sm",
        "focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px]",
        "aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive",
        className
      ),
      ...props
    }
  );
}
function usePriceDirections(coins, holdMs = 1200) {
  const prevPrices = reactExports.useRef(/* @__PURE__ */ new Map());
  const [directions, setDirections] = reactExports.useState({});
  reactExports.useEffect(() => {
    if (!coins || coins.length === 0) return;
    const updates = {};
    let changed = false;
    for (const c2 of coins) {
      const prev = prevPrices.current.get(c2.id);
      if (prev !== void 0 && prev !== c2.currentPrice) {
        updates[c2.id] = c2.currentPrice > prev ? "up" : "down";
        changed = true;
      }
      prevPrices.current.set(c2.id, c2.currentPrice);
    }
    if (changed) {
      setDirections((prev) => ({ ...prev, ...updates }));
      const timer = window.setTimeout(() => {
        setDirections((prev) => {
          const next = { ...prev };
          for (const id of Object.keys(updates)) {
            next[id] = null;
          }
          return next;
        });
      }, holdMs);
      return () => window.clearTimeout(timer);
    }
  }, [coins, holdMs]);
  return directions;
}
const STORAGE_KEY = "cryptomarket.watchlist.v1";
function readStorage() {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return parsed.filter((x2) => typeof x2 === "string");
    }
    return [];
  } catch {
    return [];
  }
}
function writeStorage(ids) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
  } catch {
  }
}
function useWatchlist() {
  const [ids, setIds] = reactExports.useState(() => readStorage());
  reactExports.useEffect(() => {
    writeStorage(ids);
  }, [ids]);
  reactExports.useEffect(() => {
    if (typeof window === "undefined") return;
    const handler = (e2) => {
      if (e2.key === STORAGE_KEY) setIds(readStorage());
    };
    window.addEventListener("storage", handler);
    return () => window.removeEventListener("storage", handler);
  }, []);
  const has = reactExports.useCallback((id) => ids.includes(id), [ids]);
  const toggle = reactExports.useCallback((id) => {
    setIds(
      (prev) => prev.includes(id) ? prev.filter((x2) => x2 !== id) : [...prev, id]
    );
  }, []);
  const clear = reactExports.useCallback(() => setIds([]), []);
  return { ids, has, toggle, clear };
}
function pctFor(c2, tf) {
  if (tf === "1h") return c2.priceChangePercentage1h;
  if (tf === "7d") return c2.priceChangePercentage7d;
  return c2.priceChangePercentage24h;
}
function Col({ k: k2, label, className, sortKey, sortDir, onSort }) {
  const active = sortKey === k2;
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    "button",
    {
      type: "button",
      onClick: () => onSort(k2),
      className: `flex items-center gap-1 text-[11px] font-medium uppercase tracking-wider transition-colors ${active ? "text-foreground" : "text-muted-foreground hover:text-foreground"} ${className ?? ""}`,
      "data-ocid": `market.sort_${k2}`,
      children: [
        label,
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          ArrowUpDown,
          {
            className: `w-3 h-3 transition-opacity ${active ? "opacity-100" : "opacity-40"} ${active && sortDir === "asc" ? "rotate-180" : ""}`
          }
        )
      ]
    }
  );
}
function TableHeader(props) {
  return /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "grid grid-cols-[auto_auto_1fr_auto_auto] sm:grid-cols-[auto_auto_1fr_auto_auto_auto_auto] md:grid-cols-[auto_auto_1fr_auto_auto_auto_auto_auto] items-center gap-2 sm:gap-4 px-3 sm:px-4 py-2.5 border-b border-border/60 bg-muted/20 sticky top-14 z-30 backdrop-blur-sm", children: [
    /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "w-6" }),
    /* @__PURE__ */ jsxRuntimeExports.jsx(Col, { k: "rank", label: "#", className: "w-6 justify-end", ...props }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-[11px] text-muted-foreground uppercase tracking-wider", children: "Krypto" }),
    /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "hidden sm:block" }),
    /* @__PURE__ */ jsxRuntimeExports.jsx(Col, { k: "price", label: "Preis", className: "justify-end", ...props }),
    /* @__PURE__ */ jsxRuntimeExports.jsx(Col, { k: "change", label: `${props.timeframe} %`, className: "justify-end min-w-[60px]", ...props }),
    /* @__PURE__ */ jsxRuntimeExports.jsx(Col, { k: "volume", label: "Volumen", className: "hidden md:flex justify-end min-w-[80px]", ...props }),
    /* @__PURE__ */ jsxRuntimeExports.jsx(Col, { k: "marketCap", label: "Marktkapital.", className: "hidden sm:flex justify-end min-w-[88px]", ...props })
  ] });
}
const TIMEFRAMES = ["1h", "24h", "7d"];
function MarketPage() {
  const {
    data: pages,
    isLoading,
    isError,
    dataUpdatedAt,
    refetch,
    isFetching,
    fetchNextPage,
    hasNextPage: hasNextPage2,
    isFetchingNextPage
  } = useMarketDataInfinite();
  const coins = reactExports.useMemo(() => {
    if (!pages) return [];
    const seen = /* @__PURE__ */ new Set();
    const out = [];
    for (const page of pages.pages) {
      for (const c2 of page.coins) {
        if (seen.has(c2.id)) continue;
        seen.add(c2.id);
        out.push(c2);
      }
    }
    return out;
  }, [pages]);
  const { data: globalStats, isLoading: globalLoading } = useGlobalStats();
  const watchlist = useWatchlist();
  const flashes = usePriceDirections(coins);
  const [search, setSearch] = reactExports.useState("");
  const [sortKey, setSortKey] = reactExports.useState("marketCap");
  const [sortDir, setSortDir] = reactExports.useState("desc");
  const [timeframe, setTimeframe] = reactExports.useState("24h");
  const [tab, setTab] = reactExports.useState("all");
  const [lastUpdated, setLastUpdated] = reactExports.useState(null);
  const [selected, setSelected] = reactExports.useState(null);
  const [drawerOpen, setDrawerOpen] = reactExports.useState(false);
  const inputRef = reactExports.useRef(null);
  reactExports.useEffect(() => {
    if (dataUpdatedAt) setLastUpdated(new Date(dataUpdatedAt));
  }, [dataUpdatedAt]);
  reactExports.useEffect(() => {
    if (!selected) return;
    const fresh = coins.find((c2) => c2.id === selected.id);
    if (fresh && fresh !== selected) setSelected(fresh);
  }, [coins, selected]);
  reactExports.useEffect(() => {
    const handler = (e2) => {
      var _a2, _b, _c;
      if ((e2.metaKey || e2.ctrlKey) && e2.key.toLowerCase() === "k") {
        e2.preventDefault();
        (_a2 = inputRef.current) == null ? void 0 : _a2.focus();
        (_b = inputRef.current) == null ? void 0 : _b.select();
      } else if (e2.key === "Escape" && document.activeElement === inputRef.current) {
        (_c = inputRef.current) == null ? void 0 : _c.blur();
        if (search) setSearch("");
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [search]);
  const handleSort = reactExports.useCallback((k2) => {
    if (sortKey === k2) {
      setSortDir((d2) => d2 === "asc" ? "desc" : "asc");
    } else {
      setSortKey(k2);
      setSortDir(k2 === "rank" ? "asc" : "desc");
    }
  }, [sortKey]);
  const handleSelect = reactExports.useCallback((c2) => {
    setSelected(c2);
    setDrawerOpen(true);
  }, []);
  const handleDrawerOpenChange = reactExports.useCallback((open) => {
    setDrawerOpen(open);
    if (!open) {
      window.setTimeout(() => setSelected(null), 300);
    }
  }, []);
  const filteredAndSorted = reactExports.useMemo(() => {
    if (coins.length === 0) return [];
    const q2 = search.trim().toLowerCase();
    let base;
    if (tab === "watchlist") {
      base = coins.filter((c2) => watchlist.has(c2.id));
    } else if (tab === "gainers") {
      base = [...coins].filter((c2) => pctFor(c2, timeframe) > 0).sort((a2, b2) => pctFor(b2, timeframe) - pctFor(a2, timeframe)).slice(0, 50);
    } else if (tab === "losers") {
      base = [...coins].filter((c2) => pctFor(c2, timeframe) < 0).sort((a2, b2) => pctFor(a2, timeframe) - pctFor(b2, timeframe)).slice(0, 50);
    } else {
      base = coins;
    }
    const filtered = q2 ? base.filter(
      (c2) => c2.name.toLowerCase().includes(q2) || c2.symbol.toLowerCase().includes(q2) || c2.id.toLowerCase().includes(q2)
    ) : base;
    if ((tab === "gainers" || tab === "losers") && sortKey === "marketCap") {
      return filtered;
    }
    return [...filtered].sort((a2, b2) => {
      let diff = 0;
      if (sortKey === "rank") diff = a2.marketCapRank - b2.marketCapRank;
      else if (sortKey === "price") diff = a2.currentPrice - b2.currentPrice;
      else if (sortKey === "change") diff = pctFor(a2, timeframe) - pctFor(b2, timeframe);
      else if (sortKey === "marketCap") diff = a2.marketCap - b2.marketCap;
      else if (sortKey === "volume") diff = a2.totalVolume - b2.totalVolume;
      return sortDir === "asc" ? diff : -diff;
    });
  }, [coins, search, sortKey, sortDir, timeframe, tab, watchlist]);
  const counts = reactExports.useMemo(() => {
    if (coins.length === 0) return { gainers: 0, losers: 0, watchlist: 0, all: 0 };
    let g2 = 0;
    let l2 = 0;
    for (const c2 of coins) {
      const p2 = pctFor(c2, timeframe);
      if (p2 > 0) g2++;
      else if (p2 < 0) l2++;
    }
    return { gainers: g2, losers: l2, watchlist: watchlist.ids.length, all: coins.length };
  }, [coins, timeframe, watchlist.ids]);
  const canAutoLoad = tab === "all" && !search;
  const handleEndReached = reactExports.useCallback(() => {
    if (!canAutoLoad) return;
    if (hasNextPage2 && !isFetchingNextPage) {
      fetchNextPage();
    }
  }, [canAutoLoad, hasNextPage2, isFetchingNextPage, fetchNextPage]);
  return /* @__PURE__ */ jsxRuntimeExports.jsxs(
    Layout,
    {
      lastUpdated,
      isLive: !isLoading && !isError,
      isLoading,
      children: [
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          GlobalStatsBar,
          {
            data: globalStats,
            isLoading: globalLoading && !globalStats,
            coinCount: coins.length
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          TopMovers,
          {
            coins,
            isLoading,
            timeframe,
            onSelect: handleSelect
          }
        ),
        /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex flex-col sm:flex-row gap-2 sm:gap-3 mb-3", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "relative flex-1 min-w-0", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(Search, { className: "absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground pointer-events-none" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(
              Input,
              {
                ref: inputRef,
                value: search,
                onChange: (e2) => setSearch(e2.target.value),
                placeholder: "Suche nach Name oder Symbol...",
                className: "pl-9 pr-20 bg-card border-border/60 focus:border-primary/60 placeholder:text-muted-foreground/60",
                "data-ocid": "market.search_input"
              }
            ),
            /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "absolute right-2 top-1/2 -translate-y-1/2 flex items-center gap-1", children: search ? /* @__PURE__ */ jsxRuntimeExports.jsx(
              "button",
              {
                type: "button",
                onClick: () => setSearch(""),
                className: "w-6 h-6 flex items-center justify-center text-muted-foreground hover:text-foreground transition-colors rounded",
                "aria-label": "Suche löschen",
                "data-ocid": "market.search_clear_button",
                children: /* @__PURE__ */ jsxRuntimeExports.jsx(X$1, { className: "w-3.5 h-3.5" })
              }
            ) : /* @__PURE__ */ jsxRuntimeExports.jsx("kbd", { className: "hidden sm:inline-flex h-5 select-none items-center gap-1 rounded border border-border/60 bg-muted/40 px-1.5 font-mono text-[10px] font-medium text-muted-foreground", children: "⌘K" }) })
          ] }),
          /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex items-center rounded-lg border border-border/60 bg-card p-0.5 shrink-0", children: TIMEFRAMES.map((tf) => /* @__PURE__ */ jsxRuntimeExports.jsx(
            "button",
            {
              type: "button",
              onClick: () => setTimeframe(tf),
              className: `px-2.5 sm:px-3 py-1.5 rounded-md text-xs font-semibold uppercase tracking-wider transition-colors ${timeframe === tf ? "bg-primary/15 text-primary" : "text-muted-foreground hover:text-foreground"}`,
              "data-ocid": `market.timeframe_${tf}`,
              children: tf
            },
            tf
          )) }),
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            "button",
            {
              type: "button",
              onClick: () => refetch(),
              disabled: isFetching,
              className: "hidden sm:flex items-center justify-center w-9 h-9 rounded-lg border border-border/60 bg-card text-muted-foreground hover:text-foreground hover:border-border transition-colors disabled:opacity-50",
              "aria-label": "Daten aktualisieren",
              "data-ocid": "market.refresh_button",
              children: /* @__PURE__ */ jsxRuntimeExports.jsx(RefreshCw, { className: `w-4 h-4 ${isFetching ? "animate-spin" : ""}` })
            }
          )
        ] }),
        /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex items-center gap-1 mb-3 overflow-x-auto -mx-1 px-1", "data-ocid": "market.tabs", children: [
          { key: "all", label: "Alle", count: counts.all },
          { key: "watchlist", label: "Watchlist", count: counts.watchlist, icon: /* @__PURE__ */ jsxRuntimeExports.jsx(Star, { className: "w-3 h-3" }) },
          { key: "gainers", label: "Top Gewinner", count: counts.gainers },
          { key: "losers", label: "Top Verlierer", count: counts.losers }
        ].map(({ key, label, count: count2, icon }) => {
          const active = tab === key;
          return /* @__PURE__ */ jsxRuntimeExports.jsxs(
            "button",
            {
              type: "button",
              onClick: () => setTab(key),
              className: `flex items-center gap-1.5 px-3 py-1.5 rounded-md text-xs font-semibold transition-colors shrink-0 ${active ? "bg-card border border-border text-foreground" : "text-muted-foreground hover:text-foreground border border-transparent"}`,
              "data-ocid": `market.tab_${key}`,
              children: [
                icon,
                /* @__PURE__ */ jsxRuntimeExports.jsx("span", { children: label }),
                /* @__PURE__ */ jsxRuntimeExports.jsx("span", { className: "text-[10px] tabular-nums opacity-70", children: count2 })
              ]
            },
            key
          );
        }) }),
        /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "rounded-xl border border-border/60 overflow-hidden bg-background shadow-subtle", children: [
          /* @__PURE__ */ jsxRuntimeExports.jsx(
            TableHeader,
            {
              sortKey,
              sortDir,
              timeframe,
              onSort: handleSort
            }
          ),
          isLoading && /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "space-y-0", "data-ocid": "market.loading_state", children: Array.from({ length: 12 }, (_2, i) => i).map((i) => /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex items-center gap-3 px-4 py-3.5 border-b border-border/30", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "w-4 h-4 rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "w-6 h-4 rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "w-7 h-7 rounded-full" }),
            /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex-1 space-y-1", children: [
              /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-4 w-28 rounded" }),
              /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-3 w-12 rounded" })
            ] }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "hidden sm:block h-7 w-[88px] rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-4 w-20 rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "h-6 w-14 rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "hidden md:block h-4 w-20 rounded" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx(Skeleton, { className: "hidden sm:block h-4 w-20 rounded" })
          ] }, i)) }),
          isError && /* @__PURE__ */ jsxRuntimeExports.jsxs("div", { className: "flex flex-col items-center justify-center gap-3 py-16 text-center", "data-ocid": "market.error_state", children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-muted-foreground text-sm", children: "Daten konnten nicht geladen werden." }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("button", { type: "button", onClick: () => refetch(), className: "text-primary text-sm hover:underline", children: "Erneut versuchen" })
          ] }),
          !isLoading && !isError && filteredAndSorted.length === 0 && /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex flex-col items-center justify-center gap-2 py-16 text-center px-6", "data-ocid": "market.empty_state", children: tab === "watchlist" && watchlist.ids.length === 0 ? /* @__PURE__ */ jsxRuntimeExports.jsxs(jsxRuntimeExports.Fragment, { children: [
            /* @__PURE__ */ jsxRuntimeExports.jsx(Star, { className: "w-8 h-8 text-muted-foreground/40" }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-muted-foreground text-sm", children: "Noch keine Coins in deiner Watchlist." }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-muted-foreground/60 text-xs max-w-sm", children: "Tippe auf das Stern-Symbol neben einem Coin, um ihn hier zu speichern." }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("button", { type: "button", onClick: () => setTab("all"), className: "text-primary text-sm hover:underline mt-1", children: "Alle Coins anzeigen" })
          ] }) : search ? /* @__PURE__ */ jsxRuntimeExports.jsxs(jsxRuntimeExports.Fragment, { children: [
            /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "text-muted-foreground text-sm", children: [
              'Keine Ergebnisse für "',
              search,
              '"'
            ] }),
            /* @__PURE__ */ jsxRuntimeExports.jsx("button", { type: "button", onClick: () => setSearch(""), className: "text-primary text-sm hover:underline", children: "Suche zurücksetzen" })
          ] }) : /* @__PURE__ */ jsxRuntimeExports.jsx("p", { className: "text-muted-foreground text-sm", children: "Keine Coins im aktuellen Filter." }) }),
          !isLoading && !isError && filteredAndSorted.length > 0 && /* @__PURE__ */ jsxRuntimeExports.jsx(
            VirtualCoinList,
            {
              coins: filteredAndSorted,
              timeframe,
              watchlistHas: watchlist.has,
              flashes,
              onToggleFavorite: watchlist.toggle,
              onSelect: handleSelect,
              onEndReached: handleEndReached
            }
          )
        ] }),
        !isLoading && !isError && canAutoLoad && hasNextPage2 && /* @__PURE__ */ jsxRuntimeExports.jsx("div", { className: "flex justify-center mt-4", children: /* @__PURE__ */ jsxRuntimeExports.jsx(
          "button",
          {
            type: "button",
            onClick: () => fetchNextPage(),
            disabled: isFetchingNextPage,
            className: "px-4 py-2 rounded-lg border border-border/60 bg-card text-sm font-semibold text-foreground hover:bg-card/80 disabled:opacity-60 transition-colors",
            "data-ocid": "market.load_more_button",
            children: isFetchingNextPage ? `Lädt... (${coins.length} von ${TOTAL_COINS_TARGET})` : `Mehr laden (${coins.length} von ${TOTAL_COINS_TARGET})`
          }
        ) }),
        !isLoading && !isError && canAutoLoad && !hasNextPage2 && coins.length >= MAX_PAGES * 100 && /* @__PURE__ */ jsxRuntimeExports.jsxs("p", { className: "text-center text-xs text-muted-foreground mt-4", children: [
          "Alle ",
          coins.length,
          " Coins geladen."
        ] }),
        /* @__PURE__ */ jsxRuntimeExports.jsx(
          CoinDetailDrawer,
          {
            coin: selected,
            open: drawerOpen,
            onOpenChange: handleDrawerOpenChange,
            isFavorite: selected ? watchlist.has(selected.id) : false,
            onToggleFavorite: watchlist.toggle
          }
        )
      ]
    }
  );
}
export {
  MarketPage as default
};
