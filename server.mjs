import { createRequire as __cr } from 'node:module'; const require = __cr(import.meta.url);
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __commonJS = (cb, mod) => function __require2() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/constants.js
var require_constants = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/constants.js"(exports, module) {
    "use strict";
    var BINARY_TYPES = ["nodebuffer", "arraybuffer", "fragments"];
    var hasBlob = typeof Blob !== "undefined";
    if (hasBlob) BINARY_TYPES.push("blob");
    module.exports = {
      BINARY_TYPES,
      CLOSE_TIMEOUT: 3e4,
      EMPTY_BUFFER: Buffer.alloc(0),
      GUID: "258EAFA5-E914-47DA-95CA-C5AB0DC85B11",
      hasBlob,
      kForOnEventAttribute: Symbol("kIsForOnEventAttribute"),
      kListener: Symbol("kListener"),
      kStatusCode: Symbol("status-code"),
      kWebSocket: Symbol("websocket"),
      NOOP: () => {
      }
    };
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/buffer-util.js
var require_buffer_util = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/buffer-util.js"(exports, module) {
    "use strict";
    var { EMPTY_BUFFER } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    function concat(list, totalLength) {
      if (list.length === 0) return EMPTY_BUFFER;
      if (list.length === 1) return list[0];
      const target = Buffer.allocUnsafe(totalLength);
      let offset = 0;
      for (let i = 0; i < list.length; i++) {
        const buf = list[i];
        target.set(buf, offset);
        offset += buf.length;
      }
      if (offset < totalLength) {
        return new FastBuffer(target.buffer, target.byteOffset, offset);
      }
      return target;
    }
    function _mask(source, mask, output, offset, length) {
      for (let i = 0; i < length; i++) {
        output[offset + i] = source[i] ^ mask[i & 3];
      }
    }
    function _unmask(buffer, mask) {
      for (let i = 0; i < buffer.length; i++) {
        buffer[i] ^= mask[i & 3];
      }
    }
    function toArrayBuffer(buf) {
      if (buf.length === buf.buffer.byteLength) {
        return buf.buffer;
      }
      return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.length);
    }
    function toBuffer(data) {
      toBuffer.readOnly = true;
      if (Buffer.isBuffer(data)) return data;
      let buf;
      if (data instanceof ArrayBuffer) {
        buf = new FastBuffer(data);
      } else if (ArrayBuffer.isView(data)) {
        buf = new FastBuffer(data.buffer, data.byteOffset, data.byteLength);
      } else {
        buf = Buffer.from(data);
        toBuffer.readOnly = false;
      }
      return buf;
    }
    module.exports = {
      concat,
      mask: _mask,
      toArrayBuffer,
      toBuffer,
      unmask: _unmask
    };
    if (!process.env.WS_NO_BUFFER_UTIL) {
      try {
        const bufferUtil = __require("bufferutil");
        module.exports.mask = function(source, mask, output, offset, length) {
          if (length < 48) _mask(source, mask, output, offset, length);
          else bufferUtil.mask(source, mask, output, offset, length);
        };
        module.exports.unmask = function(buffer, mask) {
          if (buffer.length < 32) _unmask(buffer, mask);
          else bufferUtil.unmask(buffer, mask);
        };
      } catch (e) {
      }
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/limiter.js
var require_limiter = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/limiter.js"(exports, module) {
    "use strict";
    var kDone = Symbol("kDone");
    var kRun = Symbol("kRun");
    var Limiter = class {
      /**
       * Creates a new `Limiter`.
       *
       * @param {Number} [concurrency=Infinity] The maximum number of jobs allowed
       *     to run concurrently
       */
      constructor(concurrency) {
        this[kDone] = () => {
          this.pending--;
          this[kRun]();
        };
        this.concurrency = concurrency || Infinity;
        this.jobs = [];
        this.pending = 0;
      }
      /**
       * Adds a job to the queue.
       *
       * @param {Function} job The job to run
       * @public
       */
      add(job) {
        this.jobs.push(job);
        this[kRun]();
      }
      /**
       * Removes a job from the queue and runs it if possible.
       *
       * @private
       */
      [kRun]() {
        if (this.pending === this.concurrency) return;
        if (this.jobs.length) {
          const job = this.jobs.shift();
          this.pending++;
          job(this[kDone]);
        }
      }
    };
    module.exports = Limiter;
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/permessage-deflate.js
var require_permessage_deflate = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/permessage-deflate.js"(exports, module) {
    "use strict";
    var zlib = __require("zlib");
    var bufferUtil = require_buffer_util();
    var Limiter = require_limiter();
    var { kStatusCode } = require_constants();
    var FastBuffer = Buffer[Symbol.species];
    var TRAILER = Buffer.from([0, 0, 255, 255]);
    var kPerMessageDeflate = Symbol("permessage-deflate");
    var kTotalLength = Symbol("total-length");
    var kCallback = Symbol("callback");
    var kBuffers = Symbol("buffers");
    var kError = Symbol("error");
    var zlibLimiter;
    var PerMessageDeflate2 = class {
      /**
       * Creates a PerMessageDeflate instance.
       *
       * @param {Object} [options] Configuration options
       * @param {(Boolean|Number)} [options.clientMaxWindowBits] Advertise support
       *     for, or request, a custom client window size
       * @param {Boolean} [options.clientNoContextTakeover=false] Advertise/
       *     acknowledge disabling of client context takeover
       * @param {Number} [options.concurrencyLimit=10] The number of concurrent
       *     calls to zlib
       * @param {Boolean} [options.isServer=false] Create the instance in either
       *     server or client mode
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {(Boolean|Number)} [options.serverMaxWindowBits] Request/confirm the
       *     use of a custom server window size
       * @param {Boolean} [options.serverNoContextTakeover=false] Request/accept
       *     disabling of server context takeover
       * @param {Number} [options.threshold=1024] Size (in bytes) below which
       *     messages should not be compressed if context takeover is disabled
       * @param {Object} [options.zlibDeflateOptions] Options to pass to zlib on
       *     deflate
       * @param {Object} [options.zlibInflateOptions] Options to pass to zlib on
       *     inflate
       */
      constructor(options) {
        this._options = options || {};
        this._threshold = this._options.threshold !== void 0 ? this._options.threshold : 1024;
        this._maxPayload = this._options.maxPayload | 0;
        this._isServer = !!this._options.isServer;
        this._deflate = null;
        this._inflate = null;
        this.params = null;
        if (!zlibLimiter) {
          const concurrency = this._options.concurrencyLimit !== void 0 ? this._options.concurrencyLimit : 10;
          zlibLimiter = new Limiter(concurrency);
        }
      }
      /**
       * @type {String}
       */
      static get extensionName() {
        return "permessage-deflate";
      }
      /**
       * Create an extension negotiation offer.
       *
       * @return {Object} Extension parameters
       * @public
       */
      offer() {
        const params = {};
        if (this._options.serverNoContextTakeover) {
          params.server_no_context_takeover = true;
        }
        if (this._options.clientNoContextTakeover) {
          params.client_no_context_takeover = true;
        }
        if (this._options.serverMaxWindowBits) {
          params.server_max_window_bits = this._options.serverMaxWindowBits;
        }
        if (this._options.clientMaxWindowBits) {
          params.client_max_window_bits = this._options.clientMaxWindowBits;
        } else if (this._options.clientMaxWindowBits == null) {
          params.client_max_window_bits = true;
        }
        return params;
      }
      /**
       * Accept an extension negotiation offer/response.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Object} Accepted configuration
       * @public
       */
      accept(configurations) {
        configurations = this.normalizeParams(configurations);
        this.params = this._isServer ? this.acceptAsServer(configurations) : this.acceptAsClient(configurations);
        return this.params;
      }
      /**
       * Releases all resources used by the extension.
       *
       * @public
       */
      cleanup() {
        if (this._inflate) {
          this._inflate.close();
          this._inflate = null;
        }
        if (this._deflate) {
          const callback = this._deflate[kCallback];
          this._deflate.close();
          this._deflate = null;
          if (callback) {
            callback(
              new Error(
                "The deflate stream was closed while data was being processed"
              )
            );
          }
        }
      }
      /**
       *  Accept an extension negotiation offer.
       *
       * @param {Array} offers The extension negotiation offers
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsServer(offers) {
        const opts = this._options;
        const accepted = offers.find((params) => {
          if (opts.serverNoContextTakeover === false && params.server_no_context_takeover || params.server_max_window_bits && (opts.serverMaxWindowBits === false || typeof opts.serverMaxWindowBits === "number" && opts.serverMaxWindowBits > params.server_max_window_bits) || typeof opts.clientMaxWindowBits === "number" && (typeof params.client_max_window_bits === "number" ? opts.clientMaxWindowBits > params.client_max_window_bits : !params.client_max_window_bits)) {
            return false;
          }
          return true;
        });
        if (!accepted) {
          throw new Error("None of the extension offers can be accepted");
        }
        if (opts.serverNoContextTakeover) {
          accepted.server_no_context_takeover = true;
        }
        if (opts.clientNoContextTakeover) {
          accepted.client_no_context_takeover = true;
        }
        if (typeof opts.serverMaxWindowBits === "number") {
          accepted.server_max_window_bits = opts.serverMaxWindowBits;
        }
        if (typeof opts.clientMaxWindowBits === "number") {
          accepted.client_max_window_bits = opts.clientMaxWindowBits;
        } else if (accepted.client_max_window_bits === true || opts.clientMaxWindowBits === false) {
          delete accepted.client_max_window_bits;
        }
        return accepted;
      }
      /**
       * Accept the extension negotiation response.
       *
       * @param {Array} response The extension negotiation response
       * @return {Object} Accepted configuration
       * @private
       */
      acceptAsClient(response) {
        const params = response[0];
        if (this._options.clientNoContextTakeover === false && params.client_no_context_takeover) {
          throw new Error('Unexpected parameter "client_no_context_takeover"');
        }
        if (!params.client_max_window_bits) {
          if (typeof this._options.clientMaxWindowBits === "number") {
            params.client_max_window_bits = this._options.clientMaxWindowBits;
          }
        } else if (this._options.clientMaxWindowBits === false || typeof this._options.clientMaxWindowBits === "number" && params.client_max_window_bits > this._options.clientMaxWindowBits) {
          throw new Error(
            'Unexpected or invalid parameter "client_max_window_bits"'
          );
        }
        return params;
      }
      /**
       * Normalize parameters.
       *
       * @param {Array} configurations The extension negotiation offers/reponse
       * @return {Array} The offers/response with normalized parameters
       * @private
       */
      normalizeParams(configurations) {
        configurations.forEach((params) => {
          Object.keys(params).forEach((key) => {
            let value = params[key];
            if (value.length > 1) {
              throw new Error(`Parameter "${key}" must have only a single value`);
            }
            value = value[0];
            if (key === "client_max_window_bits") {
              if (value !== true) {
                const num = +value;
                if (!Number.isInteger(num) || num < 8 || num > 15) {
                  throw new TypeError(
                    `Invalid value for parameter "${key}": ${value}`
                  );
                }
                value = num;
              } else if (!this._isServer) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else if (key === "server_max_window_bits") {
              const num = +value;
              if (!Number.isInteger(num) || num < 8 || num > 15) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
              value = num;
            } else if (key === "client_no_context_takeover" || key === "server_no_context_takeover") {
              if (value !== true) {
                throw new TypeError(
                  `Invalid value for parameter "${key}": ${value}`
                );
              }
            } else {
              throw new Error(`Unknown parameter "${key}"`);
            }
            params[key] = value;
          });
        });
        return configurations;
      }
      /**
       * Decompress data. Concurrency limited.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      decompress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._decompress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Compress data. Concurrency limited.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @public
       */
      compress(data, fin, callback) {
        zlibLimiter.add((done) => {
          this._compress(data, fin, (err, result) => {
            done();
            callback(err, result);
          });
        });
      }
      /**
       * Decompress data.
       *
       * @param {Buffer} data Compressed data
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _decompress(data, fin, callback) {
        const endpoint = this._isServer ? "client" : "server";
        if (!this._inflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._inflate = zlib.createInflateRaw({
            ...this._options.zlibInflateOptions,
            windowBits
          });
          this._inflate[kPerMessageDeflate] = this;
          this._inflate[kTotalLength] = 0;
          this._inflate[kBuffers] = [];
          this._inflate.on("error", inflateOnError);
          this._inflate.on("data", inflateOnData);
        }
        this._inflate[kCallback] = callback;
        this._inflate.write(data);
        if (fin) this._inflate.write(TRAILER);
        this._inflate.flush(() => {
          const err = this._inflate[kError];
          if (err) {
            this._inflate.close();
            this._inflate = null;
            callback(err);
            return;
          }
          const data2 = bufferUtil.concat(
            this._inflate[kBuffers],
            this._inflate[kTotalLength]
          );
          if (this._inflate._readableState.endEmitted) {
            this._inflate.close();
            this._inflate = null;
          } else {
            this._inflate[kTotalLength] = 0;
            this._inflate[kBuffers] = [];
            if (fin && this.params[`${endpoint}_no_context_takeover`]) {
              this._inflate.reset();
            }
          }
          callback(null, data2);
        });
      }
      /**
       * Compress data.
       *
       * @param {(Buffer|String)} data Data to compress
       * @param {Boolean} fin Specifies whether or not this is the last fragment
       * @param {Function} callback Callback
       * @private
       */
      _compress(data, fin, callback) {
        const endpoint = this._isServer ? "server" : "client";
        if (!this._deflate) {
          const key = `${endpoint}_max_window_bits`;
          const windowBits = typeof this.params[key] !== "number" ? zlib.Z_DEFAULT_WINDOWBITS : this.params[key];
          this._deflate = zlib.createDeflateRaw({
            ...this._options.zlibDeflateOptions,
            windowBits
          });
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          this._deflate.on("data", deflateOnData);
        }
        this._deflate[kCallback] = callback;
        this._deflate.write(data);
        this._deflate.flush(zlib.Z_SYNC_FLUSH, () => {
          if (!this._deflate) {
            return;
          }
          let data2 = bufferUtil.concat(
            this._deflate[kBuffers],
            this._deflate[kTotalLength]
          );
          if (fin) {
            data2 = new FastBuffer(data2.buffer, data2.byteOffset, data2.length - 4);
          }
          this._deflate[kCallback] = null;
          this._deflate[kTotalLength] = 0;
          this._deflate[kBuffers] = [];
          if (fin && this.params[`${endpoint}_no_context_takeover`]) {
            this._deflate.reset();
          }
          callback(null, data2);
        });
      }
    };
    module.exports = PerMessageDeflate2;
    function deflateOnData(chunk) {
      this[kBuffers].push(chunk);
      this[kTotalLength] += chunk.length;
    }
    function inflateOnData(chunk) {
      this[kTotalLength] += chunk.length;
      if (this[kPerMessageDeflate]._maxPayload < 1 || this[kTotalLength] <= this[kPerMessageDeflate]._maxPayload) {
        this[kBuffers].push(chunk);
        return;
      }
      this[kError] = new RangeError("Max payload size exceeded");
      this[kError].code = "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH";
      this[kError][kStatusCode] = 1009;
      this.removeListener("data", inflateOnData);
      this.reset();
    }
    function inflateOnError(err) {
      this[kPerMessageDeflate]._inflate = null;
      if (this[kError]) {
        this[kCallback](this[kError]);
        return;
      }
      err[kStatusCode] = 1007;
      this[kCallback](err);
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/validation.js
var require_validation = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/validation.js"(exports, module) {
    "use strict";
    var { isUtf8 } = __require("buffer");
    var { hasBlob } = require_constants();
    var tokenChars = [
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 0 - 15
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      // 16 - 31
      0,
      1,
      0,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      1,
      1,
      0,
      1,
      1,
      0,
      // 32 - 47
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      0,
      0,
      0,
      // 48 - 63
      0,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 64 - 79
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      0,
      0,
      1,
      1,
      // 80 - 95
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      // 96 - 111
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      0,
      1,
      0,
      1,
      0
      // 112 - 127
    ];
    function isValidStatusCode(code) {
      return code >= 1e3 && code <= 1014 && code !== 1004 && code !== 1005 && code !== 1006 || code >= 3e3 && code <= 4999;
    }
    function _isValidUTF8(buf) {
      const len = buf.length;
      let i = 0;
      while (i < len) {
        if ((buf[i] & 128) === 0) {
          i++;
        } else if ((buf[i] & 224) === 192) {
          if (i + 1 === len || (buf[i + 1] & 192) !== 128 || (buf[i] & 254) === 192) {
            return false;
          }
          i += 2;
        } else if ((buf[i] & 240) === 224) {
          if (i + 2 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || buf[i] === 224 && (buf[i + 1] & 224) === 128 || // Overlong
          buf[i] === 237 && (buf[i + 1] & 224) === 160) {
            return false;
          }
          i += 3;
        } else if ((buf[i] & 248) === 240) {
          if (i + 3 >= len || (buf[i + 1] & 192) !== 128 || (buf[i + 2] & 192) !== 128 || (buf[i + 3] & 192) !== 128 || buf[i] === 240 && (buf[i + 1] & 240) === 128 || // Overlong
          buf[i] === 244 && buf[i + 1] > 143 || buf[i] > 244) {
            return false;
          }
          i += 4;
        } else {
          return false;
        }
      }
      return true;
    }
    function isBlob(value) {
      return hasBlob && typeof value === "object" && typeof value.arrayBuffer === "function" && typeof value.type === "string" && typeof value.stream === "function" && (value[Symbol.toStringTag] === "Blob" || value[Symbol.toStringTag] === "File");
    }
    module.exports = {
      isBlob,
      isValidStatusCode,
      isValidUTF8: _isValidUTF8,
      tokenChars
    };
    if (isUtf8) {
      module.exports.isValidUTF8 = function(buf) {
        return buf.length < 24 ? _isValidUTF8(buf) : isUtf8(buf);
      };
    } else if (!process.env.WS_NO_UTF_8_VALIDATE) {
      try {
        const isValidUTF8 = __require("utf-8-validate");
        module.exports.isValidUTF8 = function(buf) {
          return buf.length < 32 ? _isValidUTF8(buf) : isValidUTF8(buf);
        };
      } catch (e) {
      }
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/receiver.js
var require_receiver = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/receiver.js"(exports, module) {
    "use strict";
    var { Writable } = __require("stream");
    var PerMessageDeflate2 = require_permessage_deflate();
    var {
      BINARY_TYPES,
      EMPTY_BUFFER,
      kStatusCode,
      kWebSocket
    } = require_constants();
    var { concat, toArrayBuffer, unmask } = require_buffer_util();
    var { isValidStatusCode, isValidUTF8 } = require_validation();
    var FastBuffer = Buffer[Symbol.species];
    var GET_INFO = 0;
    var GET_PAYLOAD_LENGTH_16 = 1;
    var GET_PAYLOAD_LENGTH_64 = 2;
    var GET_MASK = 3;
    var GET_DATA = 4;
    var INFLATING = 5;
    var DEFER_EVENT = 6;
    var Receiver2 = class extends Writable {
      /**
       * Creates a Receiver instance.
       *
       * @param {Object} [options] Options object
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {String} [options.binaryType=nodebuffer] The type for binary data
       * @param {Object} [options.extensions] An object containing the negotiated
       *     extensions
       * @param {Boolean} [options.isServer=false] Specifies whether to operate in
       *     client or server mode
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message length
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       */
      constructor(options = {}) {
        super();
        this._allowSynchronousEvents = options.allowSynchronousEvents !== void 0 ? options.allowSynchronousEvents : true;
        this._binaryType = options.binaryType || BINARY_TYPES[0];
        this._extensions = options.extensions || {};
        this._isServer = !!options.isServer;
        this._maxBufferedChunks = options.maxBufferedChunks | 0;
        this._maxFragments = options.maxFragments | 0;
        this._maxPayload = options.maxPayload | 0;
        this._skipUTF8Validation = !!options.skipUTF8Validation;
        this[kWebSocket] = void 0;
        this._bufferedBytes = 0;
        this._buffers = [];
        this._compressed = false;
        this._payloadLength = 0;
        this._mask = void 0;
        this._fragmented = 0;
        this._masked = false;
        this._fin = false;
        this._opcode = 0;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._numFragments = 0;
        this._fragments = [];
        this._errored = false;
        this._loop = false;
        this._state = GET_INFO;
      }
      /**
       * Implements `Writable.prototype._write()`.
       *
       * @param {Buffer} chunk The chunk of data to write
       * @param {String} encoding The character encoding of `chunk`
       * @param {Function} cb Callback
       * @private
       */
      _write(chunk, encoding, cb) {
        if (this._opcode === 8 && this._state == GET_INFO) return cb();
        if (this._maxBufferedChunks > 0 && this._buffers.length >= this._maxBufferedChunks) {
          cb(
            this.createError(
              RangeError,
              "Too many buffered chunks",
              false,
              1008,
              "WS_ERR_TOO_MANY_BUFFERED_PARTS"
            )
          );
          return;
        }
        this._bufferedBytes += chunk.length;
        this._buffers.push(chunk);
        this.startLoop(cb);
      }
      /**
       * Consumes `n` bytes from the buffered data.
       *
       * @param {Number} n The number of bytes to consume
       * @return {Buffer} The consumed bytes
       * @private
       */
      consume(n) {
        this._bufferedBytes -= n;
        if (n === this._buffers[0].length) return this._buffers.shift();
        if (n < this._buffers[0].length) {
          const buf = this._buffers[0];
          this._buffers[0] = new FastBuffer(
            buf.buffer,
            buf.byteOffset + n,
            buf.length - n
          );
          return new FastBuffer(buf.buffer, buf.byteOffset, n);
        }
        const dst = Buffer.allocUnsafe(n);
        do {
          const buf = this._buffers[0];
          const offset = dst.length - n;
          if (n >= buf.length) {
            dst.set(this._buffers.shift(), offset);
          } else {
            dst.set(new Uint8Array(buf.buffer, buf.byteOffset, n), offset);
            this._buffers[0] = new FastBuffer(
              buf.buffer,
              buf.byteOffset + n,
              buf.length - n
            );
          }
          n -= buf.length;
        } while (n > 0);
        return dst;
      }
      /**
       * Starts the parsing loop.
       *
       * @param {Function} cb Callback
       * @private
       */
      startLoop(cb) {
        this._loop = true;
        do {
          switch (this._state) {
            case GET_INFO:
              this.getInfo(cb);
              break;
            case GET_PAYLOAD_LENGTH_16:
              this.getPayloadLength16(cb);
              break;
            case GET_PAYLOAD_LENGTH_64:
              this.getPayloadLength64(cb);
              break;
            case GET_MASK:
              this.getMask();
              break;
            case GET_DATA:
              this.getData(cb);
              break;
            case INFLATING:
            case DEFER_EVENT:
              this._loop = false;
              return;
          }
        } while (this._loop);
        if (!this._errored) cb();
      }
      /**
       * Reads the first two bytes of a frame.
       *
       * @param {Function} cb Callback
       * @private
       */
      getInfo(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        const buf = this.consume(2);
        if ((buf[0] & 48) !== 0) {
          const error = this.createError(
            RangeError,
            "RSV2 and RSV3 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_2_3"
          );
          cb(error);
          return;
        }
        const compressed = (buf[0] & 64) === 64;
        if (compressed && !this._extensions[PerMessageDeflate2.extensionName]) {
          const error = this.createError(
            RangeError,
            "RSV1 must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_RSV_1"
          );
          cb(error);
          return;
        }
        this._fin = (buf[0] & 128) === 128;
        this._opcode = buf[0] & 15;
        this._payloadLength = buf[1] & 127;
        if (this._opcode === 0) {
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (!this._fragmented) {
            const error = this.createError(
              RangeError,
              "invalid opcode 0",
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._opcode = this._fragmented;
        } else if (this._opcode === 1 || this._opcode === 2) {
          if (this._fragmented) {
            const error = this.createError(
              RangeError,
              `invalid opcode ${this._opcode}`,
              true,
              1002,
              "WS_ERR_INVALID_OPCODE"
            );
            cb(error);
            return;
          }
          this._compressed = compressed;
        } else if (this._opcode > 7 && this._opcode < 11) {
          if (!this._fin) {
            const error = this.createError(
              RangeError,
              "FIN must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_FIN"
            );
            cb(error);
            return;
          }
          if (compressed) {
            const error = this.createError(
              RangeError,
              "RSV1 must be clear",
              true,
              1002,
              "WS_ERR_UNEXPECTED_RSV_1"
            );
            cb(error);
            return;
          }
          if (this._payloadLength > 125 || this._opcode === 8 && this._payloadLength === 1) {
            const error = this.createError(
              RangeError,
              `invalid payload length ${this._payloadLength}`,
              true,
              1002,
              "WS_ERR_INVALID_CONTROL_PAYLOAD_LENGTH"
            );
            cb(error);
            return;
          }
        } else {
          const error = this.createError(
            RangeError,
            `invalid opcode ${this._opcode}`,
            true,
            1002,
            "WS_ERR_INVALID_OPCODE"
          );
          cb(error);
          return;
        }
        if (!this._fin && !this._fragmented) this._fragmented = this._opcode;
        this._masked = (buf[1] & 128) === 128;
        if (this._isServer) {
          if (!this._masked) {
            const error = this.createError(
              RangeError,
              "MASK must be set",
              true,
              1002,
              "WS_ERR_EXPECTED_MASK"
            );
            cb(error);
            return;
          }
        } else if (this._masked) {
          const error = this.createError(
            RangeError,
            "MASK must be clear",
            true,
            1002,
            "WS_ERR_UNEXPECTED_MASK"
          );
          cb(error);
          return;
        }
        if (this._payloadLength === 126) this._state = GET_PAYLOAD_LENGTH_16;
        else if (this._payloadLength === 127) this._state = GET_PAYLOAD_LENGTH_64;
        else this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+16).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength16(cb) {
        if (this._bufferedBytes < 2) {
          this._loop = false;
          return;
        }
        this._payloadLength = this.consume(2).readUInt16BE(0);
        this.haveLength(cb);
      }
      /**
       * Gets extended payload length (7+64).
       *
       * @param {Function} cb Callback
       * @private
       */
      getPayloadLength64(cb) {
        if (this._bufferedBytes < 8) {
          this._loop = false;
          return;
        }
        const buf = this.consume(8);
        const num = buf.readUInt32BE(0);
        if (num > Math.pow(2, 53 - 32) - 1) {
          const error = this.createError(
            RangeError,
            "Unsupported WebSocket frame: payload length > 2^53 - 1",
            false,
            1009,
            "WS_ERR_UNSUPPORTED_DATA_PAYLOAD_LENGTH"
          );
          cb(error);
          return;
        }
        this._payloadLength = num * Math.pow(2, 32) + buf.readUInt32BE(4);
        this.haveLength(cb);
      }
      /**
       * Payload length has been read.
       *
       * @param {Function} cb Callback
       * @private
       */
      haveLength(cb) {
        if (this._payloadLength && this._opcode < 8) {
          this._totalPayloadLength += this._payloadLength;
          if (this._totalPayloadLength > this._maxPayload && this._maxPayload > 0) {
            const error = this.createError(
              RangeError,
              "Max payload size exceeded",
              false,
              1009,
              "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
            );
            cb(error);
            return;
          }
        }
        if (this._masked) this._state = GET_MASK;
        else this._state = GET_DATA;
      }
      /**
       * Reads mask bytes.
       *
       * @private
       */
      getMask() {
        if (this._bufferedBytes < 4) {
          this._loop = false;
          return;
        }
        this._mask = this.consume(4);
        this._state = GET_DATA;
      }
      /**
       * Reads data bytes.
       *
       * @param {Function} cb Callback
       * @private
       */
      getData(cb) {
        let data = EMPTY_BUFFER;
        if (this._payloadLength) {
          if (this._bufferedBytes < this._payloadLength) {
            this._loop = false;
            return;
          }
          data = this.consume(this._payloadLength);
          if (this._masked && (this._mask[0] | this._mask[1] | this._mask[2] | this._mask[3]) !== 0) {
            unmask(data, this._mask);
          }
        }
        if (this._opcode > 7) {
          this.controlMessage(data, cb);
          return;
        }
        if (this._maxFragments > 0 && ++this._numFragments > this._maxFragments) {
          const error = this.createError(
            RangeError,
            "Too many message fragments",
            false,
            1008,
            "WS_ERR_TOO_MANY_BUFFERED_PARTS"
          );
          cb(error);
          return;
        }
        if (this._compressed) {
          this._state = INFLATING;
          this.decompress(data, cb);
          return;
        }
        if (data.length) {
          this._messageLength = this._totalPayloadLength;
          this._fragments.push(data);
        }
        this.dataMessage(cb);
      }
      /**
       * Decompresses data.
       *
       * @param {Buffer} data Compressed data
       * @param {Function} cb Callback
       * @private
       */
      decompress(data, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        perMessageDeflate.decompress(data, this._fin, (err, buf) => {
          if (err) return cb(err);
          if (buf.length) {
            this._messageLength += buf.length;
            if (this._messageLength > this._maxPayload && this._maxPayload > 0) {
              const error = this.createError(
                RangeError,
                "Max payload size exceeded",
                false,
                1009,
                "WS_ERR_UNSUPPORTED_MESSAGE_LENGTH"
              );
              cb(error);
              return;
            }
            this._fragments.push(buf);
          }
          this.dataMessage(cb);
          if (this._state === GET_INFO) this.startLoop(cb);
        });
      }
      /**
       * Handles a data message.
       *
       * @param {Function} cb Callback
       * @private
       */
      dataMessage(cb) {
        if (!this._fin) {
          this._state = GET_INFO;
          return;
        }
        const messageLength = this._messageLength;
        const fragments = this._fragments;
        this._totalPayloadLength = 0;
        this._messageLength = 0;
        this._fragmented = 0;
        this._numFragments = 0;
        this._fragments = [];
        if (this._opcode === 2) {
          let data;
          if (this._binaryType === "nodebuffer") {
            data = concat(fragments, messageLength);
          } else if (this._binaryType === "arraybuffer") {
            data = toArrayBuffer(concat(fragments, messageLength));
          } else if (this._binaryType === "blob") {
            data = new Blob(fragments);
          } else {
            data = fragments;
          }
          if (this._allowSynchronousEvents) {
            this.emit("message", data, true);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", data, true);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        } else {
          const buf = concat(fragments, messageLength);
          if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
            const error = this.createError(
              Error,
              "invalid UTF-8 sequence",
              true,
              1007,
              "WS_ERR_INVALID_UTF8"
            );
            cb(error);
            return;
          }
          if (this._state === INFLATING || this._allowSynchronousEvents) {
            this.emit("message", buf, false);
            this._state = GET_INFO;
          } else {
            this._state = DEFER_EVENT;
            setImmediate(() => {
              this.emit("message", buf, false);
              this._state = GET_INFO;
              this.startLoop(cb);
            });
          }
        }
      }
      /**
       * Handles a control message.
       *
       * @param {Buffer} data Data to handle
       * @return {(Error|RangeError|undefined)} A possible error
       * @private
       */
      controlMessage(data, cb) {
        if (this._opcode === 8) {
          if (data.length === 0) {
            this._loop = false;
            this.emit("conclude", 1005, EMPTY_BUFFER);
            this.end();
          } else {
            const code = data.readUInt16BE(0);
            if (!isValidStatusCode(code)) {
              const error = this.createError(
                RangeError,
                `invalid status code ${code}`,
                true,
                1002,
                "WS_ERR_INVALID_CLOSE_CODE"
              );
              cb(error);
              return;
            }
            const buf = new FastBuffer(
              data.buffer,
              data.byteOffset + 2,
              data.length - 2
            );
            if (!this._skipUTF8Validation && !isValidUTF8(buf)) {
              const error = this.createError(
                Error,
                "invalid UTF-8 sequence",
                true,
                1007,
                "WS_ERR_INVALID_UTF8"
              );
              cb(error);
              return;
            }
            this._loop = false;
            this.emit("conclude", code, buf);
            this.end();
          }
          this._state = GET_INFO;
          return;
        }
        if (this._allowSynchronousEvents) {
          this.emit(this._opcode === 9 ? "ping" : "pong", data);
          this._state = GET_INFO;
        } else {
          this._state = DEFER_EVENT;
          setImmediate(() => {
            this.emit(this._opcode === 9 ? "ping" : "pong", data);
            this._state = GET_INFO;
            this.startLoop(cb);
          });
        }
      }
      /**
       * Builds an error object.
       *
       * @param {function(new:Error|RangeError)} ErrorCtor The error constructor
       * @param {String} message The error message
       * @param {Boolean} prefix Specifies whether or not to add a default prefix to
       *     `message`
       * @param {Number} statusCode The status code
       * @param {String} errorCode The exposed error code
       * @return {(Error|RangeError)} The error
       * @private
       */
      createError(ErrorCtor, message, prefix, statusCode, errorCode) {
        this._loop = false;
        this._errored = true;
        const err = new ErrorCtor(
          prefix ? `Invalid WebSocket frame: ${message}` : message
        );
        Error.captureStackTrace(err, this.createError);
        err.code = errorCode;
        err[kStatusCode] = statusCode;
        return err;
      }
    };
    module.exports = Receiver2;
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/sender.js
var require_sender = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/sender.js"(exports, module) {
    "use strict";
    var { Duplex } = __require("stream");
    var { randomFillSync } = __require("crypto");
    var {
      types: { isUint8Array }
    } = __require("util");
    var PerMessageDeflate2 = require_permessage_deflate();
    var { EMPTY_BUFFER, kWebSocket, NOOP } = require_constants();
    var { isBlob, isValidStatusCode } = require_validation();
    var { mask: applyMask, toBuffer } = require_buffer_util();
    var kByteLength = Symbol("kByteLength");
    var maskBuffer = Buffer.alloc(4);
    var RANDOM_POOL_SIZE = 8 * 1024;
    var randomPool;
    var randomPoolPointer = RANDOM_POOL_SIZE;
    var DEFAULT = 0;
    var DEFLATING = 1;
    var GET_BLOB_DATA = 2;
    var Sender2 = class _Sender {
      /**
       * Creates a Sender instance.
       *
       * @param {Duplex} socket The connection socket
       * @param {Object} [extensions] An object containing the negotiated extensions
       * @param {Function} [generateMask] The function used to generate the masking
       *     key
       */
      constructor(socket, extensions, generateMask) {
        this._extensions = extensions || {};
        if (generateMask) {
          this._generateMask = generateMask;
          this._maskBuffer = Buffer.alloc(4);
        }
        this._socket = socket;
        this._firstFragment = true;
        this._compress = false;
        this._bufferedBytes = 0;
        this._queue = [];
        this._state = DEFAULT;
        this.onerror = NOOP;
        this[kWebSocket] = void 0;
      }
      /**
       * Frames a piece of data according to the HyBi WebSocket protocol.
       *
       * @param {(Buffer|String)} data The data to frame
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @return {(Buffer|String)[]} The framed data
       * @public
       */
      static frame(data, options) {
        let mask;
        let merge = false;
        let offset = 2;
        let skipMasking = false;
        if (options.mask) {
          mask = options.maskBuffer || maskBuffer;
          if (options.generateMask) {
            options.generateMask(mask);
          } else {
            if (randomPoolPointer === RANDOM_POOL_SIZE) {
              if (randomPool === void 0) {
                randomPool = Buffer.alloc(RANDOM_POOL_SIZE);
              }
              randomFillSync(randomPool, 0, RANDOM_POOL_SIZE);
              randomPoolPointer = 0;
            }
            mask[0] = randomPool[randomPoolPointer++];
            mask[1] = randomPool[randomPoolPointer++];
            mask[2] = randomPool[randomPoolPointer++];
            mask[3] = randomPool[randomPoolPointer++];
          }
          skipMasking = (mask[0] | mask[1] | mask[2] | mask[3]) === 0;
          offset = 6;
        }
        let dataLength;
        if (typeof data === "string") {
          if ((!options.mask || skipMasking) && options[kByteLength] !== void 0) {
            dataLength = options[kByteLength];
          } else {
            data = Buffer.from(data);
            dataLength = data.length;
          }
        } else {
          dataLength = data.length;
          merge = options.mask && options.readOnly && !skipMasking;
        }
        let payloadLength = dataLength;
        if (dataLength >= 65536) {
          offset += 8;
          payloadLength = 127;
        } else if (dataLength > 125) {
          offset += 2;
          payloadLength = 126;
        }
        const target = Buffer.allocUnsafe(merge ? dataLength + offset : offset);
        target[0] = options.fin ? options.opcode | 128 : options.opcode;
        if (options.rsv1) target[0] |= 64;
        target[1] = payloadLength;
        if (payloadLength === 126) {
          target.writeUInt16BE(dataLength, 2);
        } else if (payloadLength === 127) {
          target[2] = target[3] = 0;
          target.writeUIntBE(dataLength, 4, 6);
        }
        if (!options.mask) return [target, data];
        target[1] |= 128;
        target[offset - 4] = mask[0];
        target[offset - 3] = mask[1];
        target[offset - 2] = mask[2];
        target[offset - 1] = mask[3];
        if (skipMasking) return [target, data];
        if (merge) {
          applyMask(data, mask, target, offset, dataLength);
          return [target];
        }
        applyMask(data, mask, data, 0, dataLength);
        return [target, data];
      }
      /**
       * Sends a close message to the other peer.
       *
       * @param {Number} [code] The status code component of the body
       * @param {(String|Buffer)} [data] The message component of the body
       * @param {Boolean} [mask=false] Specifies whether or not to mask the message
       * @param {Function} [cb] Callback
       * @public
       */
      close(code, data, mask, cb) {
        let buf;
        if (code === void 0) {
          buf = EMPTY_BUFFER;
        } else if (typeof code !== "number" || !isValidStatusCode(code)) {
          throw new TypeError("First argument must be a valid error code number");
        } else if (data === void 0 || !data.length) {
          buf = Buffer.allocUnsafe(2);
          buf.writeUInt16BE(code, 0);
        } else {
          const length = Buffer.byteLength(data);
          if (length > 123) {
            throw new RangeError("The message must not be greater than 123 bytes");
          }
          buf = Buffer.allocUnsafe(2 + length);
          buf.writeUInt16BE(code, 0);
          if (typeof data === "string") {
            buf.write(data, 2);
          } else if (isUint8Array(data)) {
            buf.set(data, 2);
          } else {
            throw new TypeError("Second argument must be a string or a Uint8Array");
          }
        }
        const options = {
          [kByteLength]: buf.length,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 8,
          readOnly: false,
          rsv1: false
        };
        if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, buf, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(buf, options), cb);
        }
      }
      /**
       * Sends a ping message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      ping(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 9,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a pong message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Boolean} [mask=false] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback
       * @public
       */
      pong(data, mask, cb) {
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (byteLength > 125) {
          throw new RangeError("The data size must not be greater than 125 bytes");
        }
        const options = {
          [kByteLength]: byteLength,
          fin: true,
          generateMask: this._generateMask,
          mask,
          maskBuffer: this._maskBuffer,
          opcode: 10,
          readOnly,
          rsv1: false
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, false, options, cb]);
          } else {
            this.getBlobData(data, false, options, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, false, options, cb]);
        } else {
          this.sendFrame(_Sender.frame(data, options), cb);
        }
      }
      /**
       * Sends a data message to the other peer.
       *
       * @param {*} data The message to send
       * @param {Object} options Options object
       * @param {Boolean} [options.binary=false] Specifies whether `data` is binary
       *     or text
       * @param {Boolean} [options.compress=false] Specifies whether or not to
       *     compress `data`
       * @param {Boolean} [options.fin=false] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Function} [cb] Callback
       * @public
       */
      send(data, options, cb) {
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        let opcode = options.binary ? 2 : 1;
        let rsv1 = options.compress;
        let byteLength;
        let readOnly;
        if (typeof data === "string") {
          byteLength = Buffer.byteLength(data);
          readOnly = false;
        } else if (isBlob(data)) {
          byteLength = data.size;
          readOnly = false;
        } else {
          data = toBuffer(data);
          byteLength = data.length;
          readOnly = toBuffer.readOnly;
        }
        if (this._firstFragment) {
          this._firstFragment = false;
          if (rsv1 && perMessageDeflate && perMessageDeflate.params[perMessageDeflate._isServer ? "server_no_context_takeover" : "client_no_context_takeover"]) {
            rsv1 = byteLength >= perMessageDeflate._threshold;
          }
          this._compress = rsv1;
        } else {
          rsv1 = false;
          opcode = 0;
        }
        if (options.fin) this._firstFragment = true;
        const opts = {
          [kByteLength]: byteLength,
          fin: options.fin,
          generateMask: this._generateMask,
          mask: options.mask,
          maskBuffer: this._maskBuffer,
          opcode,
          readOnly,
          rsv1
        };
        if (isBlob(data)) {
          if (this._state !== DEFAULT) {
            this.enqueue([this.getBlobData, data, this._compress, opts, cb]);
          } else {
            this.getBlobData(data, this._compress, opts, cb);
          }
        } else if (this._state !== DEFAULT) {
          this.enqueue([this.dispatch, data, this._compress, opts, cb]);
        } else {
          this.dispatch(data, this._compress, opts, cb);
        }
      }
      /**
       * Gets the contents of a blob as binary data.
       *
       * @param {Blob} blob The blob
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     the data
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      getBlobData(blob, compress, options, cb) {
        this._bufferedBytes += options[kByteLength];
        this._state = GET_BLOB_DATA;
        blob.arrayBuffer().then((arrayBuffer) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while the blob was being read"
            );
            process.nextTick(callCallbacks, this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          const data = toBuffer(arrayBuffer);
          if (!compress) {
            this._state = DEFAULT;
            this.sendFrame(_Sender.frame(data, options), cb);
            this.dequeue();
          } else {
            this.dispatch(data, compress, options, cb);
          }
        }).catch((err) => {
          process.nextTick(onError, this, err, cb);
        });
      }
      /**
       * Dispatches a message.
       *
       * @param {(Buffer|String)} data The message to send
       * @param {Boolean} [compress=false] Specifies whether or not to compress
       *     `data`
       * @param {Object} options Options object
       * @param {Boolean} [options.fin=false] Specifies whether or not to set the
       *     FIN bit
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Boolean} [options.mask=false] Specifies whether or not to mask
       *     `data`
       * @param {Buffer} [options.maskBuffer] The buffer used to store the masking
       *     key
       * @param {Number} options.opcode The opcode
       * @param {Boolean} [options.readOnly=false] Specifies whether `data` can be
       *     modified
       * @param {Boolean} [options.rsv1=false] Specifies whether or not to set the
       *     RSV1 bit
       * @param {Function} [cb] Callback
       * @private
       */
      dispatch(data, compress, options, cb) {
        if (!compress) {
          this.sendFrame(_Sender.frame(data, options), cb);
          return;
        }
        const perMessageDeflate = this._extensions[PerMessageDeflate2.extensionName];
        this._bufferedBytes += options[kByteLength];
        this._state = DEFLATING;
        perMessageDeflate.compress(data, options.fin, (_, buf) => {
          if (this._socket.destroyed) {
            const err = new Error(
              "The socket was closed while data was being compressed"
            );
            callCallbacks(this, err, cb);
            return;
          }
          this._bufferedBytes -= options[kByteLength];
          this._state = DEFAULT;
          options.readOnly = false;
          this.sendFrame(_Sender.frame(buf, options), cb);
          this.dequeue();
        });
      }
      /**
       * Executes queued send operations.
       *
       * @private
       */
      dequeue() {
        while (this._state === DEFAULT && this._queue.length) {
          const params = this._queue.shift();
          this._bufferedBytes -= params[3][kByteLength];
          Reflect.apply(params[0], this, params.slice(1));
        }
      }
      /**
       * Enqueues a send operation.
       *
       * @param {Array} params Send operation parameters.
       * @private
       */
      enqueue(params) {
        this._bufferedBytes += params[3][kByteLength];
        this._queue.push(params);
      }
      /**
       * Sends a frame.
       *
       * @param {(Buffer | String)[]} list The frame to send
       * @param {Function} [cb] Callback
       * @private
       */
      sendFrame(list, cb) {
        if (list.length === 2) {
          this._socket.cork();
          this._socket.write(list[0]);
          this._socket.write(list[1], cb);
          this._socket.uncork();
        } else {
          this._socket.write(list[0], cb);
        }
      }
    };
    module.exports = Sender2;
    function callCallbacks(sender, err, cb) {
      if (typeof cb === "function") cb(err);
      for (let i = 0; i < sender._queue.length; i++) {
        const params = sender._queue[i];
        const callback = params[params.length - 1];
        if (typeof callback === "function") callback(err);
      }
    }
    function onError(sender, err, cb) {
      callCallbacks(sender, err, cb);
      sender.onerror(err);
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/event-target.js
var require_event_target = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/event-target.js"(exports, module) {
    "use strict";
    var { kForOnEventAttribute, kListener } = require_constants();
    var kCode = Symbol("kCode");
    var kData = Symbol("kData");
    var kError = Symbol("kError");
    var kMessage = Symbol("kMessage");
    var kReason = Symbol("kReason");
    var kTarget = Symbol("kTarget");
    var kType = Symbol("kType");
    var kWasClean = Symbol("kWasClean");
    var Event = class {
      /**
       * Create a new `Event`.
       *
       * @param {String} type The name of the event
       * @throws {TypeError} If the `type` argument is not specified
       */
      constructor(type) {
        this[kTarget] = null;
        this[kType] = type;
      }
      /**
       * @type {*}
       */
      get target() {
        return this[kTarget];
      }
      /**
       * @type {String}
       */
      get type() {
        return this[kType];
      }
    };
    Object.defineProperty(Event.prototype, "target", { enumerable: true });
    Object.defineProperty(Event.prototype, "type", { enumerable: true });
    var CloseEvent = class extends Event {
      /**
       * Create a new `CloseEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {Number} [options.code=0] The status code explaining why the
       *     connection was closed
       * @param {String} [options.reason=''] A human-readable string explaining why
       *     the connection was closed
       * @param {Boolean} [options.wasClean=false] Indicates whether or not the
       *     connection was cleanly closed
       */
      constructor(type, options = {}) {
        super(type);
        this[kCode] = options.code === void 0 ? 0 : options.code;
        this[kReason] = options.reason === void 0 ? "" : options.reason;
        this[kWasClean] = options.wasClean === void 0 ? false : options.wasClean;
      }
      /**
       * @type {Number}
       */
      get code() {
        return this[kCode];
      }
      /**
       * @type {String}
       */
      get reason() {
        return this[kReason];
      }
      /**
       * @type {Boolean}
       */
      get wasClean() {
        return this[kWasClean];
      }
    };
    Object.defineProperty(CloseEvent.prototype, "code", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "reason", { enumerable: true });
    Object.defineProperty(CloseEvent.prototype, "wasClean", { enumerable: true });
    var ErrorEvent = class extends Event {
      /**
       * Create a new `ErrorEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.error=null] The error that generated this event
       * @param {String} [options.message=''] The error message
       */
      constructor(type, options = {}) {
        super(type);
        this[kError] = options.error === void 0 ? null : options.error;
        this[kMessage] = options.message === void 0 ? "" : options.message;
      }
      /**
       * @type {*}
       */
      get error() {
        return this[kError];
      }
      /**
       * @type {String}
       */
      get message() {
        return this[kMessage];
      }
    };
    Object.defineProperty(ErrorEvent.prototype, "error", { enumerable: true });
    Object.defineProperty(ErrorEvent.prototype, "message", { enumerable: true });
    var MessageEvent = class extends Event {
      /**
       * Create a new `MessageEvent`.
       *
       * @param {String} type The name of the event
       * @param {Object} [options] A dictionary object that allows for setting
       *     attributes via object members of the same name
       * @param {*} [options.data=null] The message content
       */
      constructor(type, options = {}) {
        super(type);
        this[kData] = options.data === void 0 ? null : options.data;
      }
      /**
       * @type {*}
       */
      get data() {
        return this[kData];
      }
    };
    Object.defineProperty(MessageEvent.prototype, "data", { enumerable: true });
    var EventTarget = {
      /**
       * Register an event listener.
       *
       * @param {String} type A string representing the event type to listen for
       * @param {(Function|Object)} handler The listener to add
       * @param {Object} [options] An options object specifies characteristics about
       *     the event listener
       * @param {Boolean} [options.once=false] A `Boolean` indicating that the
       *     listener should be invoked at most once after being added. If `true`,
       *     the listener would be automatically removed when invoked.
       * @public
       */
      addEventListener(type, handler, options = {}) {
        for (const listener of this.listeners(type)) {
          if (!options[kForOnEventAttribute] && listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            return;
          }
        }
        let wrapper;
        if (type === "message") {
          wrapper = function onMessage(data, isBinary) {
            const event = new MessageEvent("message", {
              data: isBinary ? data : data.toString()
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "close") {
          wrapper = function onClose(code, message) {
            const event = new CloseEvent("close", {
              code,
              reason: message.toString(),
              wasClean: this._closeFrameReceived && this._closeFrameSent
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "error") {
          wrapper = function onError(error) {
            const event = new ErrorEvent("error", {
              error,
              message: error.message
            });
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else if (type === "open") {
          wrapper = function onOpen() {
            const event = new Event("open");
            event[kTarget] = this;
            callListener(handler, this, event);
          };
        } else {
          return;
        }
        wrapper[kForOnEventAttribute] = !!options[kForOnEventAttribute];
        wrapper[kListener] = handler;
        if (options.once) {
          this.once(type, wrapper);
        } else {
          this.on(type, wrapper);
        }
      },
      /**
       * Remove an event listener.
       *
       * @param {String} type A string representing the event type to remove
       * @param {(Function|Object)} handler The listener to remove
       * @public
       */
      removeEventListener(type, handler) {
        for (const listener of this.listeners(type)) {
          if (listener[kListener] === handler && !listener[kForOnEventAttribute]) {
            this.removeListener(type, listener);
            break;
          }
        }
      }
    };
    module.exports = {
      CloseEvent,
      ErrorEvent,
      Event,
      EventTarget,
      MessageEvent
    };
    function callListener(listener, thisArg, event) {
      if (typeof listener === "object" && listener.handleEvent) {
        listener.handleEvent.call(listener, event);
      } else {
        listener.call(thisArg, event);
      }
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/extension.js
var require_extension = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/extension.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function push(dest, name, elem) {
      if (dest[name] === void 0) dest[name] = [elem];
      else dest[name].push(elem);
    }
    function parse(header) {
      const offers = /* @__PURE__ */ Object.create(null);
      let params = /* @__PURE__ */ Object.create(null);
      let mustUnescape = false;
      let isEscaping = false;
      let inQuotes = false;
      let extensionName;
      let paramName;
      let start = -1;
      let code = -1;
      let end2 = -1;
      let i = 0;
      for (; i < header.length; i++) {
        code = header.charCodeAt(i);
        if (extensionName === void 0) {
          if (end2 === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (i !== 0 && (code === 32 || code === 9)) {
            if (end2 === -1 && start !== -1) end2 = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end2 === -1) end2 = i;
            const name = header.slice(start, end2);
            if (code === 44) {
              push(offers, name, params);
              params = /* @__PURE__ */ Object.create(null);
            } else {
              extensionName = name;
            }
            start = end2 = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else if (paramName === void 0) {
          if (end2 === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (code === 32 || code === 9) {
            if (end2 === -1 && start !== -1) end2 = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end2 === -1) end2 = i;
            push(params, header.slice(start, end2), true);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            start = end2 = -1;
          } else if (code === 61 && start !== -1 && end2 === -1) {
            paramName = header.slice(start, i);
            start = end2 = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        } else {
          if (isEscaping) {
            if (tokenChars[code] !== 1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (start === -1) start = i;
            else if (!mustUnescape) mustUnescape = true;
            isEscaping = false;
          } else if (inQuotes) {
            if (tokenChars[code] === 1) {
              if (start === -1) start = i;
            } else if (code === 34 && start !== -1) {
              inQuotes = false;
              end2 = i;
            } else if (code === 92) {
              isEscaping = true;
            } else {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
          } else if (code === 34 && header.charCodeAt(i - 1) === 61) {
            inQuotes = true;
          } else if (end2 === -1 && tokenChars[code] === 1) {
            if (start === -1) start = i;
          } else if (start !== -1 && (code === 32 || code === 9)) {
            if (end2 === -1) end2 = i;
          } else if (code === 59 || code === 44) {
            if (start === -1) {
              throw new SyntaxError(`Unexpected character at index ${i}`);
            }
            if (end2 === -1) end2 = i;
            let value = header.slice(start, end2);
            if (mustUnescape) {
              value = value.replace(/\\/g, "");
              mustUnescape = false;
            }
            push(params, paramName, value);
            if (code === 44) {
              push(offers, extensionName, params);
              params = /* @__PURE__ */ Object.create(null);
              extensionName = void 0;
            }
            paramName = void 0;
            start = end2 = -1;
          } else {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
        }
      }
      if (start === -1 || inQuotes || code === 32 || code === 9) {
        throw new SyntaxError("Unexpected end of input");
      }
      if (end2 === -1) end2 = i;
      const token = header.slice(start, end2);
      if (extensionName === void 0) {
        push(offers, token, params);
      } else {
        if (paramName === void 0) {
          push(params, token, true);
        } else if (mustUnescape) {
          push(params, paramName, token.replace(/\\/g, ""));
        } else {
          push(params, paramName, token);
        }
        push(offers, extensionName, params);
      }
      return offers;
    }
    function format(extensions) {
      return Object.keys(extensions).map((extension2) => {
        let configurations = extensions[extension2];
        if (!Array.isArray(configurations)) configurations = [configurations];
        return configurations.map((params) => {
          return [extension2].concat(
            Object.keys(params).map((k) => {
              let values = params[k];
              if (!Array.isArray(values)) values = [values];
              return values.map((v) => v === true ? k : `${k}=${v}`).join("; ");
            })
          ).join("; ");
        }).join(", ");
      }).join(", ");
    }
    module.exports = { format, parse };
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/websocket.js
var require_websocket = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/websocket.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var https = __require("https");
    var http = __require("http");
    var net = __require("net");
    var tls = __require("tls");
    var { randomBytes: randomBytes2, createHash } = __require("crypto");
    var { Duplex, Readable } = __require("stream");
    var { URL: URL2 } = __require("url");
    var PerMessageDeflate2 = require_permessage_deflate();
    var Receiver2 = require_receiver();
    var Sender2 = require_sender();
    var { isBlob } = require_validation();
    var {
      BINARY_TYPES,
      CLOSE_TIMEOUT,
      EMPTY_BUFFER,
      GUID,
      kForOnEventAttribute,
      kListener,
      kStatusCode,
      kWebSocket,
      NOOP
    } = require_constants();
    var {
      EventTarget: { addEventListener, removeEventListener }
    } = require_event_target();
    var { format, parse } = require_extension();
    var { toBuffer } = require_buffer_util();
    var kAborted = Symbol("kAborted");
    var protocolVersions = [8, 13];
    var readyStates = ["CONNECTING", "OPEN", "CLOSING", "CLOSED"];
    var subprotocolRegex = /^[!#$%&'*+\-.0-9A-Z^_`|a-z~]+$/;
    var WebSocket2 = class _WebSocket extends EventEmitter {
      /**
       * Create a new `WebSocket`.
       *
       * @param {(String|URL)} address The URL to which to connect
       * @param {(String|String[])} [protocols] The subprotocols
       * @param {Object} [options] Connection options
       */
      constructor(address, protocols, options) {
        super();
        this._binaryType = BINARY_TYPES[0];
        this._closeCode = 1006;
        this._closeFrameReceived = false;
        this._closeFrameSent = false;
        this._closeMessage = EMPTY_BUFFER;
        this._closeTimer = null;
        this._errorEmitted = false;
        this._extensions = {};
        this._paused = false;
        this._protocol = "";
        this._readyState = _WebSocket.CONNECTING;
        this._receiver = null;
        this._sender = null;
        this._socket = null;
        if (address !== null) {
          this._bufferedAmount = 0;
          this._isServer = false;
          this._redirects = 0;
          if (protocols === void 0) {
            if (!options || options.protocols === void 0) {
              protocols = [];
            } else if (Array.isArray(options.protocols)) {
              protocols = options.protocols;
            } else {
              protocols = [options.protocols];
            }
          } else if (!Array.isArray(protocols)) {
            if (typeof protocols === "object" && protocols !== null) {
              options = protocols;
              if (options.protocols === void 0) {
                protocols = [];
              } else if (Array.isArray(options.protocols)) {
                protocols = options.protocols;
              } else {
                protocols = [options.protocols];
              }
            } else {
              protocols = [protocols];
            }
          }
          initAsClient(this, address, protocols, options);
        } else {
          this._autoPong = options.autoPong;
          this._closeTimeout = options.closeTimeout;
          this._isServer = true;
        }
      }
      /**
       * For historical reasons, the custom "nodebuffer" type is used by the default
       * instead of "blob".
       *
       * @type {String}
       */
      get binaryType() {
        return this._binaryType;
      }
      set binaryType(type) {
        if (!BINARY_TYPES.includes(type)) return;
        this._binaryType = type;
        if (this._receiver) this._receiver._binaryType = type;
      }
      /**
       * @type {Number}
       */
      get bufferedAmount() {
        if (!this._socket) return this._bufferedAmount;
        return this._socket._writableState.length + this._sender._bufferedBytes;
      }
      /**
       * @type {String}
       */
      get extensions() {
        return Object.keys(this._extensions).join();
      }
      /**
       * @type {Boolean}
       */
      get isPaused() {
        return this._paused;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onclose() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onerror() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onopen() {
        return null;
      }
      /**
       * @type {Function}
       */
      /* istanbul ignore next */
      get onmessage() {
        return null;
      }
      /**
       * @type {String}
       */
      get protocol() {
        return this._protocol;
      }
      /**
       * @type {Number}
       */
      get readyState() {
        return this._readyState;
      }
      /**
       * @type {String}
       */
      get url() {
        return this._url;
      }
      /**
       * Set up the socket and the internal resources.
       *
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Object} options Options object
       * @param {Boolean} [options.allowSynchronousEvents=false] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Function} [options.generateMask] The function used to generate the
       *     masking key
       * @param {Number} [options.maxBufferedChunks=0] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=0] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=0] The maximum allowed message size
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @private
       */
      setSocket(socket, head, options) {
        const receiver = new Receiver2({
          allowSynchronousEvents: options.allowSynchronousEvents,
          binaryType: this.binaryType,
          extensions: this._extensions,
          isServer: this._isServer,
          maxBufferedChunks: options.maxBufferedChunks,
          maxFragments: options.maxFragments,
          maxPayload: options.maxPayload,
          skipUTF8Validation: options.skipUTF8Validation
        });
        const sender = new Sender2(socket, this._extensions, options.generateMask);
        this._receiver = receiver;
        this._sender = sender;
        this._socket = socket;
        receiver[kWebSocket] = this;
        sender[kWebSocket] = this;
        socket[kWebSocket] = this;
        receiver.on("conclude", receiverOnConclude);
        receiver.on("drain", receiverOnDrain);
        receiver.on("error", receiverOnError);
        receiver.on("message", receiverOnMessage);
        receiver.on("ping", receiverOnPing);
        receiver.on("pong", receiverOnPong);
        sender.onerror = senderOnError;
        if (socket.setTimeout) socket.setTimeout(0);
        if (socket.setNoDelay) socket.setNoDelay();
        if (head.length > 0) socket.unshift(head);
        socket.on("close", socketOnClose);
        socket.on("data", socketOnData);
        socket.on("end", socketOnEnd);
        socket.on("error", socketOnError);
        this._readyState = _WebSocket.OPEN;
        this.emit("open");
      }
      /**
       * Emit the `'close'` event.
       *
       * @private
       */
      emitClose() {
        if (!this._socket) {
          this._readyState = _WebSocket.CLOSED;
          this.emit("close", this._closeCode, this._closeMessage);
          return;
        }
        if (this._extensions[PerMessageDeflate2.extensionName]) {
          this._extensions[PerMessageDeflate2.extensionName].cleanup();
        }
        this._receiver.removeAllListeners();
        this._readyState = _WebSocket.CLOSED;
        this.emit("close", this._closeCode, this._closeMessage);
      }
      /**
       * Start a closing handshake.
       *
       *          +----------+   +-----------+   +----------+
       *     - - -|ws.close()|-->|close frame|-->|ws.close()|- - -
       *    |     +----------+   +-----------+   +----------+     |
       *          +----------+   +-----------+         |
       * CLOSING  |ws.close()|<--|close frame|<--+-----+       CLOSING
       *          +----------+   +-----------+   |
       *    |           |                        |   +---+        |
       *                +------------------------+-->|fin| - - - -
       *    |         +---+                      |   +---+
       *     - - - - -|fin|<---------------------+
       *              +---+
       *
       * @param {Number} [code] Status code explaining why the connection is closing
       * @param {(String|Buffer)} [data] The reason why the connection is
       *     closing
       * @public
       */
      close(code, data) {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this.readyState === _WebSocket.CLOSING) {
          if (this._closeFrameSent && (this._closeFrameReceived || this._receiver._writableState.errorEmitted)) {
            this._socket.end();
          }
          return;
        }
        this._sender.close(code, data, !this._isServer, (err) => {
          if (err) return;
          this._closeFrameSent = true;
          if (this._closeFrameReceived || this._receiver._writableState.errorEmitted) {
            this._socket.end();
          }
        });
        this._readyState = _WebSocket.CLOSING;
        setCloseTimer(this);
      }
      /**
       * Pause the socket.
       *
       * @public
       */
      pause() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = true;
        this._socket.pause();
      }
      /**
       * Send a ping.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the ping is sent
       * @public
       */
      ping(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.ping(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Send a pong.
       *
       * @param {*} [data] The data to send
       * @param {Boolean} [mask] Indicates whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when the pong is sent
       * @public
       */
      pong(data, mask, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof data === "function") {
          cb = data;
          data = mask = void 0;
        } else if (typeof mask === "function") {
          cb = mask;
          mask = void 0;
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        if (mask === void 0) mask = !this._isServer;
        this._sender.pong(data || EMPTY_BUFFER, mask, cb);
      }
      /**
       * Resume the socket.
       *
       * @public
       */
      resume() {
        if (this.readyState === _WebSocket.CONNECTING || this.readyState === _WebSocket.CLOSED) {
          return;
        }
        this._paused = false;
        if (!this._receiver._writableState.needDrain) this._socket.resume();
      }
      /**
       * Send a data message.
       *
       * @param {*} data The message to send
       * @param {Object} [options] Options object
       * @param {Boolean} [options.binary] Specifies whether `data` is binary or
       *     text
       * @param {Boolean} [options.compress] Specifies whether or not to compress
       *     `data`
       * @param {Boolean} [options.fin=true] Specifies whether the fragment is the
       *     last one
       * @param {Boolean} [options.mask] Specifies whether or not to mask `data`
       * @param {Function} [cb] Callback which is executed when data is written out
       * @public
       */
      send(data, options, cb) {
        if (this.readyState === _WebSocket.CONNECTING) {
          throw new Error("WebSocket is not open: readyState 0 (CONNECTING)");
        }
        if (typeof options === "function") {
          cb = options;
          options = {};
        }
        if (typeof data === "number") data = data.toString();
        if (this.readyState !== _WebSocket.OPEN) {
          sendAfterClose(this, data, cb);
          return;
        }
        const opts = {
          binary: typeof data !== "string",
          mask: !this._isServer,
          compress: true,
          fin: true,
          ...options
        };
        if (!this._extensions[PerMessageDeflate2.extensionName]) {
          opts.compress = false;
        }
        this._sender.send(data || EMPTY_BUFFER, opts, cb);
      }
      /**
       * Forcibly close the connection.
       *
       * @public
       */
      terminate() {
        if (this.readyState === _WebSocket.CLOSED) return;
        if (this.readyState === _WebSocket.CONNECTING) {
          const msg = "WebSocket was closed before the connection was established";
          abortHandshake(this, this._req, msg);
          return;
        }
        if (this._socket) {
          this._readyState = _WebSocket.CLOSING;
          this._socket.destroy();
        }
      }
    };
    Object.defineProperty(WebSocket2, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2.prototype, "CONNECTING", {
      enumerable: true,
      value: readyStates.indexOf("CONNECTING")
    });
    Object.defineProperty(WebSocket2, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2.prototype, "OPEN", {
      enumerable: true,
      value: readyStates.indexOf("OPEN")
    });
    Object.defineProperty(WebSocket2, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSING", {
      enumerable: true,
      value: readyStates.indexOf("CLOSING")
    });
    Object.defineProperty(WebSocket2, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    Object.defineProperty(WebSocket2.prototype, "CLOSED", {
      enumerable: true,
      value: readyStates.indexOf("CLOSED")
    });
    [
      "binaryType",
      "bufferedAmount",
      "extensions",
      "isPaused",
      "protocol",
      "readyState",
      "url"
    ].forEach((property) => {
      Object.defineProperty(WebSocket2.prototype, property, { enumerable: true });
    });
    ["open", "error", "close", "message"].forEach((method) => {
      Object.defineProperty(WebSocket2.prototype, `on${method}`, {
        enumerable: true,
        get() {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) return listener[kListener];
          }
          return null;
        },
        set(handler) {
          for (const listener of this.listeners(method)) {
            if (listener[kForOnEventAttribute]) {
              this.removeListener(method, listener);
              break;
            }
          }
          if (typeof handler !== "function") return;
          this.addEventListener(method, handler, {
            [kForOnEventAttribute]: true
          });
        }
      });
    });
    WebSocket2.prototype.addEventListener = addEventListener;
    WebSocket2.prototype.removeEventListener = removeEventListener;
    module.exports = WebSocket2;
    function initAsClient(websocket, address, protocols, options) {
      const opts = {
        allowSynchronousEvents: true,
        autoPong: true,
        closeTimeout: CLOSE_TIMEOUT,
        protocolVersion: protocolVersions[1],
        maxBufferedChunks: 256 * 1024,
        maxFragments: 16 * 1024,
        maxPayload: 100 * 1024 * 1024,
        skipUTF8Validation: false,
        perMessageDeflate: true,
        followRedirects: false,
        maxRedirects: 10,
        ...options,
        socketPath: void 0,
        hostname: void 0,
        protocol: void 0,
        protocols: void 0,
        timeout: void 0,
        method: "GET",
        host: void 0,
        path: void 0,
        port: void 0
      };
      websocket._autoPong = opts.autoPong;
      websocket._closeTimeout = opts.closeTimeout;
      if (!protocolVersions.includes(opts.protocolVersion)) {
        throw new RangeError(
          `Unsupported protocol version: ${opts.protocolVersion} (supported versions: ${protocolVersions.join(", ")})`
        );
      }
      let parsedUrl;
      if (address instanceof URL2) {
        parsedUrl = address;
      } else {
        try {
          parsedUrl = new URL2(address);
        } catch {
          throw new SyntaxError(`Invalid URL: ${address}`);
        }
      }
      if (parsedUrl.protocol === "http:") {
        parsedUrl.protocol = "ws:";
      } else if (parsedUrl.protocol === "https:") {
        parsedUrl.protocol = "wss:";
      }
      websocket._url = parsedUrl.href;
      const isSecure = parsedUrl.protocol === "wss:";
      const isIpcUrl = parsedUrl.protocol === "ws+unix:";
      let invalidUrlMessage;
      if (parsedUrl.protocol !== "ws:" && !isSecure && !isIpcUrl) {
        invalidUrlMessage = `The URL's protocol must be one of "ws:", "wss:", "http:", "https:", or "ws+unix:"`;
      } else if (isIpcUrl && !parsedUrl.pathname) {
        invalidUrlMessage = "The URL's pathname is empty";
      } else if (parsedUrl.hash) {
        invalidUrlMessage = "The URL contains a fragment identifier";
      }
      if (invalidUrlMessage) {
        const err = new SyntaxError(invalidUrlMessage);
        if (websocket._redirects === 0) {
          throw err;
        } else {
          emitErrorAndClose(websocket, err);
          return;
        }
      }
      const defaultPort = isSecure ? 443 : 80;
      const key = randomBytes2(16).toString("base64");
      const request = isSecure ? https.request : http.request;
      const protocolSet = /* @__PURE__ */ new Set();
      let perMessageDeflate;
      opts.createConnection = opts.createConnection || (isSecure ? tlsConnect : netConnect);
      opts.defaultPort = opts.defaultPort || defaultPort;
      opts.port = parsedUrl.port || defaultPort;
      opts.host = parsedUrl.hostname.startsWith("[") ? parsedUrl.hostname.slice(1, -1) : parsedUrl.hostname;
      opts.headers = {
        ...opts.headers,
        "Sec-WebSocket-Version": opts.protocolVersion,
        "Sec-WebSocket-Key": key,
        Connection: "Upgrade",
        Upgrade: "websocket"
      };
      opts.path = parsedUrl.pathname + parsedUrl.search;
      opts.timeout = opts.handshakeTimeout;
      if (opts.perMessageDeflate) {
        perMessageDeflate = new PerMessageDeflate2({
          ...opts.perMessageDeflate,
          isServer: false,
          maxPayload: opts.maxPayload
        });
        opts.headers["Sec-WebSocket-Extensions"] = format({
          [PerMessageDeflate2.extensionName]: perMessageDeflate.offer()
        });
      }
      if (protocols.length) {
        for (const protocol of protocols) {
          if (typeof protocol !== "string" || !subprotocolRegex.test(protocol) || protocolSet.has(protocol)) {
            throw new SyntaxError(
              "An invalid or duplicated subprotocol was specified"
            );
          }
          protocolSet.add(protocol);
        }
        opts.headers["Sec-WebSocket-Protocol"] = protocols.join(",");
      }
      if (opts.origin) {
        if (opts.protocolVersion < 13) {
          opts.headers["Sec-WebSocket-Origin"] = opts.origin;
        } else {
          opts.headers.Origin = opts.origin;
        }
      }
      if (parsedUrl.username || parsedUrl.password) {
        opts.auth = `${parsedUrl.username}:${parsedUrl.password}`;
      }
      if (isIpcUrl) {
        const parts = opts.path.split(":");
        opts.socketPath = parts[0];
        opts.path = parts[1];
      }
      let req;
      if (opts.followRedirects) {
        if (websocket._redirects === 0) {
          websocket._originalIpc = isIpcUrl;
          websocket._originalSecure = isSecure;
          websocket._originalHostOrSocketPath = isIpcUrl ? opts.socketPath : parsedUrl.host;
          const headers = options && options.headers;
          options = { ...options, headers: {} };
          if (headers) {
            for (const [key2, value] of Object.entries(headers)) {
              options.headers[key2.toLowerCase()] = value;
            }
          }
        } else if (websocket.listenerCount("redirect") === 0) {
          const isSameHost = isIpcUrl ? websocket._originalIpc ? opts.socketPath === websocket._originalHostOrSocketPath : false : websocket._originalIpc ? false : parsedUrl.host === websocket._originalHostOrSocketPath;
          if (!isSameHost || websocket._originalSecure && !isSecure) {
            delete opts.headers.authorization;
            delete opts.headers.cookie;
            if (!isSameHost) delete opts.headers.host;
            opts.auth = void 0;
          }
        }
        if (opts.auth && !options.headers.authorization) {
          options.headers.authorization = "Basic " + Buffer.from(opts.auth).toString("base64");
        }
        req = websocket._req = request(opts);
        if (websocket._redirects) {
          websocket.emit("redirect", websocket.url, req);
        }
      } else {
        req = websocket._req = request(opts);
      }
      if (opts.timeout) {
        req.on("timeout", () => {
          abortHandshake(websocket, req, "Opening handshake has timed out");
        });
      }
      req.on("error", (err) => {
        if (req === null || req[kAborted]) return;
        req = websocket._req = null;
        emitErrorAndClose(websocket, err);
      });
      req.on("response", (res) => {
        const location = res.headers.location;
        const statusCode = res.statusCode;
        if (location && opts.followRedirects && statusCode >= 300 && statusCode < 400) {
          if (++websocket._redirects > opts.maxRedirects) {
            abortHandshake(websocket, req, "Maximum redirects exceeded");
            return;
          }
          req.abort();
          let addr;
          try {
            addr = new URL2(location, address);
          } catch (e) {
            const err = new SyntaxError(`Invalid URL: ${location}`);
            emitErrorAndClose(websocket, err);
            return;
          }
          initAsClient(websocket, addr, protocols, options);
        } else if (!websocket.emit("unexpected-response", req, res)) {
          abortHandshake(
            websocket,
            req,
            `Unexpected server response: ${res.statusCode}`
          );
        }
      });
      req.on("upgrade", (res, socket, head) => {
        websocket.emit("upgrade", res);
        if (websocket.readyState !== WebSocket2.CONNECTING) return;
        req = websocket._req = null;
        const upgrade = res.headers.upgrade;
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          abortHandshake(websocket, socket, "Invalid Upgrade header");
          return;
        }
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        if (res.headers["sec-websocket-accept"] !== digest) {
          abortHandshake(websocket, socket, "Invalid Sec-WebSocket-Accept header");
          return;
        }
        const serverProt = res.headers["sec-websocket-protocol"];
        let protError;
        if (serverProt !== void 0) {
          if (!protocolSet.size) {
            protError = "Server sent a subprotocol but none was requested";
          } else if (!protocolSet.has(serverProt)) {
            protError = "Server sent an invalid subprotocol";
          }
        } else if (protocolSet.size) {
          protError = "Server sent no subprotocol";
        }
        if (protError) {
          abortHandshake(websocket, socket, protError);
          return;
        }
        if (serverProt) websocket._protocol = serverProt;
        const secWebSocketExtensions = res.headers["sec-websocket-extensions"];
        if (secWebSocketExtensions !== void 0) {
          if (!perMessageDeflate) {
            const message = "Server sent a Sec-WebSocket-Extensions header but no extension was requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          let extensions;
          try {
            extensions = parse(secWebSocketExtensions);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          const extensionNames = Object.keys(extensions);
          if (extensionNames.length !== 1 || extensionNames[0] !== PerMessageDeflate2.extensionName) {
            const message = "Server indicated an extension that was not requested";
            abortHandshake(websocket, socket, message);
            return;
          }
          try {
            perMessageDeflate.accept(extensions[PerMessageDeflate2.extensionName]);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Extensions header";
            abortHandshake(websocket, socket, message);
            return;
          }
          websocket._extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
        }
        websocket.setSocket(socket, head, {
          allowSynchronousEvents: opts.allowSynchronousEvents,
          generateMask: opts.generateMask,
          maxBufferedChunks: opts.maxBufferedChunks,
          maxFragments: opts.maxFragments,
          maxPayload: opts.maxPayload,
          skipUTF8Validation: opts.skipUTF8Validation
        });
      });
      if (opts.finishRequest) {
        opts.finishRequest(req, websocket);
      } else {
        req.end();
      }
    }
    function emitErrorAndClose(websocket, err) {
      websocket._readyState = WebSocket2.CLOSING;
      websocket._errorEmitted = true;
      websocket.emit("error", err);
      websocket.emitClose();
    }
    function netConnect(options) {
      options.path = options.socketPath;
      return net.connect(options);
    }
    function tlsConnect(options) {
      options.path = void 0;
      if (!options.servername && options.servername !== "") {
        options.servername = net.isIP(options.host) ? "" : options.host;
      }
      return tls.connect(options);
    }
    function abortHandshake(websocket, stream, message) {
      websocket._readyState = WebSocket2.CLOSING;
      const err = new Error(message);
      Error.captureStackTrace(err, abortHandshake);
      if (stream.setHeader) {
        stream[kAborted] = true;
        stream.abort();
        if (stream.socket && !stream.socket.destroyed) {
          stream.socket.destroy();
        }
        process.nextTick(emitErrorAndClose, websocket, err);
      } else {
        stream.destroy(err);
        stream.once("error", websocket.emit.bind(websocket, "error"));
        stream.once("close", websocket.emitClose.bind(websocket));
      }
    }
    function sendAfterClose(websocket, data, cb) {
      if (data) {
        const length = isBlob(data) ? data.size : toBuffer(data).length;
        if (websocket._socket) websocket._sender._bufferedBytes += length;
        else websocket._bufferedAmount += length;
      }
      if (cb) {
        const err = new Error(
          `WebSocket is not open: readyState ${websocket.readyState} (${readyStates[websocket.readyState]})`
        );
        process.nextTick(cb, err);
      }
    }
    function receiverOnConclude(code, reason) {
      const websocket = this[kWebSocket];
      websocket._closeFrameReceived = true;
      websocket._closeMessage = reason;
      websocket._closeCode = code;
      if (websocket._socket[kWebSocket] === void 0) return;
      websocket._socket.removeListener("data", socketOnData);
      process.nextTick(resume, websocket._socket);
      if (code === 1005) websocket.close();
      else websocket.close(code, reason);
    }
    function receiverOnDrain() {
      const websocket = this[kWebSocket];
      if (!websocket.isPaused) websocket._socket.resume();
    }
    function receiverOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket._socket[kWebSocket] !== void 0) {
        websocket._socket.removeListener("data", socketOnData);
        process.nextTick(resume, websocket._socket);
        websocket.close(err[kStatusCode]);
      }
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function receiverOnFinish() {
      this[kWebSocket].emitClose();
    }
    function receiverOnMessage(data, isBinary) {
      this[kWebSocket].emit("message", data, isBinary);
    }
    function receiverOnPing(data) {
      const websocket = this[kWebSocket];
      if (websocket._autoPong) websocket.pong(data, !this._isServer, NOOP);
      websocket.emit("ping", data);
    }
    function receiverOnPong(data) {
      this[kWebSocket].emit("pong", data);
    }
    function resume(stream) {
      stream.resume();
    }
    function senderOnError(err) {
      const websocket = this[kWebSocket];
      if (websocket.readyState === WebSocket2.CLOSED) return;
      if (websocket.readyState === WebSocket2.OPEN) {
        websocket._readyState = WebSocket2.CLOSING;
        setCloseTimer(websocket);
      }
      this._socket.end();
      if (!websocket._errorEmitted) {
        websocket._errorEmitted = true;
        websocket.emit("error", err);
      }
    }
    function setCloseTimer(websocket) {
      websocket._closeTimer = setTimeout(
        websocket._socket.destroy.bind(websocket._socket),
        websocket._closeTimeout
      );
    }
    function socketOnClose() {
      const websocket = this[kWebSocket];
      this.removeListener("close", socketOnClose);
      this.removeListener("data", socketOnData);
      this.removeListener("end", socketOnEnd);
      websocket._readyState = WebSocket2.CLOSING;
      if (!this._readableState.endEmitted && !websocket._closeFrameReceived && !websocket._receiver._writableState.errorEmitted && this._readableState.length !== 0) {
        const chunk = this.read(this._readableState.length);
        websocket._receiver.write(chunk);
      }
      websocket._receiver.end();
      this[kWebSocket] = void 0;
      clearTimeout(websocket._closeTimer);
      if (websocket._receiver._writableState.finished || websocket._receiver._writableState.errorEmitted) {
        websocket.emitClose();
      } else {
        websocket._receiver.on("error", receiverOnFinish);
        websocket._receiver.on("finish", receiverOnFinish);
      }
    }
    function socketOnData(chunk) {
      if (!this[kWebSocket]._receiver.write(chunk)) {
        this.pause();
      }
    }
    function socketOnEnd() {
      const websocket = this[kWebSocket];
      websocket._readyState = WebSocket2.CLOSING;
      websocket._receiver.end();
      this.end();
    }
    function socketOnError() {
      const websocket = this[kWebSocket];
      this.removeListener("error", socketOnError);
      this.on("error", NOOP);
      if (websocket) {
        websocket._readyState = WebSocket2.CLOSING;
        this.destroy();
      }
    }
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/stream.js
var require_stream = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/stream.js"(exports, module) {
    "use strict";
    var WebSocket2 = require_websocket();
    var { Duplex } = __require("stream");
    function emitClose(stream) {
      stream.emit("close");
    }
    function duplexOnEnd() {
      if (!this.destroyed && this._writableState.finished) {
        this.destroy();
      }
    }
    function duplexOnError(err) {
      this.removeListener("error", duplexOnError);
      this.destroy();
      if (this.listenerCount("error") === 0) {
        this.emit("error", err);
      }
    }
    function createWebSocketStream2(ws, options) {
      let terminateOnDestroy = true;
      const duplex = new Duplex({
        ...options,
        autoDestroy: false,
        emitClose: false,
        objectMode: false,
        writableObjectMode: false
      });
      ws.on("message", function message(msg, isBinary) {
        const data = !isBinary && duplex._readableState.objectMode ? msg.toString() : msg;
        if (!duplex.push(data)) ws.pause();
      });
      ws.once("error", function error(err) {
        if (duplex.destroyed) return;
        terminateOnDestroy = false;
        duplex.destroy(err);
      });
      ws.once("close", function close() {
        if (duplex.destroyed) return;
        duplex.push(null);
      });
      duplex._destroy = function(err, callback) {
        if (ws.readyState === ws.CLOSED) {
          callback(err);
          process.nextTick(emitClose, duplex);
          return;
        }
        let called = false;
        ws.once("error", function error(err2) {
          called = true;
          callback(err2);
        });
        ws.once("close", function close() {
          if (!called) callback(err);
          process.nextTick(emitClose, duplex);
        });
        if (terminateOnDestroy) ws.terminate();
      };
      duplex._final = function(callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._final(callback);
          });
          return;
        }
        if (ws._socket === null) return;
        if (ws._socket._writableState.finished) {
          callback();
          if (duplex._readableState.endEmitted) duplex.destroy();
        } else {
          ws._socket.once("finish", function finish() {
            callback();
          });
          ws.close();
        }
      };
      duplex._read = function() {
        if (ws.isPaused) ws.resume();
      };
      duplex._write = function(chunk, encoding, callback) {
        if (ws.readyState === ws.CONNECTING) {
          ws.once("open", function open() {
            duplex._write(chunk, encoding, callback);
          });
          return;
        }
        ws.send(chunk, callback);
      };
      duplex.on("end", duplexOnEnd);
      duplex.on("error", duplexOnError);
      return duplex;
    }
    module.exports = createWebSocketStream2;
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/subprotocol.js
var require_subprotocol = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/subprotocol.js"(exports, module) {
    "use strict";
    var { tokenChars } = require_validation();
    function parse(header) {
      const protocols = /* @__PURE__ */ new Set();
      let start = -1;
      let end2 = -1;
      let i = 0;
      for (i; i < header.length; i++) {
        const code = header.charCodeAt(i);
        if (end2 === -1 && tokenChars[code] === 1) {
          if (start === -1) start = i;
        } else if (i !== 0 && (code === 32 || code === 9)) {
          if (end2 === -1 && start !== -1) end2 = i;
        } else if (code === 44) {
          if (start === -1) {
            throw new SyntaxError(`Unexpected character at index ${i}`);
          }
          if (end2 === -1) end2 = i;
          const protocol2 = header.slice(start, end2);
          if (protocols.has(protocol2)) {
            throw new SyntaxError(`The "${protocol2}" subprotocol is duplicated`);
          }
          protocols.add(protocol2);
          start = end2 = -1;
        } else {
          throw new SyntaxError(`Unexpected character at index ${i}`);
        }
      }
      if (start === -1 || end2 !== -1) {
        throw new SyntaxError("Unexpected end of input");
      }
      const protocol = header.slice(start, i);
      if (protocols.has(protocol)) {
        throw new SyntaxError(`The "${protocol}" subprotocol is duplicated`);
      }
      protocols.add(protocol);
      return protocols;
    }
    module.exports = { parse };
  }
});

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/websocket-server.js
var require_websocket_server = __commonJS({
  "../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/lib/websocket-server.js"(exports, module) {
    "use strict";
    var EventEmitter = __require("events");
    var http = __require("http");
    var { Duplex } = __require("stream");
    var { createHash } = __require("crypto");
    var extension2 = require_extension();
    var PerMessageDeflate2 = require_permessage_deflate();
    var subprotocol2 = require_subprotocol();
    var WebSocket2 = require_websocket();
    var { CLOSE_TIMEOUT, GUID, kWebSocket } = require_constants();
    var keyRegex = /^[+/0-9A-Za-z]{22}==$/;
    var RUNNING = 0;
    var CLOSING = 1;
    var CLOSED = 2;
    var WebSocketServer2 = class extends EventEmitter {
      /**
       * Create a `WebSocketServer` instance.
       *
       * @param {Object} options Configuration options
       * @param {Boolean} [options.allowSynchronousEvents=true] Specifies whether
       *     any of the `'message'`, `'ping'`, and `'pong'` events can be emitted
       *     multiple times in the same tick
       * @param {Boolean} [options.autoPong=true] Specifies whether or not to
       *     automatically send a pong in response to a ping
       * @param {Number} [options.backlog=511] The maximum length of the queue of
       *     pending connections
       * @param {Boolean} [options.clientTracking=true] Specifies whether or not to
       *     track clients
       * @param {Number} [options.closeTimeout=30000] Duration in milliseconds to
       *     wait for the closing handshake to finish after `websocket.close()` is
       *     called
       * @param {Function} [options.handleProtocols] A hook to handle protocols
       * @param {String} [options.host] The hostname where to bind the server
       * @param {Number} [options.maxBufferedChunks=262144] The maximum number of
       *     buffered data chunks
       * @param {Number} [options.maxFragments=16384] The maximum number of message
       *     fragments
       * @param {Number} [options.maxPayload=104857600] The maximum allowed message
       *     size
       * @param {Boolean} [options.noServer=false] Enable no server mode
       * @param {String} [options.path] Accept only connections matching this path
       * @param {(Boolean|Object)} [options.perMessageDeflate=false] Enable/disable
       *     permessage-deflate
       * @param {Number} [options.port] The port where to bind the server
       * @param {(http.Server|https.Server)} [options.server] A pre-created HTTP/S
       *     server to use
       * @param {Boolean} [options.skipUTF8Validation=false] Specifies whether or
       *     not to skip UTF-8 validation for text and close messages
       * @param {Function} [options.verifyClient] A hook to reject connections
       * @param {Function} [options.WebSocket=WebSocket] Specifies the `WebSocket`
       *     class to use. It must be the `WebSocket` class or class that extends it
       * @param {Function} [callback] A listener for the `listening` event
       */
      constructor(options, callback) {
        super();
        options = {
          allowSynchronousEvents: true,
          autoPong: true,
          maxBufferedChunks: 256 * 1024,
          maxFragments: 16 * 1024,
          maxPayload: 100 * 1024 * 1024,
          skipUTF8Validation: false,
          perMessageDeflate: false,
          handleProtocols: null,
          clientTracking: true,
          closeTimeout: CLOSE_TIMEOUT,
          verifyClient: null,
          noServer: false,
          backlog: null,
          // use default (511 as implemented in net.js)
          server: null,
          host: null,
          path: null,
          port: null,
          WebSocket: WebSocket2,
          ...options
        };
        if (options.port == null && !options.server && !options.noServer || options.port != null && (options.server || options.noServer) || options.server && options.noServer) {
          throw new TypeError(
            'One and only one of the "port", "server", or "noServer" options must be specified'
          );
        }
        if (options.port != null) {
          this._server = http.createServer((req, res) => {
            const body = http.STATUS_CODES[426];
            res.writeHead(426, {
              "Content-Length": body.length,
              "Content-Type": "text/plain"
            });
            res.end(body);
          });
          this._server.listen(
            options.port,
            options.host,
            options.backlog,
            callback
          );
        } else if (options.server) {
          this._server = options.server;
        }
        if (this._server) {
          const emitConnection = this.emit.bind(this, "connection");
          this._removeListeners = addListeners(this._server, {
            listening: this.emit.bind(this, "listening"),
            error: this.emit.bind(this, "error"),
            upgrade: (req, socket, head) => {
              this.handleUpgrade(req, socket, head, emitConnection);
            }
          });
        }
        if (options.perMessageDeflate === true) options.perMessageDeflate = {};
        if (options.clientTracking) {
          this.clients = /* @__PURE__ */ new Set();
          this._shouldEmitClose = false;
        }
        this.options = options;
        this._state = RUNNING;
      }
      /**
       * Returns the bound address, the address family name, and port of the server
       * as reported by the operating system if listening on an IP socket.
       * If the server is listening on a pipe or UNIX domain socket, the name is
       * returned as a string.
       *
       * @return {(Object|String|null)} The address of the server
       * @public
       */
      address() {
        if (this.options.noServer) {
          throw new Error('The server is operating in "noServer" mode');
        }
        if (!this._server) return null;
        return this._server.address();
      }
      /**
       * Stop the server from accepting new connections and emit the `'close'` event
       * when all existing connections are closed.
       *
       * @param {Function} [cb] A one-time listener for the `'close'` event
       * @public
       */
      close(cb) {
        if (this._state === CLOSED) {
          if (cb) {
            this.once("close", () => {
              cb(new Error("The server is not running"));
            });
          }
          process.nextTick(emitClose, this);
          return;
        }
        if (cb) this.once("close", cb);
        if (this._state === CLOSING) return;
        this._state = CLOSING;
        if (this.options.noServer || this.options.server) {
          if (this._server) {
            this._removeListeners();
            this._removeListeners = this._server = null;
          }
          if (this.clients) {
            if (!this.clients.size) {
              process.nextTick(emitClose, this);
            } else {
              this._shouldEmitClose = true;
            }
          } else {
            process.nextTick(emitClose, this);
          }
        } else {
          const server2 = this._server;
          this._removeListeners();
          this._removeListeners = this._server = null;
          server2.close(() => {
            emitClose(this);
          });
        }
      }
      /**
       * See if a given request should be handled by this server instance.
       *
       * @param {http.IncomingMessage} req Request object to inspect
       * @return {Boolean} `true` if the request is valid, else `false`
       * @public
       */
      shouldHandle(req) {
        if (this.options.path) {
          const index = req.url.indexOf("?");
          const pathname = index !== -1 ? req.url.slice(0, index) : req.url;
          if (pathname !== this.options.path) return false;
        }
        return true;
      }
      /**
       * Handle a HTTP Upgrade request.
       *
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @public
       */
      handleUpgrade(req, socket, head, cb) {
        socket.on("error", socketOnError);
        const key = req.headers["sec-websocket-key"];
        const upgrade = req.headers.upgrade;
        const version = +req.headers["sec-websocket-version"];
        if (req.method !== "GET") {
          const message = "Invalid HTTP method";
          abortHandshakeOrEmitwsClientError(this, req, socket, 405, message);
          return;
        }
        if (upgrade === void 0 || upgrade.toLowerCase() !== "websocket") {
          const message = "Invalid Upgrade header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (key === void 0 || !keyRegex.test(key)) {
          const message = "Missing or invalid Sec-WebSocket-Key header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
          return;
        }
        if (version !== 13 && version !== 8) {
          const message = "Missing or invalid Sec-WebSocket-Version header";
          abortHandshakeOrEmitwsClientError(this, req, socket, 400, message, {
            "Sec-WebSocket-Version": "13, 8"
          });
          return;
        }
        if (!this.shouldHandle(req)) {
          abortHandshake(socket, 400);
          return;
        }
        const secWebSocketProtocol = req.headers["sec-websocket-protocol"];
        let protocols = /* @__PURE__ */ new Set();
        if (secWebSocketProtocol !== void 0) {
          try {
            protocols = subprotocol2.parse(secWebSocketProtocol);
          } catch (err) {
            const message = "Invalid Sec-WebSocket-Protocol header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        const secWebSocketExtensions = req.headers["sec-websocket-extensions"];
        const extensions = {};
        if (this.options.perMessageDeflate && secWebSocketExtensions !== void 0) {
          const perMessageDeflate = new PerMessageDeflate2({
            ...this.options.perMessageDeflate,
            isServer: true,
            maxPayload: this.options.maxPayload
          });
          try {
            const offers = extension2.parse(secWebSocketExtensions);
            if (offers[PerMessageDeflate2.extensionName]) {
              perMessageDeflate.accept(offers[PerMessageDeflate2.extensionName]);
              extensions[PerMessageDeflate2.extensionName] = perMessageDeflate;
            }
          } catch (err) {
            const message = "Invalid or unacceptable Sec-WebSocket-Extensions header";
            abortHandshakeOrEmitwsClientError(this, req, socket, 400, message);
            return;
          }
        }
        if (this.options.verifyClient) {
          const info = {
            origin: req.headers[`${version === 8 ? "sec-websocket-origin" : "origin"}`],
            secure: !!(req.socket.authorized || req.socket.encrypted),
            req
          };
          if (this.options.verifyClient.length === 2) {
            this.options.verifyClient(info, (verified, code, message, headers) => {
              if (!verified) {
                return abortHandshake(socket, code || 401, message, headers);
              }
              this.completeUpgrade(
                extensions,
                key,
                protocols,
                req,
                socket,
                head,
                cb
              );
            });
            return;
          }
          if (!this.options.verifyClient(info)) return abortHandshake(socket, 401);
        }
        this.completeUpgrade(extensions, key, protocols, req, socket, head, cb);
      }
      /**
       * Upgrade the connection to WebSocket.
       *
       * @param {Object} extensions The accepted extensions
       * @param {String} key The value of the `Sec-WebSocket-Key` header
       * @param {Set} protocols The subprotocols
       * @param {http.IncomingMessage} req The request object
       * @param {Duplex} socket The network socket between the server and client
       * @param {Buffer} head The first packet of the upgraded stream
       * @param {Function} cb Callback
       * @throws {Error} If called more than once with the same socket
       * @private
       */
      completeUpgrade(extensions, key, protocols, req, socket, head, cb) {
        if (!socket.readable || !socket.writable) return socket.destroy();
        if (socket[kWebSocket]) {
          throw new Error(
            "server.handleUpgrade() was called more than once with the same socket, possibly due to a misconfiguration"
          );
        }
        if (this._state > RUNNING) return abortHandshake(socket, 503);
        const digest = createHash("sha1").update(key + GUID).digest("base64");
        const headers = [
          "HTTP/1.1 101 Switching Protocols",
          "Upgrade: websocket",
          "Connection: Upgrade",
          `Sec-WebSocket-Accept: ${digest}`
        ];
        const ws = new this.options.WebSocket(null, void 0, this.options);
        if (protocols.size) {
          const protocol = this.options.handleProtocols ? this.options.handleProtocols(protocols, req) : protocols.values().next().value;
          if (protocol) {
            headers.push(`Sec-WebSocket-Protocol: ${protocol}`);
            ws._protocol = protocol;
          }
        }
        if (extensions[PerMessageDeflate2.extensionName]) {
          const params = extensions[PerMessageDeflate2.extensionName].params;
          const value = extension2.format({
            [PerMessageDeflate2.extensionName]: [params]
          });
          headers.push(`Sec-WebSocket-Extensions: ${value}`);
          ws._extensions = extensions;
        }
        this.emit("headers", headers, req);
        socket.write(headers.concat("\r\n").join("\r\n"));
        socket.removeListener("error", socketOnError);
        ws.setSocket(socket, head, {
          allowSynchronousEvents: this.options.allowSynchronousEvents,
          maxBufferedChunks: this.options.maxBufferedChunks,
          maxFragments: this.options.maxFragments,
          maxPayload: this.options.maxPayload,
          skipUTF8Validation: this.options.skipUTF8Validation
        });
        if (this.clients) {
          this.clients.add(ws);
          ws.on("close", () => {
            this.clients.delete(ws);
            if (this._shouldEmitClose && !this.clients.size) {
              process.nextTick(emitClose, this);
            }
          });
        }
        cb(ws, req);
      }
    };
    module.exports = WebSocketServer2;
    function addListeners(server2, map) {
      for (const event of Object.keys(map)) server2.on(event, map[event]);
      return function removeListeners() {
        for (const event of Object.keys(map)) {
          server2.removeListener(event, map[event]);
        }
      };
    }
    function emitClose(server2) {
      server2._state = CLOSED;
      server2.emit("close");
    }
    function socketOnError() {
      this.destroy();
    }
    function abortHandshake(socket, code, message, headers) {
      message = message || http.STATUS_CODES[code];
      headers = {
        Connection: "close",
        "Content-Type": "text/html",
        "Content-Length": Buffer.byteLength(message),
        ...headers
      };
      socket.once("finish", socket.destroy);
      socket.end(
        `HTTP/1.1 ${code} ${http.STATUS_CODES[code]}\r
` + Object.keys(headers).map((h) => `${h}: ${headers[h]}`).join("\r\n") + "\r\n\r\n" + message
      );
    }
    function abortHandshakeOrEmitwsClientError(server2, req, socket, code, message, headers) {
      if (server2.listenerCount("wsClientError")) {
        const err = new Error(message);
        Error.captureStackTrace(err, abortHandshakeOrEmitwsClientError);
        server2.emit("wsClientError", err, socket, req);
      } else {
        abortHandshake(socket, code, message, headers);
      }
    }
  }
});

// src/main.ts
import { existsSync, readFileSync as readFileSync2 } from "node:fs";
import { createServer } from "node:http";
import path2 from "node:path";
import { fileURLToPath } from "node:url";

// ../../packages/online/src/protocol.ts
var PROTOCOL_VERSION = 2;
var MAX_SLOTS = 4;
var MAX_PLAYERS = 8;
var CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
var CODE_LENGTH = 6;
var ROOM_TTL_MS = 24 * 60 * 60 * 1e3;
var MAX_MESSAGE_BYTES = 16 * 1024;
var CODE_RE = new RegExp(`^[${CODE_ALPHABET}]{${CODE_LENGTH}}$`);
var isRoomCode = (s) => CODE_RE.test(s);
var checkOf = (g) => ({ turn: g.turn, current: g.current, rng: g.rngState });
var isBotSlot = (s) => !!s && (!s.owner || !!s.away);
var DIRS = /* @__PURE__ */ new Set([1, 2, 4, 8]);
var FIGHT_OPTIONS = /* @__PURE__ */ new Set(["weaponId", "spellId", "castHeilzauber", "useWuchtschlag", "useZweiterSchlag", "useZielsicher", "useGlueckspilz", "useSchmetterling", "useAusweichen"]);
var WORLD_IDS = /* @__PURE__ */ new Set(["wald", "wueste", "schloss"]);
var SIZES = /* @__PURE__ */ new Set(["kurz", "normal", "lang"]);
var isObj = (x) => typeof x === "object" && x !== null && !Array.isArray(x);
var isInt = (x, min = -1e6, max = 1e6) => Number.isInteger(x) && x >= min && x <= max;
var optBool = (x) => x === void 0 || typeof x === "boolean";
var optStr = (x, max = 40) => x === void 0 || typeof x === "string" && x.length <= max;
function isWorldId(x) {
  return typeof x === "string" && WORLD_IDS.has(x);
}
function isGameSize(x) {
  return typeof x === "string" && SIZES.has(x);
}
function isCommand(x) {
  if (!isObj(x) || typeof x.type !== "string") return false;
  switch (x.type) {
    case "move":
      return DIRS.has(x.dir) && (x.passBy === void 0 || x.passBy === "schleicher" || x.passBy === "sturmangriff");
    case "place":
      return x.rotation === void 0 || isInt(x.rotation, 0, 3);
    case "fight": {
      if (x.options === void 0) return true;
      if (!isObj(x.options)) return false;
      return Object.entries(x.options).every(([k, v]) => FIGHT_OPTIONS.has(k) && (k === "weaponId" || k === "spellId" ? optStr(v) : optBool(v)));
    }
    case "flee":
      return optBool(x.useAusweichen);
    case "fairy_ring":
    case "cast_waldpfad":
      return isInt(x.x, -500, 500) && isInt(x.y, -500, 500);
    case "cast_sturmboee":
      return DIRS.has(x.dir);
    case "redraw":
    case "use_herb":
    case "open_chest":
    case "healing_hands":
    case "peek":
    case "end_turn":
      return true;
    default:
      return false;
  }
}
function parseClientMessage(raw) {
  if (raw.length > MAX_MESSAGE_BYTES) return void 0;
  let m;
  try {
    m = JSON.parse(raw);
  } catch {
    return void 0;
  }
  if (!isObj(m) || typeof m.t !== "string") return void 0;
  const companion = (c) => c === void 0 || typeof c === "string" && c.length <= 20;
  switch (m.t) {
    case "hello":
      return isInt(m.v, 0, 1e3) && typeof m.token === "string" && /^[A-Za-z0-9_-]{16,64}$/.test(m.token) ? m : void 0;
    case "claim":
      return isInt(m.hero, 0, 63) && companion(m.companionId) ? m : void 0;
    case "pick":
      return isInt(m.slot, 0, MAX_SLOTS - 1) && isInt(m.hero, 0, 63) && companion(m.companionId) ? m : void 0;
    case "release":
      return isInt(m.slot, 0, MAX_SLOTS - 1) ? m : void 0;
    case "bot":
      return isInt(m.hero, 0, 63) ? m : void 0;
    case "order":
      return isInt(m.slot, 0, MAX_SLOTS - 1) && (m.dir === -1 || m.dir === 1) ? m : void 0;
    case "settings":
      return (m.world === void 0 || isWorldId(m.world)) && (m.size === void 0 || isGameSize(m.size)) ? m : void 0;
    case "cmd":
      return isInt(m.seq, 0, 1e9) && isCommand(m.cmd) ? m : void 0;
    case "takeover":
      return isInt(m.slot, 0, MAX_SLOTS - 1) && typeof m.bot === "boolean" ? m : void 0;
    case "start":
    case "resync":
    case "again":
    case "leave":
    case "ping":
      return { t: m.t };
    default:
      return void 0;
  }
}

// ../../packages/content/src/dice.ts
var d = (count, sides, plus = 0) => ({ count, sides, plus });
function diceLabel(spec) {
  return `${spec.count}W${spec.sides}${spec.plus ? `+${spec.plus}` : ""}`;
}
function diceAverage(spec) {
  return spec.count * ((spec.sides + 1) / 2) + (spec.plus ?? 0);
}

// ../../packages/content/src/abilities.ts
var ABILITIES = [
  { id: "schleicher", name: "Schleicher", kind: "erkundung", cost: 2, description: "Einmal pro Zug an einem Monster vorbeigehen, ohne zu k\xE4mpfen." },
  { id: "sturmangriff", name: "Sturmangriff", kind: "erkundung", cost: 1, description: "Durch ein Monsterfeld hindurchziehen f\xFCr 1 Aktion und 2 LP." },
  { id: "fernblick", name: "Fernblick", kind: "erkundung", cost: 1, description: "Einmal pro Zug ein Nachbarpl\xE4ttchen ansehen, bevor man hinzieht." },
  { id: "schnelllaeufer", name: "Schnelll\xE4ufer", kind: "erkundung", cost: 3, description: "5 statt 4 Aktionen pro Zug." },
  { id: "schatzjaeger", name: "Schatzj\xE4ger", kind: "erkundung", cost: 1, description: "Einmal pro Spiel eine Truhe ohne Schl\xFCssel \xF6ffnen." },
  { id: "wildniskenner", name: "Wildniskenner", kind: "erkundung", cost: 1, description: "Einmal pro Spiel ein gezogenes Pl\xE4ttchen zur\xFCcklegen und neu ziehen." },
  { id: "zaeh", name: "Z\xE4h", kind: "erkundung", cost: 1, description: "+3 Lebenspunkte." },
  { id: "heilende_haende", name: "Heilende H\xE4nde", kind: "erkundung", cost: 2, description: "Einmal pro Spiel alle LP zur\xFCck, jederzeit." },
  { id: "tierfreund", name: "Tierfreund", kind: "erkundung", cost: 2, description: "Zu Spielbeginn werden zwei Begleiter ausgew\xFCrfelt, du w\xE4hlst einen davon." },
  { id: "wuchtschlag", name: "Wuchtschlag", kind: "kampf", cost: 2, description: "Einmal pro Kampf: ein Treffer macht doppelten Schaden." },
  { id: "zweiter_schlag", name: "Zweiter Schlag", kind: "kampf", cost: 2, description: "Einmal pro Kampf: in einer Runde zweimal angreifen." },
  { id: "ausweichen", name: "Ausweichen", kind: "kampf", cost: 2, description: "Einmal pro Kampf: einen Treffer des Monsters ignorieren." },
  { id: "zielsicher", name: "Zielsicher", kind: "kampf", cost: 2, description: "Einmal pro Kampf: einen verfehlten Angriff neu w\xFCrfeln." },
  { id: "heilzauber", name: "Heilzauber", kind: "kampf", cost: 2, description: "Einmal pro Kampf: statt anzugreifen 1W6 + Magie LP heilen." },
  { id: "kriegsschrei", name: "Kriegsschrei", kind: "kampf", cost: 1, description: "Das Monster hat den ganzen Kampf \u22122 auf seine Angriffe." },
  { id: "waffenmeister", name: "Waffenmeister", kind: "kampf", cost: 2, description: "+1 Schaden mit jeder Waffe." },
  { id: "glueckspilz", name: "Gl\xFCckspilz", kind: "kampf", cost: 1, description: "Einmal pro Kampf eine nat\xFCrliche 1 neu w\xFCrfeln." }
];
var ABILITY_BY_ID = Object.fromEntries(
  ABILITIES.map((a) => [a.id, a])
);

// ../../packages/content/src/monsters.ts
var MONSTERS = [
  { id: "stachelmaus", name: "Stachelmaus-Rudel", armor: 10, hp: 3, attack: 1, damage: d(1, 4), loot: "heilkraut", count: 5 },
  { id: "pilzkobold", name: "Pilzkobold", armor: 11, hp: 4, attack: 2, damage: d(1, 4), loot: "zauber", count: 5 },
  { id: "wildkeiler", name: "Wildkeiler", armor: 12, hp: 6, attack: 2, damage: d(1, 6), loot: "waffe", count: 5 },
  { id: "netzspinne", name: "Netzspinne", armor: 12, hp: 6, attack: 2, damage: d(1, 6), loot: "schluessel", count: 4 },
  { id: "irrlicht", name: "Irrlicht", armor: 14, hp: 4, attack: 3, damage: d(1, 4), loot: "zauber", count: 4 },
  { id: "dornenwolf", name: "Dornenwolf", armor: 13, hp: 7, attack: 3, damage: d(1, 6), loot: "waffe", count: 4 },
  { id: "sumpftroll", name: "Sumpftroll", armor: 12, hp: 10, attack: 3, damage: d(1, 8), loot: "splitter", count: 3 },
  { id: "moosgolem", name: "Moosgolem", armor: 14, hp: 9, attack: 3, damage: d(1, 6), loot: "truhe", count: 3 },
  { id: "nebelhexe", name: "Nebelhexe", armor: 13, hp: 9, attack: 4, damage: d(1, 6, 1), loot: "splitter", count: 2 },
  { id: "dornenkoenig", name: "Dornenk\xF6nig", armor: 17, hp: 20, attack: 7, damage: d(1, 10), loot: "kristallherz", count: 1, boss: true }
];
function reskin(names) {
  return MONSTERS.map((m, i) => ({ ...m, id: names[i][0], name: names[i][1] }));
}
var DESERT_MONSTERS = reskin([
  ["springmaus", "Springmaus-Bande"],
  ["kaktuskobold", "Kaktuskobold"],
  ["sandwaran", "Sandwaran"],
  ["skorpion", "Riesenskorpion"],
  ["hitzegeist", "Hitzegeist"],
  ["wuestenschakal", "W\xFCstenschakal"],
  ["duenenwurm", "D\xFCnenwurm"],
  ["sandsteingolem", "Sandsteingolem"],
  ["sturmdschinn", "Sturmdschinn"],
  ["glutsphinx", "Glutsphinx"]
]);
var CASTLE_MONSTERS = reskin([
  ["fledermaeuse", "Fledermaus-Schwarm"],
  ["kerzenkobold", "Kerzenkobold"],
  ["polterkiste", "Polterkiste"],
  ["kerkerratte", "Kerkerratte"],
  ["schlossgespenst", "Schlossgespenst"],
  ["spukruestung", "Spukr\xFCstung"],
  ["kerkertroll", "Kerkertroll"],
  ["wasserspeier", "Wasserspeier"],
  ["spiegelhexe", "Spiegelhexe"],
  ["burgdrache", "Burgdrache"]
]);
var ALL_MONSTERS = [...MONSTERS, ...DESERT_MONSTERS, ...CASTLE_MONSTERS];
var MONSTER_BY_ID = Object.fromEntries(ALL_MONSTERS.map((m) => [m.id, m]));
var MONSTER_SPLITTER_ONE_IN = 4;
var FINALE_ROUNDS_BY_HEROES = [12, 12, 10, 8, 7];
function finaleRounds(heroCount) {
  return FINALE_ROUNDS_BY_HEROES[Math.min(4, Math.max(1, heroCount))];
}

// ../../packages/content/src/items.ts
var FISTS = { id: "faeuste", name: "F\xE4uste", attribute: "staerke", damage: d(1, 4), count: 0 };
var WEAPONS = [
  { id: "wanderstock", name: "Wanderstock", attribute: "staerke", damage: d(1, 6), count: 4 },
  { id: "waldaxt", name: "Waldaxt", attribute: "staerke", damage: d(1, 8), count: 3 },
  { id: "elfenbogen", name: "Elfenbogen", attribute: "geschick", damage: d(1, 6, 1), count: 2 },
  { id: "kristallstab", name: "Kristallstab", attribute: "magie", damage: d(1, 8), count: 2 }
];
var STARTER_WEAPONS = [
  { id: "schleuder", name: "Schleuder", attribute: "geschick", damage: d(1, 6), count: 0 },
  { id: "zauberstab", name: "Zauberstab", attribute: "magie", damage: d(1, 6), count: 0 }
];
var STARTER_WEAPON_BY_ATTRIBUTE = { staerke: "wanderstock", geschick: "schleuder", magie: "zauberstab" };
var WEAPON_BY_ID = Object.fromEntries([FISTS, ...WEAPONS, ...STARTER_WEAPONS].map((w) => [w.id, w]));
var SPELLS = [
  { id: "funkenzauber", name: "Funkenzauber", count: 4, effect: { type: "damage", dice: d(1, 6) }, description: "Trifft automatisch: 1W6 + Magie Schaden." },
  { id: "sonnenstrahl", name: "Sonnenstrahl", count: 2, effect: { type: "damage", dice: d(2, 6) }, description: "Trifft automatisch: 2W6 + Magie Schaden." },
  { id: "sturmboee", name: "Sturmb\xF6e", count: 2, effect: { type: "push" }, description: "Schiebt ein Monster auf ein benachbartes leeres Pl\xE4ttchen." },
  { id: "waldpfad", name: "Waldpfad", count: 2, effect: { type: "teleport_spring" }, description: "Versetzt den Helden zu einer aufgedeckten Quelle." },
  { id: "nebelmantel", name: "Nebelmantel", count: 2, effect: { type: "monster_misses", rounds: 2 }, description: "Das Monster verfehlt automatisch f\xFCr 2 Kampfrunden." }
];
var SPELL_BY_ID = Object.fromEntries(SPELLS.map((s) => [s.id, s]));
var MAX_WEAPONS = 2;
var MAX_SPELLS = 3;
var MAX_HERBS = 2;
var HERB_HEAL = 5;

// ../../packages/content/src/tiles.ts
var N = 1;
var E = 2;
var S = 4;
var W = 8;
var TILES = [
  { id: "pfad_gerade", kind: "pfad", exits: N | S, count: 4 },
  { id: "pfad_kurve", kind: "pfad", exits: N | E, count: 4 },
  { id: "pfad_t", kind: "pfad", exits: N | E | S, count: 7 },
  { id: "pfad_kreuz", kind: "pfad", exits: N | E | S | W, count: 6 },
  { id: "lichtung_2", kind: "lichtung", exits: N | S, count: 5 },
  { id: "lichtung_2k", kind: "lichtung", exits: N | E, count: 5 },
  { id: "lichtung_3", kind: "lichtung", exits: N | E | S, count: 8 },
  { id: "lichtung_4", kind: "lichtung", exits: N | E | S | W, count: 6 },
  { id: "quelle", kind: "quelle", exits: N | E | S, count: 3 },
  { id: "feenring", kind: "feenring", exits: N | S, count: 5 },
  { id: "dickicht", kind: "dickicht", exits: N, count: 4 },
  { id: "dornenherz", kind: "dornenherz", exits: N | S, count: 1 }
];
var START_TILE = { id: "start", kind: "start", exits: N | E | S | W, count: 1 };
var DICKICHT_SPLITTER_ONE_IN = 2;
var GAME_SIZES = { kurz: 35, normal: 58, lang: 80 };

// ../../packages/content/src/heroes.ts
var attr = (staerke, geschick, magie) => ({ staerke, geschick, magie });
var PRESET_HEROES = [
  { name: "Fenna", title: "die Waldl\xE4uferin", attributes: attr(1, 3, 0), abilities: ["fernblick", "schnelllaeufer"], portrait: "fenna", color: "#3a8f4a" },
  { name: "Bruno", title: "der B\xE4renh\xFCter", attributes: attr(3, 1, 0), abilities: ["zaeh", "kriegsschrei"], portrait: "bruno", color: "#8b5a2b" },
  { name: "Lilo", title: "die Pilzhexe", attributes: attr(0, 1, 3), abilities: ["heilzauber", "zielsicher"], portrait: "lilo", color: "#c73e3e" },
  { name: "Kiro", title: "der Fuchsdieb", attributes: attr(1, 2, 1), abilities: ["schleicher", "schatzjaeger"], portrait: "kiro", color: "#e07b2a" },
  { name: "Ondra", title: "der Quellenh\xFCter", attributes: attr(1, 1, 2), abilities: ["heilende_haende", "ausweichen"], portrait: "ondra", color: "#2f7fb8" },
  { name: "Mira", title: "die Tierfreundin", attributes: attr(1, 1, 2), abilities: ["tierfreund", "zweiter_schlag"], portrait: "mira", color: "#8e5bb5" },
  { name: "Jonas", title: "der Kristallritter", attributes: attr(2, 1, 1), abilities: ["sturmangriff", "wuchtschlag"], portrait: "jonas", color: "#d9a21b" },
  { name: "Leonie", title: "die Sternensch\xFCtzin", attributes: attr(0, 2, 2), abilities: ["zielsicher", "glueckspilz"], portrait: "leonie", color: "#d6548f" }
];
var BASE_HP = 12;
var ZAEH_BONUS_HP = 3;
var BASE_ARMOR = 10;
var BASE_ACTIONS = 4;
var COMBAT_ACTIONS_PER_TURN = 0;

// ../../packages/content/src/companions.ts
var COMPANIONS = [
  { id: "fuchs", name: "Fuchs", abilityName: "Listig", description: "Flucht aus einem Kampf ohne freien Angriff des Monsters.", effect: { type: "free_flee" }, unlock: { type: "start" } },
  { id: "eule", name: "Eule", abilityName: "Weitblick", description: "Einmal pro Zug ein Nachbarpl\xE4ttchen ansehen, bevor du hinziehst.", effect: { type: "peek_tile" }, unlock: { type: "start" } },
  { id: "igel", name: "Igel", abilityName: "Stachelpanzer", description: "+1 R\xFCstung.", effect: { type: "armor", bonus: 1 }, unlock: { type: "start" } },
  { id: "frosch", name: "Frosch", abilityName: "Heilzunge", description: "Am Ende jedes Zuges 1 LP heilen.", effect: { type: "regen", hp: 1 }, unlock: { type: "start" } },
  { id: "schmetterling", name: "Schmetterling", abilityName: "Gl\xFCcksfl\xFCgel", description: "Einmal pro Spiel einen beliebigen W\xFCrfelwurf wiederholen.", effect: { type: "reroll_once_per_game" }, unlock: { type: "start" } },
  { id: "wolfswelpe", name: "Wolfswelpe", abilityName: "Biss", description: "Greift jede Kampfrunde zus\xE4tzlich an: W20 gegen RW, 1W4 Schaden.", effect: { type: "attack", bonus: 0, damage: d(1, 4) }, unlock: { type: "monsters_defeated", count: 10 } },
  { id: "rabe", name: "Rabe", abilityName: "Diebisch", description: "Hat ein besiegtes Monster eine Zauberrolle als Beute, bekommst du eine zweite.", effect: { type: "extra_spell_loot" }, unlock: { type: "games_won", count: 3 } },
  { id: "waldbaer", name: "Waldb\xE4r", abilityName: "B\xE4renkraft", description: "+1 Schaden mit Nahkampfwaffen.", effect: { type: "melee_damage", bonus: 1 }, unlock: { type: "boss_defeated", count: 1 } },
  { id: "gluehwuermchen", name: "Gl\xFChw\xFCrmchen-Schwarm", abilityName: "Leuchten", description: "Monster haben gegen dich \u22121 R\xFCstung.", effect: { type: "monster_armor", malus: 1 }, unlock: { type: "games_played", count: 5 } },
  { id: "salamander", name: "Salamander", abilityName: "Feueratem", description: "Schadenszauber machen +2 Schaden.", effect: { type: "spell_damage", bonus: 2 }, unlock: { type: "spells_cast", count: 10 } }
];
var COMPANION_BY_ID = Object.fromEntries(
  COMPANIONS.map((c) => [c.id, c])
);
var EMPTY_STATS = { monstersDefeated: 0, gamesWon: 0, gamesPlayed: 0, bossDefeated: 0, spellsCast: 0 };

// ../../packages/content/src/worlds.ts
var bossOf = (list) => list.find((m) => m.boss);
var WORLDS = [
  {
    id: "wald",
    name: "Dornenwald",
    title: "Der Dornenwald",
    short: "Wald",
    intro: "Moosige Pfade, Feenringe und der Dornenk\xF6nig.",
    monsters: MONSTERS,
    boss: bossOf(MONSTERS),
    tileNames: { pfad: "Pfad", lichtung: "Lichtung", quelle: "Quelle", feenring: "Feenring", dickicht: "Dickicht", dornenherz: "Dornenherz", start: "Startlichtung" },
    tileFlavor: {
      pfad: "Ein stiller Waldweg. Hier ist es ruhig.",
      lichtung: "Eine Lichtung \u2026 hier lauert ein Monster!",
      quelle: "Eine Quelle! Wer hier seinen Zug beendet, wird ganz geheilt.",
      feenring: "Ein Feenring! Von hier springst du zu jedem anderen Feenring.",
      dickicht: "Dichtes Gestr\xFCpp, eine Sackgasse. Manchmal glitzert hier ein Splitter.",
      dornenherz: "Das Dornenherz \u2026 hier schl\xE4ft der Dornenk\xF6nig!",
      start: "Die Startlichtung."
    },
    saved: "Der Wald ist gerettet!",
    explored: "Der Wald ist erkundet.",
    icon: "\u{1F332}",
    bossNom: "der Dornenk\xF6nig",
    bossAcc: "den Dornenk\xF6nig",
    bossDat: "dem Dornenk\xF6nig",
    bossHe: "er",
    bossHim: "ihm",
    acc: "den Wald",
    gen: "des Waldes",
    itemNames: {},
    herbIcon: "\u{1F33F}",
    spring: { the: "die Quelle", toOne: "zu einer Quelle", aGlowing: "eine leuchtende Quelle", theGlowing: "die leuchtende Quelle" }
  },
  {
    id: "wueste",
    name: "Glutw\xFCste",
    title: "Die Glutw\xFCste",
    short: "W\xFCste",
    intro: "Hei\xDFe D\xFCnen, Oasen und die Glutsphinx.",
    monsters: DESERT_MONSTERS,
    boss: bossOf(DESERT_MONSTERS),
    tileNames: { pfad: "Sandweg", lichtung: "Ruinenplatz", quelle: "Oase", feenring: "Sandwirbel", dickicht: "D\xFCnenkamm", dornenherz: "Sonnentempel", start: "Karawanenlager" },
    tileFlavor: {
      pfad: "Ein Weg aus festem Sand zwischen den D\xFCnen.",
      lichtung: "Alte S\xE4ulen im Sand \u2026 hier lauert ein Monster!",
      quelle: "Eine Oase! Wer hier seinen Zug beendet, wird ganz geheilt.",
      feenring: "Ein Sandwirbel! Von hier springst du zu jedem anderen Sandwirbel.",
      dickicht: "Ein hoher D\xFCnenkamm, eine Sackgasse. Manchmal glitzert hier ein Splitter.",
      dornenherz: "Der Sonnentempel \u2026 hier schl\xE4ft die Glutsphinx!",
      start: "Das Lager der Karawane."
    },
    saved: "Die W\xFCste ist gerettet!",
    explored: "Die W\xFCste ist erkundet.",
    icon: "\u{1F3DC}\uFE0F",
    bossNom: "die Glutsphinx",
    bossAcc: "die Glutsphinx",
    bossDat: "der Glutsphinx",
    bossHe: "sie",
    bossHim: "ihr",
    acc: "die W\xFCste",
    gen: "der W\xFCste",
    itemNames: {
      heilkraut: "W\xFCstenkraut",
      schluessel: "Sonnenschl\xFCssel",
      truhe: "Sandsteintruhe",
      waldaxt: "W\xFCstenaxt",
      elfenbogen: "Falkenbogen",
      waldpfad: "Oasenpfad",
      nebelmantel: "Sandschleier"
    },
    herbIcon: "\u{1F335}",
    spring: { the: "die Oase", toOne: "zu einer Oase", aGlowing: "eine leuchtende Oase", theGlowing: "die leuchtende Oase" }
  },
  {
    id: "schloss",
    name: "Mitternachtsburg",
    title: "Die Mitternachtsburg",
    short: "Burg",
    intro: "Lange G\xE4nge, Zauberspiegel und der Burgdrache.",
    monsters: CASTLE_MONSTERS,
    boss: bossOf(CASTLE_MONSTERS),
    tileNames: { pfad: "Gang", lichtung: "Saal", quelle: "Brunnen", feenring: "Zauberspiegel", dickicht: "Rumpelkammer", dornenherz: "Thronsaal", start: "Burgtor" },
    tileFlavor: {
      pfad: "Ein langer Gang mit rotem Teppich.",
      lichtung: "Ein gro\xDFer Saal \u2026 hier lauert ein Monster!",
      quelle: "Ein Brunnen! Wer hier seinen Zug beendet, wird ganz geheilt.",
      feenring: "Ein Zauberspiegel! Durch ihn springst du zu jedem anderen Spiegel.",
      dickicht: "Eine Rumpelkammer, eine Sackgasse. Manchmal glitzert hier ein Splitter.",
      dornenherz: "Der Thronsaal \u2026 hier schl\xE4ft der Burgdrache!",
      start: "Das gro\xDFe Burgtor."
    },
    saved: "Die Burg ist gerettet!",
    explored: "Die Burg ist erkundet.",
    icon: "\u{1F3F0}",
    bossNom: "der Burgdrache",
    bossAcc: "den Burgdrachen",
    bossDat: "dem Burgdrachen",
    bossHe: "er",
    bossHim: "ihm",
    acc: "die Burg",
    gen: "der Burg",
    itemNames: {
      heilkraut: "Heiltr\xE4nkchen",
      schluessel: "Kerkerschl\xFCssel",
      truhe: "Eisentruhe",
      waldaxt: "Streitaxt",
      elfenbogen: "Armbrust",
      waldpfad: "Geheimgang",
      sonnenstrahl: "Mondstrahl",
      nebelmantel: "Schattenmantel"
    },
    herbIcon: "\u{1F9EA}",
    spring: { the: "der Brunnen", toOne: "zu einem Brunnen", aGlowing: "einen leuchtenden Brunnen", theGlowing: "den leuchtenden Brunnen" }
  }
];
var WORLD_BY_ID = Object.fromEntries(WORLDS.map((w) => [w.id, w]));
var worldOf = (id) => WORLD_BY_ID[id ?? "wald"];

// ../../packages/rules/src/rng.ts
function createRng(seed) {
  let a = seed >>> 0;
  const next = () => {
    a = a + 1831565813 >>> 0;
    let t = a;
    t = Math.imul(t ^ t >>> 15, t | 1);
    t ^= t + Math.imul(t ^ t >>> 7, t | 61);
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
  return {
    int: (min, max) => min + Math.floor(next() * (max - min + 1)),
    d6: () => 1 + Math.floor(next() * 6),
    shuffle: (items) => {
      const copy = [...items];
      for (let i = copy.length - 1; i > 0; i--) {
        const j = Math.floor(next() * (i + 1));
        [copy[i], copy[j]] = [copy[j], copy[i]];
      }
      return copy;
    },
    state: () => a
  };
}

// ../../packages/rules/src/companions.ts
function unlockedCompanions(stats) {
  return COMPANIONS.filter((c) => {
    const u = c.unlock;
    switch (u.type) {
      case "start":
        return true;
      case "monsters_defeated":
        return stats.monstersDefeated >= u.count;
      case "games_won":
        return stats.gamesWon >= u.count;
      case "games_played":
        return stats.gamesPlayed >= u.count;
      case "boss_defeated":
        return stats.bossDefeated >= u.count;
      case "spells_cast":
        return stats.spellsCast >= u.count;
    }
  });
}
function rollCompanions(stats, rng, count = 1) {
  const pool = rng.shuffle(unlockedCompanions(stats));
  return pool.slice(0, Math.min(count, pool.length));
}
function companionEffect(companion, type) {
  if (!companion || companion.effect.type !== type) return void 0;
  return companion.effect;
}

// ../../packages/rules/src/hero.ts
function hasAbility(hero, id) {
  return hero.abilities.includes(id);
}
function maxHp(hero) {
  return BASE_HP + (hasAbility(hero, "zaeh") ? ZAEH_BONUS_HP : 0);
}
function heroArmor(hero, companion) {
  return BASE_ARMOR + hero.attributes.geschick + (companionEffect(companion, "armor")?.bonus ?? 0);
}
function actionsPerTurn(hero) {
  return BASE_ACTIONS + (hasAbility(hero, "schnelllaeufer") ? 1 : 0);
}

// ../../packages/rules/src/combat.ts
function rollDice(spec, rng) {
  let sum = spec.plus ?? 0;
  for (let i = 0; i < spec.count; i++) sum += rng.int(1, spec.sides);
  return sum;
}
function startCombat(hero, monster, heroHp, monsterHp = monster.hp, companion) {
  const state = {
    hero,
    companion,
    monster,
    heroHp,
    monsterHp,
    round: 0,
    used: /* @__PURE__ */ new Set(),
    monsterMissRounds: 0,
    monsterAttackPenalty: 0,
    log: [],
    status: "running"
  };
  if (hasAbility(hero, "kriegsschrei")) {
    state.monsterAttackPenalty = 2;
    state.log.push({ type: "ability", id: "kriegsschrei", note: "Monster hat \u22122 auf Angriffe" });
  }
  return state;
}
function monsterArmor(state) {
  return state.monster.armor - (companionEffect(state.companion, "monster_armor")?.malus ?? 0);
}
function weaponOf(hero, weaponId) {
  return weaponId && WEAPON_BY_ID[weaponId] || FISTS;
}
function attackRoll(bonus, target, rng) {
  const roll = rng.int(1, 20);
  const crit = roll === 20;
  const fumble = roll === 1;
  const hit = crit || !fumble && roll + bonus >= target;
  return { roll, hit, crit, fumble };
}
function dealToMonster(state, who, dice, flat, doubled, rng) {
  let amount = rollDice(dice, rng) + flat;
  if (doubled) amount *= 2;
  amount = Math.max(1, amount);
  state.monsterHp = Math.max(0, state.monsterHp - amount);
  state.log.push({ type: "damage", who, dice: diceLabel(dice), amount, doubled });
  if (state.monsterHp === 0) end(state, "won");
}
function end(state, status) {
  state.status = status;
  state.log.push({ type: "end", status });
}
function beginRound(state) {
  state.round += 1;
  state.log.push({ type: "round", round: state.round });
}
function heroAction(state, opts, rng) {
  if (state.status !== "running") return;
  const { hero, monster } = state;
  if (opts.castHeilzauber && hasAbility(hero, "heilzauber") && !state.used.has("heilzauber")) {
    state.used.add("heilzauber");
    const amount = rng.int(1, 6) + hero.attributes.magie;
    state.heroHp = Math.min(maxHp(hero), state.heroHp + amount);
    state.log.push({ type: "heal", amount });
  } else if (opts.castSpell) {
    const amount = rollDice(opts.castSpell.dice, rng) + hero.attributes.magie + (companionEffect(state.companion, "spell_damage")?.bonus ?? 0);
    state.log.push({ type: "spell", id: opts.castSpell.id, amount });
    state.monsterHp = Math.max(0, state.monsterHp - amount);
    if (state.monsterHp === 0) end(state, "won");
  } else if (opts.castNebelmantel) {
    state.monsterMissRounds = Math.max(state.monsterMissRounds, opts.castNebelmantel.rounds);
    state.log.push({ type: "spell", id: "nebelmantel", amount: 0 });
  } else {
    const swings = opts.useZweiterSchlag && hasAbility(hero, "zweiter_schlag") && !state.used.has("zweiter_schlag") ? 2 : 1;
    if (swings === 2) {
      state.used.add("zweiter_schlag");
      state.log.push({ type: "ability", id: "zweiter_schlag", note: "zwei Angriffe" });
    }
    for (let i = 0; i < swings && state.status === "running"; i++) heroSwing(state, opts, rng);
  }
  const bite = companionEffect(state.companion, "attack");
  if (state.status === "running" && bite) {
    const target = monsterArmor(state);
    const r = attackRoll(bite.bonus, target, rng);
    state.log.push({ type: "attack", who: "companion", bonus: bite.bonus, target, ...r });
    if (r.hit) dealToMonster(state, "companion", bite.damage, 0, r.crit, rng);
  }
}
function heroSwing(state, opts, rng) {
  const { hero } = state;
  const weapon = weaponOf(hero, opts.weaponId);
  const bonus = hero.attributes[weapon.attribute];
  const armor = monsterArmor(state);
  let r = attackRoll(bonus, armor, rng);
  state.log.push({ type: "attack", who: "hero", bonus, target: armor, ...r });
  if (r.fumble && opts.useGlueckspilz && hasAbility(hero, "glueckspilz") && !state.used.has("glueckspilz")) {
    state.used.add("glueckspilz");
    state.log.push({ type: "ability", id: "glueckspilz", note: "Patzer wird neu gew\xFCrfelt" });
    r = attackRoll(bonus, armor, rng);
    state.log.push({ type: "attack", who: "hero", bonus, target: armor, ...r });
  }
  if (!r.hit && opts.useZielsicher && hasAbility(hero, "zielsicher") && !state.used.has("zielsicher")) {
    state.used.add("zielsicher");
    state.log.push({ type: "ability", id: "zielsicher", note: "Fehlschlag wird neu gew\xFCrfelt" });
    r = attackRoll(bonus, armor, rng);
    state.log.push({ type: "attack", who: "hero", bonus, target: armor, ...r });
  }
  if (!r.hit && opts.extraReroll && !state.used.has("schmetterling")) {
    state.used.add("schmetterling");
    state.log.push({ type: "ability", id: "schmetterling", note: "Gl\xFCcksfl\xFCgel: Wurf wird wiederholt" });
    r = attackRoll(bonus, armor, rng);
    state.log.push({ type: "attack", who: "hero", bonus, target: armor, ...r });
  }
  if (!r.hit) return;
  let doubled = r.crit;
  if (opts.useWuchtschlag && hasAbility(hero, "wuchtschlag") && !state.used.has("wuchtschlag")) {
    state.used.add("wuchtschlag");
    state.log.push({ type: "ability", id: "wuchtschlag", note: "doppelter Schaden" });
    doubled = true;
  }
  let flat = hasAbility(hero, "waffenmeister") ? 1 : 0;
  if (weapon.attribute === "staerke") flat += companionEffect(state.companion, "melee_damage")?.bonus ?? 0;
  dealToMonster(state, "hero", weapon.damage, flat, doubled, rng);
}
function monsterAttack(state, rng, useAusweichen = false) {
  if (state.status !== "running") return;
  const { hero, monster } = state;
  const target = heroArmor(hero, state.companion);
  if (state.monsterMissRounds > 0) {
    state.monsterMissRounds -= 1;
    state.log.push({ type: "attack", who: "monster", roll: 0, bonus: 0, target, hit: false, crit: false, fumble: false });
    return;
  }
  const bonus = monster.attack - state.monsterAttackPenalty;
  const r = attackRoll(bonus, target, rng);
  state.log.push({ type: "attack", who: "monster", bonus, target, ...r });
  if (!r.hit) return;
  if (useAusweichen && hasAbility(hero, "ausweichen") && !state.used.has("ausweichen")) {
    state.used.add("ausweichen");
    state.log.push({ type: "ability", id: "ausweichen", note: "Treffer ignoriert" });
    return;
  }
  let amount = rollDice(monster.damage, rng);
  if (r.crit) amount *= 2;
  state.heroHp = Math.max(0, state.heroHp - amount);
  state.log.push({ type: "damage", who: "monster", dice: diceLabel(monster.damage), amount, doubled: r.crit });
  if (state.heroHp === 0) end(state, "lost");
}
function flee(state, rng, useAusweichen = false) {
  if (state.status !== "running") return;
  if (companionEffect(state.companion, "free_flee")) {
    state.log.push({ type: "ability", id: "fuchs", note: "Flucht ohne freien Angriff" });
  } else {
    monsterAttack(state, rng, useAusweichen);
  }
  if (state.status === "running") end(state, "fled");
}
function hitProbability(bonus, armor) {
  let hits = 0;
  for (let roll = 1; roll <= 20; roll++) if (roll === 20 || roll !== 1 && roll + bonus >= armor) hits++;
  return hits / 20;
}

// ../../packages/rules/src/board.ts
var DIRS2 = [N, E, S, W];
var DIR_NAME = { [N]: "N", [E]: "O", [S]: "S", [W]: "W" };
var DELTA = {
  [N]: { dx: 0, dy: -1 },
  [E]: { dx: 1, dy: 0 },
  [S]: { dx: 0, dy: 1 },
  [W]: { dx: -1, dy: 0 }
};
function opposite(dir) {
  return (dir << 2 | dir >> 2) & 15;
}
function step(x, y, dir) {
  const d2 = DELTA[dir];
  return { x: x + d2.dx, y: y + d2.dy };
}
var posKey = (x, y) => `${x},${y}`;
function rotateExits(exits, quarterTurns) {
  const r = (quarterTurns % 4 + 4) % 4;
  if (r === 0) return exits & 15;
  return (exits << r | exits >> 4 - r) & 15;
}
function hasExit(exits, dir) {
  return (exits & dir) !== 0;
}
function tileAt(board, x, y) {
  return board[posKey(x, y)];
}
function connected(board, x, y, dir) {
  const from = tileAt(board, x, y);
  if (!from || !hasExit(from.exits, dir)) return false;
  const p = step(x, y, dir);
  const to = tileAt(board, p.x, p.y);
  return !!to && hasExit(to.exits, opposite(dir));
}
function unexploredExits(board, x, y) {
  const t = tileAt(board, x, y);
  if (!t) return [];
  return DIRS2.filter((d2) => hasExit(t.exits, d2) && !tileAt(board, step(x, y, d2).x, step(x, y, d2).y));
}
function fittingRotations(board, shape, x, y, fromDir) {
  const scored = [];
  for (let rotation = 0; rotation < 4; rotation++) {
    const exits = rotateExits(shape.exits, rotation);
    if (!hasExit(exits, fromDir)) continue;
    let score = 0;
    for (const d2 of DIRS2) {
      if (!hasExit(exits, d2) || d2 === fromDir) continue;
      const p = step(x, y, d2);
      const n = tileAt(board, p.x, p.y);
      if (!n) score += 1;
      else if (hasExit(n.exits, opposite(d2))) score += 2;
      else score -= 2;
    }
    scored.push({ rotation, score });
  }
  return scored.sort((a, b) => b.score - a.score || a.rotation - b.rotation).map((s) => s.rotation);
}
var DICKICHT_FREE_TOP = 8;
function buildTileStack(size, rng) {
  const target = GAME_SIZES[size];
  const heart = TILES.find((t) => t.kind === "dornenherz");
  if (!heart) throw new Error("Kein Dornenherz-Pl\xE4ttchen in den Inhalten");
  const scale = target / GAME_SIZES.normal;
  const special = (t) => Math.max(t.kind === "feenring" ? 2 : 1, Math.round(t.count * scale));
  let pool = [];
  for (const t of TILES) {
    if (t.kind === "dornenherz") continue;
    const n = t.kind === "pfad" || t.kind === "lichtung" ? t.count : special(t);
    for (let i = 0; i < n; i++) pool.push(t);
  }
  const isFiller = (t) => t.kind === "pfad" || t.kind === "lichtung";
  const withoutHeart = target - 1;
  if (pool.length > withoutHeart) {
    const fillers = rng.shuffle(pool.map((t, i) => isFiller(t) ? i : -1).filter((i) => i >= 0));
    const remove = new Set(fillers.slice(0, pool.length - withoutHeart));
    pool = pool.filter((_, i) => !remove.has(i));
  } else if (pool.length < withoutHeart) {
    const fillers = pool.filter(isFiller);
    while (pool.length < withoutHeart) pool.push(fillers[rng.int(0, fillers.length - 1)]);
  }
  const stack = rng.shuffle(pool.filter((t) => t.kind !== "dickicht"));
  const dickichte = pool.filter((t) => t.kind === "dickicht");
  for (const t of dickichte) stack.splice(rng.int(0, Math.max(0, stack.length - DICKICHT_FREE_TOP)), 0, t);
  const third = Math.max(1, Math.floor(stack.length / 3));
  stack.splice(rng.int(0, third - 1), 0, heart);
  return stack;
}
function placeTile(board, shape, x, y, rotation) {
  const tile = { x, y, shapeId: shape.id, kind: shape.kind, exits: rotateExits(shape.exits, rotation), chests: 0 };
  board[posKey(x, y)] = tile;
  return tile;
}
function pathStep(board, x, y, isTarget, canPass) {
  const start = posKey(x, y);
  const visited = /* @__PURE__ */ new Set([start]);
  const queue = [{ x, y, first: void 0, distance: 0 }];
  while (queue.length) {
    const cur = queue.shift();
    for (const dir of DIRS2) {
      if (!connected(board, cur.x, cur.y, dir)) continue;
      const p = step(cur.x, cur.y, dir);
      const k = posKey(p.x, p.y);
      if (visited.has(k)) continue;
      visited.add(k);
      const tile = tileAt(board, p.x, p.y);
      const first = cur.first ?? dir;
      if (isTarget(tile)) return { dir: first, distance: cur.distance + 1 };
      if (canPass(tile)) queue.push({ x: p.x, y: p.y, first, distance: cur.distance + 1 });
    }
  }
  return void 0;
}

// ../../packages/rules/src/state.ts
var RuleError = class extends Error {
  constructor(code, message) {
    super(message ?? code);
    this.code = code;
    this.name = "RuleError";
  }
};

// ../../packages/rules/src/loot.ts
function buildWeaponPool(rng) {
  const ids = [];
  for (const w of WEAPONS) for (let i = 0; i < w.count; i++) ids.push(w.id);
  return rng.shuffle(ids);
}
function buildSpellPool(rng) {
  const ids = [];
  for (const s of SPELLS) for (let i = 0; i < s.count; i++) ids.push(s.id);
  return rng.shuffle(ids);
}
function weaponValue(hero, weapon) {
  return hitProbability(hero.hero.attributes[weapon.attribute], 13) * diceAverage(weapon.damage);
}
function bestWeaponId(hero) {
  let best;
  for (const id of hero.weapons) {
    const w = WEAPON_BY_ID[id];
    if (!w) continue;
    const value = weaponValue(hero, w);
    if (!best || value > best.value) best = { id, value };
  }
  return best?.id;
}
function addWeapon(state, hero, weaponId, events) {
  hero.weapons.push(weaponId);
  events.push({ type: "loot", hero: hero.index, kind: "waffe", itemId: weaponId });
  if (hero.weapons.length <= MAX_WEAPONS) return;
  let worstIdx = 0;
  let worstValue = Infinity;
  hero.weapons.forEach((id, i) => {
    const v = weaponValue(hero, WEAPON_BY_ID[id]);
    if (v < worstValue) {
      worstValue = v;
      worstIdx = i;
    }
  });
  const [dropped] = hero.weapons.splice(worstIdx, 1);
  events.push({ type: "weapon_dropped", hero: hero.index, itemId: dropped });
}
function addSpell(state, hero, events) {
  const spellId = state.spellPool.pop();
  if (!spellId) return;
  if (hero.spells.length >= MAX_SPELLS) {
    events.push({ type: "loot_discarded", hero: hero.index, kind: "zauber", itemId: spellId });
    return;
  }
  hero.spells.push(spellId);
  events.push({ type: "loot", hero: hero.index, kind: "zauber", itemId: spellId });
}
function addPoints(hero, kind, events) {
  hero.points[kind] += 1;
  events.push({ type: "points", hero: hero.index, kind, total: totalPoints(hero) });
}
function totalPoints(hero) {
  return hero.points.splitter * 2 + hero.points.truhe * 4 + hero.points.kristallherz * 3;
}
function tryOpenChest(hero, events) {
  let withAbility = false;
  if (hero.keys > 0) hero.keys -= 1;
  else if (hero.hero.abilities.includes("schatzjaeger") && !hero.usedInGame.includes("schatzjaeger")) {
    hero.usedInGame.push("schatzjaeger");
    withAbility = true;
  } else return false;
  events.push({ type: "chest_opened", hero: hero.index, x: hero.x, y: hero.y, withAbility });
  addPoints(hero, "truhe", events);
  return true;
}
function giveLoot(state, hero, kind, extraSpell, events) {
  switch (kind) {
    case "heilkraut":
      if (hero.herbs >= MAX_HERBS) events.push({ type: "loot_discarded", hero: hero.index, kind });
      else {
        hero.herbs += 1;
        events.push({ type: "loot", hero: hero.index, kind });
      }
      return;
    case "zauber":
      addSpell(state, hero, events);
      if (extraSpell) addSpell(state, hero, events);
      return;
    case "waffe": {
      const id = state.weaponPool.pop();
      if (id) addWeapon(state, hero, id, events);
      return;
    }
    case "schluessel":
      hero.keys += 1;
      events.push({ type: "loot", hero: hero.index, kind });
      return;
    case "splitter":
      events.push({ type: "loot", hero: hero.index, kind });
      addPoints(hero, "splitter", events);
      return;
    case "truhe":
      events.push({ type: "loot", hero: hero.index, kind });
      if (!tryOpenChest(hero, events)) {
        const tile = state.board[`${hero.x},${hero.y}`];
        tile.chests += 1;
        events.push({ type: "chest_left", x: hero.x, y: hero.y });
      }
      return;
    case "kristallherz":
      events.push({ type: "loot", hero: hero.index, kind });
      addPoints(hero, "kristallherz", events);
      return;
  }
}

// ../../packages/rules/src/game.ts
var TILE_BY_ID = Object.fromEntries([...TILES, START_TILE].map((t) => [t.id, t]));
function starterWeapon(hero) {
  const a = hero.attributes;
  const best = a.staerke >= a.geschick && a.staerke >= a.magie ? "staerke" : a.geschick >= a.magie ? "geschick" : "magie";
  return STARTER_WEAPON_BY_ATTRIBUTE[best];
}
function createGame(config) {
  if (config.heroes.length < 1 || config.heroes.length > 4) throw new RuleError("player_count", "1 bis 4 Helden");
  const rng = createRng(config.seed);
  const size = config.size ?? "normal";
  const tileStack = buildTileStack(size, rng);
  const monsterIds = [];
  for (const m of worldOf(config.world).monsters) if (!m.boss) for (let i = 0; i < m.count; i++) monsterIds.push(m.id);
  const state = {
    seed: config.seed,
    rngState: 0,
    world: config.world ?? "wald",
    size,
    turn: 0,
    round: 0,
    current: -1,
    phase: "playing",
    heroes: config.heroes.map(({ hero, companionId }, index) => ({
      index,
      hero,
      companionId,
      x: 0,
      y: 0,
      hp: maxHp(hero),
      maxHp: maxHp(hero),
      actionsLeft: 0,
      exhausted: false,
      weapons: [starterWeapon(hero)],
      spells: [],
      herbs: 0,
      keys: 0,
      points: { splitter: 0, truhe: 0, kristallherz: 0 },
      usedInGame: [],
      usedInTurn: [],
      stats: { monstersDefeated: 0, spellsCast: 0, timesExhausted: 0, tilesRevealed: 0, turns: 0 }
    })),
    board: {},
    tileStack,
    monsterStack: rng.shuffle(monsterIds),
    weaponPool: buildWeaponPool(rng),
    spellPool: buildSpellPool(rng),
    stackSize: tileStack.length
  };
  placeTile(state.board, START_TILE, 0, 0, 0);
  advanceTurn(state, []);
  state.rngState = rng.state();
  return state;
}
function currentHero(state) {
  return state.heroes[state.current];
}
function heroTile(state, hero) {
  return tileAt(state.board, hero.x, hero.y);
}
function scores(state) {
  const list = state.heroes.map((h) => ({ index: h.index, name: h.hero.name, points: totalPoints(h), hp: h.hp, rank: 0 }));
  list.sort((a, b) => b.points - a.points || b.hp - a.hp || a.index - b.index);
  list.forEach((s, i) => {
    s.rank = i + 1;
  });
  return list;
}
function applyCommand(state, cmd) {
  if (state.phase === "ended") throw new RuleError("game_over");
  if (state.pending && cmd.type !== "place" && cmd.type !== "redraw") throw new RuleError("tile_pending", "Erst das gezogene Pl\xE4ttchen anlegen");
  if (!state.pending && (cmd.type === "place" || cmd.type === "redraw")) throw new RuleError("nothing_pending");
  const events = [];
  const rng = createRng(state.rngState);
  const hero = currentHero(state);
  try {
    switch (cmd.type) {
      case "move":
        doMove(state, hero, cmd.dir, cmd.passBy, events);
        break;
      case "place":
        doPlace(state, hero, cmd.rotation, rng, events);
        break;
      case "redraw":
        doRedraw(state, hero, events);
        break;
      case "fight":
        doFight(state, hero, cmd.options ?? {}, rng, events);
        break;
      case "flee":
        doFlee(state, hero, cmd.useAusweichen ?? false, rng, events);
        break;
      case "use_herb":
        doUseHerb(hero, events);
        break;
      case "open_chest":
        doOpenChest(state, hero, events);
        break;
      case "healing_hands":
        doHealingHands(hero, events);
        break;
      case "fairy_ring":
        doFairyRing(state, hero, cmd.x, cmd.y, events);
        break;
      case "cast_sturmboee":
        doSturmboee(state, hero, cmd.dir, events);
        break;
      case "cast_waldpfad":
        doWaldpfad(state, hero, cmd.x, cmd.y, events);
        break;
      case "peek":
        doPeek(state, hero, events);
        break;
      case "end_turn":
        endTurn(state, hero, events);
        break;
    }
  } finally {
    state.rngState = rng.state();
  }
  return events;
}
function requireActions(hero) {
  if (hero.actionsLeft < 1) throw new RuleError("no_actions", "Keine Aktionen mehr in diesem Zug");
}
function resolvePassBy(hero, tile, passBy) {
  if (!tile.monster) return void 0;
  const canSneak = hasAbility(hero.hero, "schleicher") && !hero.usedInTurn.includes("schleicher");
  const canCharge = hasAbility(hero.hero, "sturmangriff") && hero.hp > 2;
  const chosen = passBy ?? (canSneak ? "schleicher" : canCharge ? "sturmangriff" : void 0);
  if (chosen === "schleicher" && canSneak) return chosen;
  if (chosen === "sturmangriff" && canCharge) return chosen;
  throw new RuleError("monster_blocks", "Ein Monster versperrt den Weg: k\xE4mpfen, fliehen oder Zug beenden");
}
function applyPassBy(hero, passBy, events) {
  if (passBy === "schleicher") hero.usedInTurn.push("schleicher");
  if (passBy === "sturmangriff") {
    hero.hp -= 2;
    events.push({ type: "damaged", hero: hero.index, amount: 2, source: "sturmangriff" });
  }
}
function moveHero(hero, x, y, via, events) {
  hero.prev = { x: hero.x, y: hero.y };
  hero.x = x;
  hero.y = y;
  hero.fight = void 0;
  events.push({ type: "moved", hero: hero.index, x, y, via });
}
function doMove(state, hero, dir, passBy, events) {
  requireActions(hero);
  const tile = heroTile(state, hero);
  if (!hasExit(tile.exits, dir)) throw new RuleError("no_exit", "Kein Ausgang in diese Richtung");
  const target = step(hero.x, hero.y, dir);
  const targetTile = tileAt(state.board, target.x, target.y);
  const chosenPassBy = resolvePassBy(hero, tile, passBy);
  if (targetTile) {
    if (!connected(state.board, hero.x, hero.y, dir)) throw new RuleError("not_connected", "Das Nachbarpl\xE4ttchen hat dort keinen Ausgang");
    hero.actionsLeft -= 1;
    applyPassBy(hero, chosenPassBy, events);
    moveHero(hero, target.x, target.y, chosenPassBy ?? "walk", events);
    return;
  }
  if (state.tileStack.length === 0) {
    if (state.finale) throw new RuleError("stack_empty", "Keine Pl\xE4ttchen mehr: Der Wald ist ganz aufgedeckt");
    endGame(state, "stack_empty", events);
    return;
  }
  const shape = state.tileStack.pop();
  hero.actionsLeft -= 1;
  applyPassBy(hero, chosenPassBy, events);
  state.pending = { shapeId: shape.id, x: target.x, y: target.y, fromDir: opposite(dir) };
  events.push({ type: "tile_drawn", hero: hero.index, shapeId: shape.id, x: target.x, y: target.y });
}
function doPlace(state, hero, rotation, rng, events) {
  const pending = state.pending;
  const shape = TILE_BY_ID[pending.shapeId];
  const rotations = fittingRotations(state.board, shape, pending.x, pending.y, pending.fromDir);
  const chosen = rotation ?? rotations[0];
  if (chosen === void 0 || !rotations.includes(chosen)) throw new RuleError("bad_rotation", "So angelegt schlie\xDFt das Pl\xE4ttchen nicht an");
  const tile = placeTile(state.board, shape, pending.x, pending.y, chosen);
  state.pending = void 0;
  hero.stats.tilesRevealed += 1;
  events.push({ type: "tile_placed", hero: hero.index, shapeId: shape.id, x: tile.x, y: tile.y, rotation: chosen, exits: tile.exits });
  moveHero(hero, tile.x, tile.y, "walk", events);
  if (tile.kind === "lichtung") {
    const monsterId = state.monsterStack.pop();
    if (monsterId) {
      tile.monster = { id: monsterId, hp: MONSTER_BY_ID[monsterId].hp };
      events.push({ type: "monster_appeared", monsterId, x: tile.x, y: tile.y });
    }
  } else if (tile.kind === "dornenherz") {
    const boss = worldOf(state.world).boss;
    tile.monster = { id: boss.id, hp: boss.hp };
    events.push({ type: "monster_appeared", monsterId: boss.id, x: tile.x, y: tile.y });
    const rounds = finaleRounds(state.heroes.length);
    state.finale = { endsAfterRound: state.round + rounds };
    events.push({ type: "finale_started", rounds });
  } else if (tile.kind === "dickicht" && rng.int(1, DICKICHT_SPLITTER_ONE_IN) === 1) {
    events.push({ type: "splitter_found", hero: hero.index, x: tile.x, y: tile.y, source: "dickicht" });
    hero.points.splitter += 1;
    events.push({ type: "points", hero: hero.index, kind: "splitter", total: totalPoints(hero) });
  }
}
function doRedraw(state, hero, events) {
  if (!hasAbility(hero.hero, "wildniskenner") || hero.usedInGame.includes("wildniskenner")) throw new RuleError("ability_unavailable");
  const pending = state.pending;
  const old = TILE_BY_ID[pending.shapeId];
  state.tileStack.unshift(old);
  const next = state.tileStack.pop();
  hero.usedInGame.push("wildniskenner");
  pending.shapeId = next.id;
  events.push({ type: "tile_redrawn", hero: hero.index, oldShapeId: old.id, newShapeId: next.id });
}
function doPeek(state, hero, events) {
  const allowed = hasAbility(hero.hero, "fernblick") || !!companionEffect(companionOf(hero), "peek_tile");
  if (!allowed || hero.usedInTurn.includes("peek")) throw new RuleError("ability_unavailable");
  hero.usedInTurn.push("peek");
  events.push({ type: "peeked", hero: hero.index, shapeId: state.tileStack[state.tileStack.length - 1]?.id });
}
function doFairyRing(state, hero, x, y, events) {
  requireActions(hero);
  const tile = heroTile(state, hero);
  if (tile.kind !== "feenring") throw new RuleError("not_on_fairy_ring");
  if (tile.monster) throw new RuleError("monster_blocks");
  const target = tileAt(state.board, x, y);
  if (!target || target.kind !== "feenring" || target === tile) throw new RuleError("bad_target", "Ziel ist kein anderer aufgedeckter Feenring");
  hero.actionsLeft -= 1;
  moveHero(hero, x, y, "fairy_ring", events);
}
function companionOf(hero) {
  return hero.companionId ? COMPANION_BY_ID[hero.companionId] : void 0;
}
function newFight(hero, monsterId) {
  return { monsterId, round: 0, roundsInAction: 0, used: [], monsterMissRounds: 0, monsterAttackPenalty: hasAbility(hero.hero, "kriegsschrei") ? 2 : 0 };
}
function toCombat(state, hero, tile) {
  const monster = MONSTER_BY_ID[tile.monster.id];
  let fight = hero.fight;
  if (!fight || fight.monsterId !== monster.id) fight = newFight(hero, monster.id);
  const cs = startCombat(hero.hero, monster, hero.hp, tile.monster.hp, companionOf(hero));
  cs.log = [];
  cs.round = fight.round;
  cs.used = new Set(fight.used);
  cs.monsterMissRounds = fight.monsterMissRounds;
  cs.monsterAttackPenalty = fight.monsterAttackPenalty;
  return { cs, fight };
}
function fromCombat(hero, tile, cs, fight) {
  hero.hp = cs.heroHp;
  tile.monster.hp = cs.monsterHp;
  fight.round = cs.round;
  fight.used = [...cs.used];
  fight.monsterMissRounds = cs.monsterMissRounds;
  fight.monsterAttackPenalty = cs.monsterAttackPenalty;
  if (cs.used.has("schmetterling") && !hero.usedInGame.includes("schmetterling")) hero.usedInGame.push("schmetterling");
}
function doFight(state, hero, options, rng, events) {
  const tile = heroTile(state, hero);
  if (!tile.monster) throw new RuleError("no_monster", "Hier ist kein Monster");
  if (options.weaponId && options.weaponId !== "faeuste" && !hero.weapons.includes(options.weaponId)) throw new RuleError("no_such_weapon");
  const { cs, fight } = toCombat(state, hero, tile);
  const needsAction = COMBAT_ACTIONS_PER_TURN > 0 && fight.roundsInAction === 0;
  if (needsAction && hero.actionsLeft < COMBAT_ACTIONS_PER_TURN) throw new RuleError("no_actions", "Keine Aktionen mehr in diesem Zug");
  const opts = {
    weaponId: options.weaponId,
    useWuchtschlag: options.useWuchtschlag,
    useZweiterSchlag: options.useZweiterSchlag,
    useZielsicher: options.useZielsicher,
    useGlueckspilz: options.useGlueckspilz,
    castHeilzauber: options.castHeilzauber,
    extraReroll: options.useSchmetterling && hero.companionId === "schmetterling" && !hero.usedInGame.includes("schmetterling")
  };
  let spellUsed;
  if (options.spellId) {
    const spell = SPELL_BY_ID[options.spellId];
    if (!spell || !hero.spells.includes(options.spellId)) throw new RuleError("no_such_spell");
    if (spell.effect.type === "damage") opts.castSpell = { id: spell.id, dice: spell.effect.dice };
    else if (spell.effect.type === "monster_misses") opts.castNebelmantel = { rounds: spell.effect.rounds };
    else throw new RuleError("spell_not_in_combat", "Dieser Zauber wird au\xDFerhalb des Kampfes gelesen");
    spellUsed = spell.id;
  }
  if (needsAction) hero.actionsLeft -= COMBAT_ACTIONS_PER_TURN;
  hero.fight = fight;
  if (spellUsed) {
    hero.spells.splice(hero.spells.indexOf(spellUsed), 1);
    hero.stats.spellsCast += 1;
    events.push({ type: "spell_cast", hero: hero.index, spellId: spellUsed });
  }
  const monsterHpBefore = cs.monsterHp;
  beginRound(cs);
  heroAction(cs, opts, rng);
  if (cs.monster.boss && cs.monsterHp < monsterHpBefore) {
    state.bossHelpers ??= [];
    if (!state.bossHelpers.includes(hero.index)) state.bossHelpers.push(hero.index);
  }
  if (cs.status === "running") monsterAttack(cs, rng, options.useAusweichen ?? false);
  fromCombat(hero, tile, cs, fight);
  fight.roundsInAction += 1;
  events.push({ type: "fight_round", hero: hero.index, monsterId: cs.monster.id, round: cs.round, log: cs.log, heroHp: hero.hp, monsterHp: tile.monster.hp });
  if (cs.status === "won") monsterDefeated(state, hero, tile, rng, events);
  else if (cs.status === "lost") exhaust(state, hero, cs.monster.id, events);
}
function monsterDefeated(state, hero, tile, rng, events) {
  const monster = MONSTER_BY_ID[tile.monster.id];
  tile.monster = void 0;
  hero.fight = void 0;
  hero.stats.monstersDefeated += 1;
  events.push({ type: "monster_defeated", hero: hero.index, monsterId: monster.id });
  giveLoot(state, hero, monster.loot, !!companionEffect(companionOf(hero), "extra_spell_loot"), events);
  if (monster.boss) {
    for (const index of state.bossHelpers ?? [hero.index]) {
      const helper = state.heroes[index];
      events.push({ type: "splitter_found", hero: index, x: helper.x, y: helper.y, source: "boss_help" });
      addPoints(helper, "splitter", events);
    }
    endGame(state, "boss_defeated", events);
    return;
  }
  if (monster.loot !== "splitter" && rng.int(1, MONSTER_SPLITTER_ONE_IN) === 1) {
    events.push({ type: "splitter_found", hero: hero.index, x: tile.x, y: tile.y, source: "monster" });
    addPoints(hero, "splitter", events);
  }
}
function doFlee(state, hero, useAusweichen, rng, events) {
  const tile = heroTile(state, hero);
  if (!tile.monster) throw new RuleError("no_monster");
  const { cs, fight } = toCombat(state, hero, tile);
  flee(cs, rng, useAusweichen);
  fromCombat(hero, tile, cs, fight);
  events.push({ type: "fight_round", hero: hero.index, monsterId: cs.monster.id, round: cs.round, log: cs.log, heroHp: hero.hp, monsterHp: tile.monster.hp });
  if (cs.status === "lost") {
    exhaust(state, hero, cs.monster.id, events);
    return;
  }
  events.push({ type: "fled", hero: hero.index, monsterId: cs.monster.id });
  hero.fight = void 0;
  if (hero.prev) moveHero(hero, hero.prev.x, hero.prev.y, "flee", events);
}
function exhaust(state, hero, monsterId, events) {
  hero.hp = 0;
  hero.exhausted = true;
  hero.fight = void 0;
  hero.actionsLeft = 0;
  hero.stats.timesExhausted += 1;
  events.push({ type: "exhausted", hero: hero.index, monsterId });
  if (hero.prev) {
    hero.x = hero.prev.x;
    hero.y = hero.prev.y;
    hero.prev = void 0;
    events.push({ type: "moved", hero: hero.index, x: hero.x, y: hero.y, via: "exhausted" });
  }
  hero.usedInTurn = [];
  events.push({ type: "turn_ended", hero: hero.index });
  if (!state.finale && boardClosed(state)) {
    endGame(state, "board_closed", events);
    return;
  }
  advanceTurn(state, events);
}
function heal(hero, amount, source, events) {
  const before = hero.hp;
  hero.hp = Math.min(hero.maxHp, hero.hp + amount);
  if (hero.hp > before) events.push({ type: "healed", hero: hero.index, amount: hero.hp - before, source });
}
function doUseHerb(hero, events) {
  if (hero.herbs < 1) throw new RuleError("no_herb");
  hero.herbs -= 1;
  heal(hero, HERB_HEAL, "herb", events);
}
function doHealingHands(hero, events) {
  if (!hasAbility(hero.hero, "heilende_haende") || hero.usedInGame.includes("heilende_haende")) throw new RuleError("ability_unavailable");
  hero.usedInGame.push("heilende_haende");
  heal(hero, hero.maxHp, "heilende_haende", events);
}
function doOpenChest(state, hero, events) {
  const tile = heroTile(state, hero);
  if (tile.chests < 1) throw new RuleError("no_chest");
  const canOpen = hero.keys > 0 || hasAbility(hero.hero, "schatzjaeger") && !hero.usedInGame.includes("schatzjaeger");
  if (!canOpen) throw new RuleError("no_key", "Ohne Moosschl\xFCssel bleibt die Truhe zu");
  tile.chests -= 1;
  tryOpenChest(hero, events);
}
function takeSpell(hero, spellId, events) {
  const idx = hero.spells.indexOf(spellId);
  if (idx < 0) throw new RuleError("no_such_spell");
  hero.spells.splice(idx, 1);
  hero.stats.spellsCast += 1;
  events.push({ type: "spell_cast", hero: hero.index, spellId });
}
function doSturmboee(state, hero, dir, events) {
  requireActions(hero);
  if (!hero.spells.includes("sturmboee")) throw new RuleError("no_such_spell");
  const tile = heroTile(state, hero);
  if (!tile.monster) throw new RuleError("no_monster");
  if (!connected(state.board, hero.x, hero.y, dir)) throw new RuleError("not_connected");
  const p = step(hero.x, hero.y, dir);
  const target = tileAt(state.board, p.x, p.y);
  if (target.monster || state.heroes.some((h) => h.x === p.x && h.y === p.y)) throw new RuleError("target_occupied", "Das Nachbarpl\xE4ttchen ist nicht leer");
  hero.actionsLeft -= 1;
  takeSpell(hero, "sturmboee", events);
  target.monster = tile.monster;
  tile.monster = void 0;
  hero.fight = void 0;
  events.push({ type: "monster_pushed", monsterId: target.monster.id, x: p.x, y: p.y });
}
function doWaldpfad(state, hero, x, y, events) {
  requireActions(hero);
  if (!hero.spells.includes("waldpfad")) throw new RuleError("no_such_spell");
  const target = tileAt(state.board, x, y);
  if (!target || target.kind !== "quelle") throw new RuleError("bad_target", "Ziel ist keine aufgedeckte Quelle");
  hero.actionsLeft -= 1;
  takeSpell(hero, "waldpfad", events);
  moveHero(hero, x, y, "waldpfad", events);
}
function endTurn(state, hero, events) {
  if (heroTile(state, hero).kind === "quelle") heal(hero, hero.maxHp, "quelle", events);
  const regen = companionEffect(companionOf(hero), "regen");
  if (regen) heal(hero, regen.hp, "frosch", events);
  hero.usedInTurn = [];
  hero.actionsLeft = 0;
  if (hero.fight) hero.fight.roundsInAction = 0;
  events.push({ type: "turn_ended", hero: hero.index });
  if (!state.finale && boardClosed(state)) {
    endGame(state, "board_closed", events);
    return;
  }
  advanceTurn(state, events);
}
function boardClosed(state) {
  return Object.values(state.board).every((t) => unexploredExits(state.board, t.x, t.y).length === 0);
}
function advanceTurn(state, events) {
  if (state.phase === "ended") return;
  for (let guard = 0; guard <= state.heroes.length; guard++) {
    state.current = (state.current + 1) % state.heroes.length;
    if (state.current === 0) {
      state.round += 1;
      if (state.finale) {
        const roundsLeft = state.finale.endsAfterRound - state.round + 1;
        if (roundsLeft < 1) {
          endGame(state, "finale_over", events);
          return;
        }
        events.push({ type: "finale_tick", roundsLeft });
      }
    }
    state.turn += 1;
    const hero = currentHero(state);
    if (hero.exhausted) {
      hero.exhausted = false;
      hero.hp = hero.maxHp;
      events.push({ type: "turn_skipped", hero: hero.index, reason: "exhausted" });
      continue;
    }
    hero.actionsLeft = actionsPerTurn(hero.hero);
    hero.stats.turns += 1;
    events.push({ type: "turn_started", hero: hero.index, turn: state.turn, round: state.round, actions: hero.actionsLeft });
    return;
  }
}
function endGame(state, reason, events) {
  state.phase = "ended";
  state.endReason = reason;
  state.pending = void 0;
  events.push({ type: "game_over", reason, scores: scores(state) });
}
function canStep(state, hero, dir) {
  const tile = heroTile(state, hero);
  if (!hasExit(tile.exits, dir)) return false;
  const p = step(hero.x, hero.y, dir);
  const target = tileAt(state.board, p.x, p.y);
  return target ? connected(state.board, hero.x, hero.y, dir) : true;
}

// ../../packages/rules/src/bot.ts
function fightOdds(hero, monsterId, monsterHp) {
  const monster = MONSTER_BY_ID[monsterId];
  const weaponId = bestWeaponId(hero);
  const weapon = WEAPON_BY_ID[weaponId ?? "faeuste"];
  const heroDmg = hitProbability(hero.hero.attributes[weapon.attribute], monster.armor) * diceAverage(weapon.damage);
  const companion = hero.companionId ? COMPANION_BY_ID[hero.companionId] : void 0;
  const bite = companion?.effect.type === "attack" ? hitProbability(companion.effect.bonus, monster.armor) * diceAverage(companion.effect.damage) : 0;
  const heroArmor2 = 10 + hero.hero.attributes.geschick + (companion?.effect.type === "armor" ? companion.effect.bonus : 0);
  const monsterDmg = hitProbability(monster.attack, heroArmor2) * diceAverage(monster.damage);
  return {
    heroRounds: monsterHp / Math.max(0.1, heroDmg + bite),
    monsterRounds: hero.hp / Math.max(0.1, monsterDmg)
  };
}
function fightOptions(hero, round) {
  const lowHp = hero.hp <= Math.ceil(hero.maxHp / 3);
  const damageSpell = hero.spells.find((id) => SPELL_BY_ID[id]?.effect.type === "damage");
  const fog = hero.spells.find((id) => SPELL_BY_ID[id]?.effect.type === "monster_misses");
  const opts = {
    weaponId: bestWeaponId(hero),
    useWuchtschlag: true,
    useZweiterSchlag: round === 0,
    useZielsicher: true,
    useGlueckspilz: true,
    useSchmetterling: true,
    useAusweichen: hero.hp <= 5
  };
  if (lowHp && hero.hero.abilities.includes("heilzauber") && !hero.fight?.used.includes("heilzauber")) opts.castHeilzauber = true;
  else if (lowHp && fog && (hero.fight?.monsterMissRounds ?? 0) === 0) opts.spellId = fog;
  else if (damageSpell && hero.hero.attributes.magie >= 2) opts.spellId = damageSpell;
  return opts;
}
var hasOpenExit = (state) => (t) => unexploredExits(state.board, t.x, t.y).length > 0;
var noMonster = (t) => !t.monster;
function chooseCommand(state, rng, options = {}) {
  const hero = state.heroes[state.current];
  const tile = heroTile(state, hero);
  const fleeBelow = options.fleeBelow ?? 0.3;
  const stackLeft = state.tileStack.length > 0;
  const openExit = (t) => stackLeft && hasOpenExit(state)(t);
  const isBoss = (t) => !!t.monster && !!MONSTER_BY_ID[t.monster.id]?.boss;
  const weak = hero.hp <= hero.maxHp / 3;
  const canOpenChest = hero.keys > 0 || hero.hero.abilities.includes("schatzjaeger") && !hero.usedInGame.includes("schatzjaeger");
  if (state.pending) return { type: "place" };
  if (hero.herbs > 0 && hero.hp <= hero.maxHp - 5) return { type: "use_herb" };
  if (hero.hp <= 3 && hero.hero.abilities.includes("heilende_haende") && !hero.usedInGame.includes("heilende_haende")) return { type: "healing_hands" };
  if (tile.chests > 0 && canOpenChest) return { type: "open_chest" };
  const origin = tile.monster && hero.prev ? hero.prev : { x: hero.x, y: hero.y };
  const originTile = tileAt(state.board, origin.x, origin.y);
  const frontierFree = !!originTile && !originTile.monster && openExit(originTile) || !!pathStep(state.board, origin.x, origin.y, (t) => !t.monster && openExit(t), noMonster);
  if (tile.monster) {
    const odds = fightOdds(hero, tile.monster.id, tile.monster.hp);
    const inFight = !!hero.fight && hero.fight.roundsInAction > 0;
    const hpShare = hero.hp / hero.maxHp;
    const monster = MONSTER_BY_ID[tile.monster.id];
    const worthIt = !frontierFree || monster.boss && !!state.finale || odds.monsterRounds > odds.heroRounds * (monster.boss ? 1.2 : 0.9);
    const fightNow = () => ({ type: "fight", options: fightOptions(hero, hero.fight?.round ?? 0) });
    const prevTile = hero.prev ? tileAt(state.board, hero.prev.x, hero.prev.y) : void 0;
    const fleeUseful = !!prevTile && !prevTile.monster && hero.actionsLeft > 0;
    if (inFight) {
      if (fleeUseful && frontierFree && hpShare < fleeBelow && odds.heroRounds > 1.2) return { type: "flee", useAusweichen: true };
      return fightNow();
    }
    const canFight = hero.actionsLeft >= COMBAT_ACTIONS_PER_TURN;
    if (canFight && worthIt && (hpShare >= fleeBelow || !frontierFree)) return fightNow();
    if (hero.actionsLeft > 0) {
      const sneak = hero.hero.abilities.includes("schleicher") && !hero.usedInTurn.includes("schleicher");
      const charge = hero.hero.abilities.includes("sturmangriff") && hero.hp > 4;
      if (sneak || charge) {
        const dirs = DIRS2.filter((d2) => canStep(state, hero, d2));
        if (dirs.length) return { type: "move", dir: dirs[rng.int(0, dirs.length - 1)], passBy: sneak ? "schleicher" : "sturmangriff" };
      }
      if (hero.spells.includes("sturmboee")) {
        const free = DIRS2.filter((d2) => {
          if (!canStep(state, hero, d2)) return false;
          const p = step(hero.x, hero.y, d2);
          const t = tileAt(state.board, p.x, p.y);
          return !!t && !t.monster && !state.heroes.some((h) => h.x === p.x && h.y === p.y);
        });
        if (free.length) return { type: "cast_sturmboee", dir: free[0] };
      }
    }
    if (fleeUseful) return { type: "flee", useAusweichen: true };
    if (canFight) return fightNow();
    return { type: "end_turn" };
  }
  if (hero.actionsLeft < 1) return { type: "end_turn" };
  if (weak) {
    if (tile.kind === "quelle") return { type: "end_turn" };
    const spring = Object.values(state.board).find((t) => t.kind === "quelle");
    if (spring && hero.spells.includes("waldpfad")) return { type: "cast_waldpfad", x: spring.x, y: spring.y };
    const toSpring = pathStep(state.board, hero.x, hero.y, (t) => t.kind === "quelle", noMonster);
    if (toSpring && toSpring.distance <= hero.actionsLeft + 2) return { type: "move", dir: toSpring.dir };
  }
  if (state.finale && !weak) {
    const toBoss = pathStep(state.board, hero.x, hero.y, isBoss, noMonster);
    if (toBoss) return { type: "move", dir: toBoss.dir };
  }
  if (canOpenChest) {
    const toChest = pathStep(state.board, hero.x, hero.y, (t) => t.chests > 0, noMonster);
    if (toChest && toChest.distance <= 4) return { type: "move", dir: toChest.dir };
  }
  const unexplored = stackLeft ? unexploredExits(state.board, hero.x, hero.y) : [];
  if (unexplored.length) return { type: "move", dir: unexplored[rng.int(0, unexplored.length - 1)] };
  const toFrontier = pathStep(state.board, hero.x, hero.y, (t) => !t.monster && openExit(t), noMonster);
  if (toFrontier) return { type: "move", dir: toFrontier.dir };
  const toMonster = pathStep(state.board, hero.x, hero.y, (t) => !!t.monster && openExit(t), noMonster) ?? pathStep(state.board, hero.x, hero.y, (t) => !!t.monster, noMonster);
  if (toMonster && !weak) return { type: "move", dir: toMonster.dir };
  if (toMonster && weak && !Object.values(state.board).some((t) => t.kind === "quelle")) return { type: "move", dir: toMonster.dir };
  return { type: "end_turn" };
}

// ../../packages/online/src/room.ts
var EMPTY_ROOM_TTL_MS = 10 * 60 * 1e3;
var MESSAGES = {
  version: "Das Spiel hat eine neue Version. Bitte die Seite neu laden.",
  not_found: "Diesen Raum gibt es nicht (mehr).",
  full: "Hier ist kein Platz mehr frei.",
  not_allowed: "Das darf nur, wer das Spiel erstellt hat.",
  bad_request: "Das hat nicht geklappt.",
  rule: "Das geht gerade nicht.",
  not_your_turn: "Du bist gerade nicht dran.",
  taken: "Diesen Helden spielt schon jemand.",
  busy: "Moment, das Spiel war gerade schneller."
};
var Room = class _Room {
  constructor(data) {
    this.data = data;
  }
  /** Ereignisse des letzten Befehls (bestimmen die Pause bis zum nächsten Bot-Zug). */
  lastEvents = [];
  static create(code, now) {
    return new _Room({
      v: PROTOCOL_VERSION,
      code,
      created: now,
      touched: now,
      phase: "lobby",
      world: "wald",
      size: "kurz",
      host: "",
      players: [],
      slots: [],
      seq: 0,
      botSteps: 0
    });
  }
  get code() {
    return this.data.code;
  }
  /** Gastgeber: wer den Raum erstellt hat, solange er verbunden ist, sonst das nächste verbundene Gerät. */
  hostId() {
    const { players, host: host2 } = this.data;
    if (players.find((p) => p.id === host2)?.online) return host2;
    return players.find((p) => p.online && !p.left)?.id ?? host2;
  }
  view() {
    const d2 = this.data;
    return {
      code: d2.code,
      phase: d2.phase,
      world: d2.world,
      size: d2.size,
      slots: d2.slots.map((s) => ({ ...s })),
      players: d2.players.filter((p) => !p.left).map((p) => ({ id: p.id, online: p.online })),
      host: this.hostId()
    };
  }
  onlineCount() {
    return this.data.players.filter((p) => p.online).length;
  }
  /** Darf der Raum weg? Niemand verbunden und lange nichts passiert. */
  expired(now) {
    if (this.onlineCount() > 0) return false;
    const ttl = this.data.players.length === 0 || this.data.phase === "lobby" && this.data.slots.length === 0 ? EMPTY_ROOM_TTL_MS : ROOM_TTL_MS;
    return now - this.data.touched > ttl;
  }
  // -------------------------------------------------------------------------
  // Verbindungen
  /** Ein Gerät meldet sich an (neu oder zurück nach einem Verbindungsabbruch). */
  join(token, now, random2) {
    const d2 = this.data;
    d2.touched = now;
    let p = d2.players.find((x) => x.token === token);
    if (p) {
      p.online = true;
      p.left = false;
      p.offlineSince = void 0;
      for (const s of d2.slots) if (s.owner === p.id) s.away = false;
    } else {
      if (d2.players.filter((x) => !x.left).length >= MAX_PLAYERS) return { error: "full", message: MESSAGES.full };
      p = { id: newId(random2, d2.players.map((x) => x.id)), token, online: true };
      d2.players.push(p);
      if (!d2.host) d2.host = p.id;
    }
    const welcome = { t: "welcome", v: PROTOCOL_VERSION, you: p.id, room: this.view(), game: d2.game, seq: d2.seq };
    return { id: p.id, out: [{ to: p.id, msg: welcome }, { to: "*", except: p.id, msg: { t: "room", room: this.view() } }] };
  }
  /** Die letzte Verbindung eines Geräts ist zu. */
  disconnect(id, now) {
    const p = this.data.players.find((x) => x.id === id);
    if (!p || !p.online) return [];
    p.online = false;
    p.offlineSince = now;
    this.data.touched = now;
    return [this.roomUpdate()];
  }
  /**
   * Ist ein Spieler am Zug, der seit `afterMs` getrennt ist, vertritt ihn ein Bot, damit die
   * anderen nicht warten müssen. Kommt er zurück, spielt er wieder selbst.
   */
  autoAway(now, afterMs) {
    const slot = this.currentSlot();
    if (!slot?.owner || slot.away) return [];
    const p = this.data.players.find((x) => x.id === slot.owner);
    if (!p || p.online || now - (p.offlineSince ?? now) < afterMs) return [];
    for (const s of this.data.slots) if (s.owner === p.id) s.away = true;
    return [this.roomUpdate()];
  }
  // -------------------------------------------------------------------------
  // Nachrichten
  handle(id, msg, now, random2) {
    const d2 = this.data;
    if (!d2.players.some((p) => p.id === id)) return this.error(id, "not_found");
    d2.touched = now;
    const host2 = id === this.hostId();
    const lobby = d2.phase === "lobby";
    switch (msg.t) {
      case "ping":
        return [{ to: id, msg: { t: "pong" } }];
      case "hello":
        return [];
      case "resync":
        return d2.game ? [{ to: id, msg: { t: "snapshot", game: d2.game, seq: d2.seq } }] : [{ to: id, msg: { t: "room", room: this.view() } }];
      case "claim": {
        if (!lobby) return this.error(id, "busy");
        if (d2.slots.length >= MAX_SLOTS) return this.error(id, "full");
        const bad = this.checkHero(msg.hero, msg.companionId);
        if (bad) return this.error(id, bad);
        d2.slots.push({ hero: msg.hero, companionId: msg.companionId, owner: id });
        return [this.roomUpdate()];
      }
      case "pick": {
        const slot = d2.slots[msg.slot];
        if (!lobby || !slot) return this.error(id, "bad_request");
        if (slot.owner !== id && !(host2 && !slot.owner)) return this.error(id, "not_allowed");
        const bad = this.checkHero(msg.hero, msg.companionId, msg.slot);
        if (bad) return this.error(id, bad);
        slot.hero = msg.hero;
        slot.companionId = msg.companionId;
        return [this.roomUpdate()];
      }
      case "release": {
        const slot = d2.slots[msg.slot];
        if (!lobby || !slot) return this.error(id, "bad_request");
        if (slot.owner !== id && !(host2 && !slot.owner)) return this.error(id, "not_allowed");
        d2.slots.splice(msg.slot, 1);
        return [this.roomUpdate()];
      }
      case "bot": {
        if (!host2) return this.error(id, "not_allowed");
        if (!lobby) return this.error(id, "busy");
        if (d2.slots.length >= MAX_SLOTS) return this.error(id, "full");
        const bad = this.checkHero(msg.hero, void 0);
        if (bad) return this.error(id, bad);
        d2.slots.push({ hero: msg.hero });
        return [this.roomUpdate()];
      }
      case "order": {
        if (!host2) return this.error(id, "not_allowed");
        const j = msg.slot + msg.dir;
        if (!lobby || !d2.slots[msg.slot] || !d2.slots[j]) return this.error(id, "bad_request");
        [d2.slots[msg.slot], d2.slots[j]] = [d2.slots[j], d2.slots[msg.slot]];
        return [this.roomUpdate()];
      }
      case "settings": {
        if (!host2) return this.error(id, "not_allowed");
        if (!lobby) return this.error(id, "busy");
        if (msg.world) d2.world = msg.world;
        if (msg.size) d2.size = msg.size;
        return [this.roomUpdate()];
      }
      case "start":
        return this.start(id, host2, random2);
      case "cmd": {
        const game = d2.game;
        if (d2.phase !== "playing" || !game) return this.error(id, "busy");
        if (msg.seq !== d2.seq) return this.error(id, "busy");
        const slot = d2.slots[game.current];
        if (!slot || slot.owner !== id || slot.away) return this.error(id, "not_your_turn");
        return this.apply(msg.cmd, false, id);
      }
      case "takeover": {
        const slot = d2.slots[msg.slot];
        if (d2.phase !== "playing" || !slot?.owner) return this.error(id, "bad_request");
        if (msg.bot) {
          const ownerOnline = !!d2.players.find((p) => p.id === slot.owner)?.online;
          if (slot.owner !== id && !host2 && ownerOnline) return this.error(id, "not_allowed");
          slot.away = true;
        } else {
          if (slot.owner !== id) return this.error(id, "not_allowed");
          slot.away = false;
        }
        return [this.roomUpdate()];
      }
      case "again": {
        if (!host2) return this.error(id, "not_allowed");
        if (d2.phase !== "ended") return this.error(id, "busy");
        d2.phase = "lobby";
        d2.game = void 0;
        d2.seq = 0;
        for (const s of d2.slots) {
          s.away = false;
          if (s.owner && d2.players.find((p) => p.id === s.owner)?.left) s.owner = void 0;
        }
        return [this.roomUpdate()];
      }
      case "leave": {
        const p = d2.players.find((x) => x.id === id);
        if (lobby) {
          d2.slots = d2.slots.filter((s) => s.owner !== id);
          d2.players = d2.players.filter((x) => x.id !== id);
          if (d2.host === id) d2.host = d2.players[0]?.id ?? "";
        } else {
          p.left = true;
          p.online = false;
          p.offlineSince = now;
          for (const s of d2.slots) if (s.owner === id) s.away = true;
        }
        return [this.roomUpdate()];
      }
      default:
        return this.error(id, "bad_request");
    }
  }
  // -------------------------------------------------------------------------
  // Partie
  /** Ist gerade ein Bot (oder eine Vertretung) am Zug? */
  botDue() {
    return this.data.phase === "playing" && this.data.game?.phase === "playing" && isBotSlot(this.currentSlot());
  }
  /** Pause vor dem nächsten Bot-Zug, damit alle zusehen können (Animationen auf den Geräten). */
  botDelayMs() {
    let ms = 800;
    for (const e of this.lastEvents) {
      if (e.type === "tile_drawn") ms = Math.max(ms, 1e3);
      if (e.type === "tile_placed" || e.type === "turn_ended") ms = Math.max(ms, 1200);
      if (e.type === "fight_round") ms = Math.max(ms, 1500);
      if (e.type === "monster_appeared") ms = Math.max(ms, 2600);
      if (e.type === "finale_started" || e.type === "finale_tick") ms = Math.max(ms, 3200);
    }
    return ms;
  }
  /** Ein Zug des Bots am Zug. */
  botStep(now) {
    const game = this.data.game;
    if (!game || !this.botDue()) return [];
    this.data.touched = now;
    const rng = createRng(game.seed ^ game.turn * 7919 ^ ++this.data.botSteps);
    const out = this.apply(chooseCommand(game, rng), true);
    if (out.some((o) => o.msg.t === "applied")) return out;
    return this.apply({ type: "end_turn" }, true);
  }
  currentSlot() {
    const game = this.data.game;
    return this.data.phase === "playing" && game ? this.data.slots[game.current] : void 0;
  }
  start(id, host2, random2) {
    const d2 = this.data;
    if (!host2) return this.error(id, "not_allowed");
    if (d2.phase !== "lobby") return this.error(id, "busy");
    if (!d2.slots.some((s) => s.owner)) return [{ to: id, msg: { t: "error", code: "bad_request", message: "Erst einen Helden w\xE4hlen." } }];
    const seed = Math.floor(random2() * 2 ** 31);
    const rng = createRng(seed ^ 2654435769);
    for (const s of d2.slots) s.companionId ??= rollCompanions(EMPTY_STATS, rng, 1)[0]?.id;
    d2.game = createGame({
      heroes: d2.slots.map((s) => ({ hero: PRESET_HEROES[s.hero], companionId: s.companionId })),
      size: d2.size,
      seed,
      world: d2.world
    });
    d2.phase = "playing";
    d2.seq = 0;
    d2.botSteps = 0;
    this.lastEvents = [];
    return [{ to: "*", msg: { t: "started", room: this.view(), game: d2.game, seq: 0 } }];
  }
  /** Wendet einen Befehl an. Bei einem Regelverstoß bleibt der Spielstand unverändert. */
  apply(cmd, bot, by) {
    const d2 = this.data;
    const game = d2.game;
    const slot = game.current;
    const backup = JSON.stringify(game);
    let events;
    try {
      events = applyCommand(game, cmd);
    } catch (err) {
      d2.game = JSON.parse(backup);
      const message = err instanceof RuleError ? err.message : MESSAGES.rule;
      return by ? [{ to: by, msg: { t: "error", code: "rule", message } }] : [];
    }
    d2.seq += 1;
    this.lastEvents = events;
    if (game.phase === "ended") d2.phase = "ended";
    return [{ to: "*", msg: { t: "applied", seq: d2.seq, slot, bot, cmd, check: checkOf(game) } }];
  }
  // -------------------------------------------------------------------------
  // Hilfen
  checkHero(hero, companionId, ownSlot) {
    if (!PRESET_HEROES[hero]) return "bad_request";
    if (companionId !== void 0 && !COMPANION_BY_ID[companionId]) return "bad_request";
    if (this.data.slots.some((s, i) => s.hero === hero && i !== ownSlot)) return "taken";
    return void 0;
  }
  roomUpdate() {
    return { to: "*", msg: { t: "room", room: this.view() } };
  }
  error(id, code) {
    return [{ to: id, msg: { t: "error", code, message: MESSAGES[code] } }];
  }
};
function newRoomCode(random2) {
  let s = "";
  for (let i = 0; i < 6; i++) s += CODE_ALPHABET[Math.floor(random2() * CODE_ALPHABET.length)];
  return s;
}
function newId(random2, taken) {
  for (; ; ) {
    let s = "";
    for (let i = 0; i < 8; i++) s += CODE_ALPHABET[Math.floor(random2() * CODE_ALPHABET.length)];
    if (!taken.includes(s)) return s;
  }
}
var errorMessage = (code) => MESSAGES[code];

// src/hub.ts
import { randomBytes } from "node:crypto";

// ../../node_modules/.pnpm/ws@8.22.0/node_modules/ws/wrapper.mjs
var import_stream = __toESM(require_stream(), 1);
var import_extension = __toESM(require_extension(), 1);
var import_permessage_deflate = __toESM(require_permessage_deflate(), 1);
var import_receiver = __toESM(require_receiver(), 1);
var import_sender = __toESM(require_sender(), 1);
var import_subprotocol = __toESM(require_subprotocol(), 1);
var import_websocket = __toESM(require_websocket(), 1);
var import_websocket_server = __toESM(require_websocket_server(), 1);

// src/hub.ts
var random = () => randomBytes(4).readUInt32BE() / 2 ** 32;
var Hub = class {
  constructor(opts) {
    this.opts = opts;
    this.now = opts.now ?? Date.now;
    for (const data of opts.storage?.loadAll() ?? []) this.rooms.set(data.code, new Room(data));
    for (const room of this.rooms.values()) this.scheduleBots(room);
  }
  rooms = /* @__PURE__ */ new Map();
  conns = /* @__PURE__ */ new Set();
  perIp = /* @__PURE__ */ new Map();
  /** Wann eine Adresse zuletzt Räume erstellt hat (nur im Speicher, höchstens eine Stunde). */
  createdBy = /* @__PURE__ */ new Map();
  botTimers = /* @__PURE__ */ new Map();
  wss = new import_websocket_server.default({ noServer: true, maxPayload: MAX_MESSAGE_BYTES });
  sweepTimer;
  closed = false;
  now;
  attach(server2) {
    server2.on("upgrade", (req, socket, head) => {
      const url = new URL(req.url ?? "/", "http://x");
      if (url.pathname !== "/ws") {
        socket.destroy();
        return;
      }
      if (!this.originAllowed(req)) {
        socket.write("HTTP/1.1 403 Forbidden\r\n\r\n");
        socket.destroy();
        return;
      }
      const ip = this.ipOf(req);
      if ((this.perIp.get(ip) ?? 0) >= (this.opts.maxConnectionsPerIp ?? 30)) {
        socket.write("HTTP/1.1 429 Too Many Requests\r\n\r\n");
        socket.destroy();
        return;
      }
      this.wss.handleUpgrade(req, socket, head, (ws) => this.onConnection(ws, ip, url.searchParams.get("room") ?? ""));
    });
    this.sweepTimer = setInterval(() => this.sweep(), 15e3);
  }
  stats() {
    return { rooms: this.rooms.size, connections: this.conns.size };
  }
  /** Beim Herunterfahren: speichern und Verbindungen mit „Server startet neu“ schließen (die Spiele verbinden sich neu). */
  close() {
    this.closed = true;
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    for (const t of this.botTimers.values()) clearTimeout(t);
    this.botTimers.clear();
    for (const room of this.rooms.values()) this.opts.storage?.save(room.data);
    this.opts.storage?.flush();
    for (const c of this.conns) c.ws.close(1012, "Neustart");
    this.wss.close();
  }
  // -------------------------------------------------------------------------
  onConnection(ws, ip, roomParam) {
    const conn = { ws, ip, alive: true, tokens: 20, last: this.now() };
    this.conns.add(conn);
    this.perIp.set(ip, (this.perIp.get(ip) ?? 0) + 1);
    const helloTimer = setTimeout(() => {
      if (!conn.player) ws.close(1008, "Keine Anmeldung");
    }, 1e4);
    ws.on("pong", () => {
      conn.alive = true;
    });
    ws.on("message", (raw, isBinary) => {
      if (isBinary || !this.allow(conn)) {
        ws.close(1008, "Zu viele Nachrichten");
        return;
      }
      const msg = parseClientMessage(raw.toString());
      if (!msg) {
        this.send(ws, { t: "error", code: "bad_request", message: errorMessage("bad_request") });
        return;
      }
      if (!conn.player) {
        if (msg.t !== "hello") return;
        clearTimeout(helloTimer);
        this.hello(conn, roomParam, msg.v, msg.token);
        return;
      }
      const room = conn.room;
      this.dispatch(room, room.handle(conn.player, msg, this.now(), random));
    });
    ws.on("close", () => {
      clearTimeout(helloTimer);
      this.conns.delete(conn);
      const left = (this.perIp.get(ip) ?? 1) - 1;
      if (left > 0) this.perIp.set(ip, left);
      else this.perIp.delete(ip);
      const { room, player } = conn;
      if (room && player && ![...this.conns].some((c) => c.room === room && c.player === player)) {
        this.dispatch(room, room.disconnect(player, this.now()));
      }
    });
    ws.on("error", () => {
    });
  }
  hello(conn, roomParam, v, token) {
    const fail = (code) => {
      this.send(conn.ws, { t: "error", code, message: errorMessage(code) });
      conn.ws.close(4e3, code);
    };
    if (v !== PROTOCOL_VERSION) {
      fail("version");
      return;
    }
    let room;
    if (roomParam === "new") {
      const now = this.now();
      const recent = (this.createdBy.get(conn.ip) ?? []).filter((t) => now - t < 36e5);
      if (this.rooms.size >= (this.opts.maxRooms ?? 1e3) || recent.length >= (this.opts.maxNewRoomsPerHour ?? 20)) {
        fail("full");
        return;
      }
      this.createdBy.set(conn.ip, [...recent, now]);
      let code = newRoomCode(random);
      while (this.rooms.has(code)) code = newRoomCode(random);
      room = Room.create(code, this.now());
      this.rooms.set(code, room);
    } else if (isRoomCode(roomParam)) {
      room = this.rooms.get(roomParam);
    }
    if (!room) {
      fail("not_found");
      return;
    }
    const result = room.join(token, this.now(), random);
    if ("error" in result) {
      fail(result.error === "full" ? "full" : "not_found");
      return;
    }
    conn.room = room;
    conn.player = result.id;
    this.dispatch(room, result.out);
  }
  /** Verschickt die Antworten der Raum-Logik, speichert den Raum und plant den nächsten Bot-Zug. */
  dispatch(room, out) {
    for (const o of out) {
      for (const c of this.conns) {
        if (c.room !== room || !c.player) continue;
        if (o.to === "*" ? c.player !== o.except : c.player === o.to) this.send(c.ws, o.msg);
      }
    }
    if (this.closed) return;
    this.opts.storage?.save(room.data);
    this.scheduleBots(room);
  }
  scheduleBots(room) {
    if (this.botTimers.has(room.code) || !room.botDue()) return;
    if (room.onlineCount() === 0) return;
    this.botTimers.set(room.code, setTimeout(() => {
      this.botTimers.delete(room.code);
      if (this.rooms.get(room.code) !== room) return;
      this.dispatch(room, room.botStep(this.now()));
    }, room.botDelayMs() * (this.opts.botDelayFactor ?? 1)));
  }
  sweep() {
    for (const c of this.conns) {
      if (!c.alive) {
        c.ws.terminate();
        continue;
      }
      c.alive = false;
      c.ws.ping();
    }
    const now = this.now();
    for (const [ip, times] of this.createdBy) {
      const recent = times.filter((t) => now - t < 36e5);
      if (recent.length) this.createdBy.set(ip, recent);
      else this.createdBy.delete(ip);
    }
    for (const room of [...this.rooms.values()]) {
      if (room.expired(now)) {
        this.rooms.delete(room.code);
        this.opts.storage?.remove(room.code);
        continue;
      }
      const out = room.autoAway(now, this.opts.autoAwayMs ?? 9e4);
      if (out.length) this.dispatch(room, out);
    }
  }
  /** Einfacher Bremsklotz: im Schnitt höchstens 10 Nachrichten pro Sekunde, kurze Spitzen bis 20. */
  allow(conn) {
    const now = this.now();
    const rate = this.opts.messagesPerSecond ?? 10;
    conn.tokens = Math.min(rate * 2, conn.tokens + (now - conn.last) / 1e3 * rate);
    conn.last = now;
    if (conn.tokens < 1) return false;
    conn.tokens -= 1;
    return true;
  }
  send(ws, msg) {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
  }
  originAllowed(req) {
    const allowed = this.opts.allowedOrigins;
    if (allowed.length === 0) return true;
    return allowed.includes(req.headers.origin ?? "");
  }
  ipOf(req) {
    const forwarded = this.opts.trustProxy ? String(req.headers["x-forwarded-for"] ?? "").split(",")[0]?.trim() : "";
    return forwarded || req.socket.remoteAddress || "?";
  }
};

// src/storage.ts
import { mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
var RoomStorage = class {
  constructor(dataDir2, delayMs = 500) {
    this.delayMs = delayMs;
    this.dir = path.join(dataDir2, "rooms");
    let ok = true;
    try {
      mkdirSync(this.dir, { recursive: true });
      const probe = path.join(this.dir, ".probe");
      writeFileSync(probe, "ok");
      rmSync(probe, { force: true });
    } catch (err) {
      ok = false;
      console.error(`Speicherordner ${this.dir} nicht beschreibbar (${err.message}). Der Server l\xE4uft, aber laufende Partien \xFCberstehen keinen Neustart. DATA_DIR pr\xFCfen (docs/ONLINE.md).`);
    }
    this.enabled = ok;
  }
  dir;
  pending = /* @__PURE__ */ new Map();
  timer;
  /** Ordner beschreibbar? Sonst läuft der Server ohne Sicherung weiter (statt abzustürzen). */
  enabled;
  /** Alle gespeicherten Räume (kaputte Dateien werden übersprungen). */
  loadAll() {
    const rooms = [];
    if (!this.enabled) return rooms;
    for (const file of readdirSync(this.dir)) {
      const code = file.replace(/\.json$/, "");
      if (!file.endsWith(".json") || !isRoomCode(code)) continue;
      try {
        const data = JSON.parse(readFileSync(path.join(this.dir, file), "utf8"));
        for (const p of data.players) {
          if (p.online) {
            p.online = false;
            p.offlineSince = Date.now();
          }
        }
        rooms.push(data);
      } catch {
      }
    }
    return rooms;
  }
  /** Speichert gebündelt (höchstens alle `delayMs`). */
  save(data) {
    if (!this.enabled) return;
    this.pending.set(data.code, data);
    this.timer ??= setTimeout(() => this.flush(), this.delayMs);
  }
  flush() {
    if (this.timer) clearTimeout(this.timer);
    this.timer = void 0;
    for (const [code, data] of this.pending) {
      const file = path.join(this.dir, `${code}.json`);
      try {
        writeFileSync(`${file}.tmp`, JSON.stringify(data));
        renameSync(`${file}.tmp`, file);
      } catch (err) {
        console.error(`Raum ${code} nicht gespeichert:`, err.message);
      }
    }
    this.pending.clear();
  }
  remove(code) {
    this.pending.delete(code);
    if (!this.enabled) return;
    rmSync(path.join(this.dir, `${code}.json`), { force: true });
  }
};

// src/main.ts
var here = path2.dirname(fileURLToPath(import.meta.url));
function loadVars(file) {
  if (!existsSync(file)) return;
  for (const line of readFileSync2(file, "utf8").split("\n")) {
    const m = /^\s*([A-Z_][A-Z0-9_]*)\s*=\s*(.*?)\s*$/.exec(line);
    if (!m || line.trimStart().startsWith("#")) continue;
    const value = m[2].replace(/^(['"])(.*)\1$/, "$2");
    process.env[m[1]] ??= value;
  }
}
loadVars(path2.join(here, ".dev.vars"));
loadVars(path2.join(process.cwd(), ".dev.vars"));
var port = Number(process.env.PORT ?? 8010);
var host = process.env.HOST ?? "127.0.0.1";
var allowedOrigins = (process.env.ALLOWED_ORIGINS ?? "").split(",").map((s) => s.trim()).filter(Boolean);
var dataDir = path2.resolve(process.env.DATA_DIR ?? path2.join(here, "data"));
var storage = new RoomStorage(dataDir);
var hub = new Hub({ allowedOrigins, storage, trustProxy: process.env.TRUST_PROXY === "1" });
var server = createServer((req, res) => {
  if (req.url === "/health" || req.url === "/ws/health") {
    res.writeHead(200, { "content-type": "application/json", "cache-control": "no-store" });
    res.end(JSON.stringify({ ok: true, protocol: PROTOCOL_VERSION, ...hub.stats() }));
    return;
  }
  res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  res.end("Funkenpfad Online-Server\n");
});
hub.attach(server);
server.listen(port, host, () => {
  console.log(`Funkenpfad Online-Server auf ${host}:${port}, ${hub.stats().rooms} R\xE4ume geladen, erlaubt: ${allowedOrigins.join(" ") || "alle (nur lokal!)"}`);
});
function shutdown() {
  hub.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 3e3).unref();
}
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
