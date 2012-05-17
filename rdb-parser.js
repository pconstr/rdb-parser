/*jslint white: true, plusplus: true, vars: true, bitwise: true*/

/* Copyright 2011-2012 Carlos Guerreiro
 *   http://perceptiveconstructs.com
 * Copyright 2012 Igalia S.L.
 * Licensed under the MIT license */

"use strict";

var EventEmitter = require('events').EventEmitter;
require('bufferjs');
var lzf = require('lzf');
var util = require('util');
var Int64 = require('node-int64');

var REDIS_RDB_6BITLEN = 0;
var REDIS_RDB_14BITLEN = 1;
var REDIS_RDB_32BITLEN = 2;
var REDIS_RDB_ENCVAL = 3;

var encodedLenTypeExtra = [];
encodedLenTypeExtra[REDIS_RDB_6BITLEN] = 0;
encodedLenTypeExtra[REDIS_RDB_14BITLEN] = 1;
encodedLenTypeExtra[REDIS_RDB_32BITLEN] = 4;
encodedLenTypeExtra[REDIS_RDB_ENCVAL] = 0;

var REDIS_RDB_ENC_INT8 = 0;
var REDIS_RDB_ENC_INT16 = 1;
var REDIS_RDB_ENC_INT32 = 2;
var REDIS_RDB_ENC_LZF = 3;

var REDIS_STRING = 0;
var REDIS_LIST = 1;
var REDIS_SET = 2;
var REDIS_ZSET = 3;
var REDIS_HASH = 4;

var REDIS_HASH_ZIPMAP = 9;
var REDIS_LIST_ZIPLIST = 10;
var REDIS_SET_INTSET = 11;
var REDIS_ZSET_ZIPLIST = 12;

var REDIS_SELECTDB = 254;
var REDIS_EOF = 255;

var INTSET_ENC_INT16 = 2;
var INTSET_ENC_INT32 = 4;
var INTSET_ENC_INT64 = 8;

var expectedStart = new Buffer('REDIS0002', 'ascii');

function readInt64(b, i) {
  var l, h, i64;
  l = b.readUInt32LE(i, i + 4);
  i += 4;
  h = b.readUInt32LE(i, i + 4);
  i += 4;
  i64 = new Int64(h, l);
  return new Buffer(i64.toString());
}

function buffArrayLength(bA) {
  var i, totalLength;
  totalLength = 0;
  for (i = 0; i < bA.length; i++) {
    totalLength += bA[i].length;
  }
  return totalLength;
}

function buffEquals(a, b) {
  if(a.length !== b.length) {
    return false;
  }
  var i;
  for(i=0; i<a.length; i++) {
    if(a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

function buffArrayEquals(bA, b) {
  var bI, i, subB, l;
  l = buffArrayLength(bA);
  if (l !== b.length) {
    return false;
  }
  bI = 0;
  for(i = 0; i < bA.length; i++) {
    subB = b.slice(bI, bI + bA[i].length);
    if(!buffEquals(bA[i], subB)) {
      return false;
    }
    bI = bI + bA[i].length;
  }
  return true;
}

function Parser() {
  var that, state, buf, i;
  var fixedBytesRem, fixedBytesStart, fixedBytesCB, fixedBytesBuffers;
  var encodedLenCB;
  var encodedLenType;
  var encodedLenRem;
  var encodedLen;

  var type;
  var key;

  that = this;
  state = 'start';

  function getFixedBytes(len, cb) {
    fixedBytesRem = len;
    fixedBytesStart = (i === buf.length ? 0: i);
    fixedBytesBuffers = [];
    fixedBytesCB = cb;
    state = 'fixedBytes';
  }

  function getEncodedLen(cb) {
    encodedLenCB = cb;
    state = 'encodedLen';
  }

  // TODO: write a variant that can be pipelined
  // this will require changing lzf decompression to work incrementally
  function getString(cb) {
    getEncodedLen(function(err, encLen, isEncoded) {
      if(err) {
        return cb(err);
      }
      if (isEncoded) {
        switch(encLen) {
        case REDIS_RDB_ENC_INT8:
          getFixedBytes(1, function(err, buffers) {
            if(err) {
              cb(err);
            } else {
              cb(null, new Buffer(String(Buffer.concat(buffers).readInt8(0))));
            }
          });
          break;
        case REDIS_RDB_ENC_INT16:
          getFixedBytes(2, function(err, buffers) {
            if(err) {
              cb(err);
            } else {
              cb(null, new Buffer(String(Buffer.concat(buffers).readInt16LE(0))));
            }
          });
          break;
        case REDIS_RDB_ENC_INT32:
          getFixedBytes(4, function(err, buffers) {
            if(err) {
              cb(err);
            } else {
              cb(null, new Buffer(String(Buffer.concat(buffers).readInt32LE(0))));
            }
          });
          break;
        case REDIS_RDB_ENC_LZF:
          getEncodedLen(function(err, encLen) {
            if(err) {
              return cb(err);
            }
            getEncodedLen(function(err, len) {
              if(err) {
                return cb(err);
              }
              getFixedBytes(encLen, function(err, buffers) {
                if(err) {
                  return cb(err);
                }
                var compressed = Buffer.concat(buffers);
                var decompressed = lzf.decompress(compressed);
                cb(null, decompressed);
              });
            });
          });
          break;
        default:
          cb('unknown encoding');
        }
      } else {
        // FIXME can getFixedBytes be called inside a callback?
        getFixedBytes(encLen, function(err, buffers) {
          if(err) {
            cb(err);
          } else {
            cb(null, Buffer.concat(buffers));
          }
        });
      }
    });
  }

  function getDouble(cb) {
    getFixedBytes(1, function(err, buffers) {
      var l = Buffer.concat(buffers)[0];
      if(l === 253) {
        return cb(null, 'NaN');
      }
      if(l === 254) {
        return cb(null, '+inf');
      }
      if(l === 255) {
        return cb(null, '-inf');
      }

      getFixedBytes(l, function(err, buffers) {
        if(err) {
          return cb(err);
        }
        var v = Buffer.concat(buffers).toString();
        return cb(null, v);
      });
    });
  }

  function getZipMap(cb) {
    // TODO: pipeline getString into an incremental ziplist parser
    getString(function(err, s) {
      if(err) {
        return cb(err);
      }

      var map = [];

      var i = 0;
      var cnt = 0;
      var free;

      i ++; // skip zmlen

      while(true) {
        var len = s[i++];

        if(len === 255) {
          break;
        }

        if(len === 254) {
          len = null; // free space only
        } else if(len === 253) {
          len = s.readUInt32LE(i);
          i = i + 4;
        }

        if((cnt & 1) === 1) {
          free = s[i++];
        } else {
          free = 0;
        }

        if(len !== null) {
          var v = s.slice(i, i+ len);
          map.push(v);
          i += (len + free);
        }
        cnt++;
      }
      cb(null, map);
    });
  }

  function getIntSet(cb) {
    getString(function(err, s) {
      if(err) {
        return cb(err);
      }

      var is = [];

      function outNumber(n) {
        is.push(new Buffer(String(n)));
      }

      var i = 0;
      var encoding = s.readUInt32LE(i);
      i += 4;
      var length = s.readUInt32LE(i);
      i += 4;

      var k;
      for(k = 0; k < length; ++k) {
        switch(encoding) {
        case INTSET_ENC_INT16:
          outNumber(s.readInt16LE(i));
          i += 2;
          break;
        case INTSET_ENC_INT32:
          outNumber(s.readInt32LE(i));
          i += 4;
          break;
        case INTSET_ENC_INT64:
          is.push(readInt64(s, i));
          i += 8;
          break;
        default:
          return cb('unsupported intset encoding');
        }
      }
      cb(null, is);
    });
  }

  function getZSet(cb) {
    // dict size
    // entry key (string)
    // entry score (double)

    getEncodedLen(function(err, zLen, isEncoded) {
      if(err) {
        return cb(err);
      }

      var set = [];
      var rem = zLen;

      function getItem() {
        if(rem === 0) {
          return cb(null, set);
        }

        getString(function(err, key) {
          if(err) {
            return cb(err);
          }

          getDouble(function(err, score) {
            if(err) {
              return cb(err);
            }
            set.push(key);
            set.push(score);
            --rem;
            getItem();
          });
        });
      }

      getItem();
    });
  }

  function getZipList(cb) {
    // TODO: pipeline getString into an incremental ziplist parser
    getString(function(err, s) {
      if(err) {
        return cb(err);
      }

      var list = [];
      var i = 0;

      function stringEntry(len) {
        var se = s.slice(i, i+ len);
        list.push(se);
        i += len;
      }

      function numberEntry(n) {
        list.push(new Buffer(String(n)));
      }

      var zlbytes = s.readUInt32LE(i);
      i += 4; // skip zlbytes
      i += 4; // skip zltail
      i += 2; // skip zllen

      var b, len;
      while(true) {
        if (s[i] === 255) {
          break;
        }
        i += s[i] === 254 ? 4 : 1; // skip prev len

        b = s[i];

        switch(b & 192) {
        case 0: // string 1 byte len
          i += 1;
          stringEntry(b & 63);
          break;
        case 64: // string 2 bytes
          i += 1;
          len = ((b & 63) << 8) + s[i];
          i += 1;
          stringEntry(len);
          break;
        case 128: // string 5 bytes
          i += 1;
          len = s.readInt32LE(i, i + 4);
          i += 4;
          stringEntry(len);
          break;
        case 192: // integer
          ++i;
          switch(b & 48) {
          case 0: // int16
            numberEntry(s.readInt16LE(i, i + 2));
            i += 2;
            break;
          case 16: // int32
            numberEntry(s.readInt32LE(i, i + 4));
            i += 4;
            break;
          case 32: // int64
            list.push(readInt64(s, i));
            i += 8;
            break;
          case 48: // unsupported
            return cb('undefined integer encoding');
          default:
            return cb('unknown encoding');
          }
          break;
        }
      }
      cb(null, list);
    });
  }

  function getHash(cb) {
    getEncodedLen(function(err, encLen, isEncoded) {
      if(err) {
        return cb(err);
      }

      var remCount = encLen * 2;
      var hash = [];

      function getRemaining() {
        if (remCount === 0) {
          return cb(null, hash);
        }
        getString(function(err, s) {
          if(err) {
            return cb(err);
          }
          hash.push(s);
          --remCount;
          getRemaining();
        });
      }

      getRemaining();
    });
  }

  function getList(cb) {
    getEncodedLen(function(err, encLen, isEncoded) {
      if(err) {
        return cb(err);
      }
      var remCount = encLen;
      var list = [];

      // TODO: too much recursion?
      function getRemaining() {
        if (remCount === 0) {
          return cb(null, list);
        }
        getString(function(err, s) {
          if(err) {
            return cb(err);
          }
          list.push(s);
          --remCount;
          getRemaining();
        });
      }

      getRemaining();
    });
  }

  function error(err) {
    that.emit('error', err);
    state = 'error';
  }

  function parse() {
    var c, end, completed, extra;

    switch(state) {
    case 'error':
      break;
    case 'eof':
      return error('data past eof');
    case 'start':
      getFixedBytes(9, function(err, buffers) {
        if(err) {
          return error(err);
        }
        if (!buffArrayEquals(buffers, expectedStart)) {
          return error('unsupported rdb format');
        }
        state = 'type';
      });
      break;
    case 'type':
      type = buf[i++];
      if(type === REDIS_SELECTDB) {
        state = 'dbId';
      } else if(type === REDIS_EOF) {
        state = 'eof';
      } else {
        state = 'key';
      }
      break;
    case 'dbId':
      getEncodedLen(function(err, dbId) {
        if(err) {
          return error(err);
        }
        state = 'type';
      });
      break;
    case 'key':
      getString(function(err, s) {
        if(err) {
          return error(err);
        }
        key = s;
        state = 'value';
      });
      break;
    case 'value':
      switch(type) {
      case REDIS_STRING:
        getString(function(err, s) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [type, key, s]);
          state = 'type';
        });
        break;
      case REDIS_LIST_ZIPLIST:
        // TODO: emit incrementally
        getZipList(function(err, l) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_LIST, key, l]);
          state = 'type';
        });
        break;
      case REDIS_LIST:
        // TODO: emit incrementally
        getList(function(err, l) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_LIST, key, l]);
          state = 'type';
        });
        break;
      case REDIS_SET:
        // TODO: emit incrementally
        getList(function(err, s) { // encoded identially to a list
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_SET, key, s]);
          state = 'type';
        });
        break;
      case REDIS_SET_INTSET:
        getIntSet(function(err, s) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_SET, key, s]);
          state = 'type';
        });
        break;
      case REDIS_ZSET_ZIPLIST:
        // TODO: emit incrementally
        getZipList(function(err, zs) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_ZSET, key, zs]);
          state = 'type';
        });
        break;
      case REDIS_ZSET:
        getZSet(function(err, zs) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_ZSET, key, zs]);
          state = 'type';
        });
        break;
      case REDIS_HASH:
        getHash(function(err, h) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_HASH, key, h]);
          state = 'type';
        });
        break;
      case REDIS_HASH_ZIPMAP:
        // TODO: emit incrementally
        getZipMap(function(err, h) {
          if(err) {
            return error(err);
          }
          that.emit('entity', [REDIS_HASH, key, h]);
          state = 'type';
        });
        break;
      default:
        console.error(type, key);
        return error('unknown type');
      }
      break;
    case 'fixedBytes':
      end = fixedBytesStart + fixedBytesRem;
      completed = false;
      if(end > buf.length) {
        end = buf.length;
      } else {
        completed = true;
      }
      fixedBytesBuffers.push(buf.slice(fixedBytesStart, end));
      fixedBytesRem = fixedBytesRem - (end - fixedBytesStart);
      i = end;
      if(completed) {
        fixedBytesCB(null, fixedBytesBuffers);
      } else {
        fixedBytesStart = 0;
      }
      break;
    case 'encodedLen':
      c = buf[i++];
      encodedLenType = c >> 6;
      extra = encodedLenTypeExtra[encodedLenType];
      if(extra > 0) {
        // TODO: optimize for common case where all extra is available in buffer
        encodedLenRem = extra;
        encodedLen = extra === 4 ? 0 : c & 63;
        state = 'encodedLenExtra';
      } else {
        encodedLenCB(null, c & 63, encodedLenType === REDIS_RDB_ENCVAL);
      }
      break;
    case 'encodedLenExtra':
      c = buf[i++];
      encodedLen = (encodedLen << 8) + c;
      --encodedLenRem;
      if (encodedLenRem === 0) {
        encodedLenCB(null, encodedLen, false);
      }
      break;
    default:
      return encodedLenCB('unknown state: '+ state);
    }
  }

  this.writable = true;

  this.write = function(data) {
    buf = data; i = 0;
    while(state !== 'error' && i < buf.length) {
      parse();
    }
  };

  this.end = function() {
    if(state !== 'eof') {
      error('unexpected end');
    } else {
      that.emit('end');
    }
  };
}

util.inherits(Parser, EventEmitter);

exports.Parser = Parser;
exports.types = {
  REDIS_STRING : REDIS_STRING,
  REDIS_LIST : REDIS_LIST,
  REDIS_SET : REDIS_SET,
  REDIS_ZSET : REDIS_ZSET,
  REDIS_HASH : REDIS_HASH
};
