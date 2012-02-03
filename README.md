rdb-parser
----------

[node.js](http://nodejs.org/) asynchronous streaming parser for [redis](http://redis.io) RDB database dumps, written in 100% Javascript

Installation
------------

`npm install rdb-parser`

Usage
-----

```javascript
var rdb = require('rdb-parser');

console.log(rdb.types);

var parser = new rdb.Parser();

parser.on('entity', function(e) {
  console.log(e);
});

parser.on('error', function(err) {
  throw err;
});

parser.on('end', function() {
  console.log('done');
});

process.stdin.pipe(parser);
process.stdin.resume();
```

Status
------

`rdb-parser` aims for complete coverage of rdb dumps.

It is close to complete at this point supporting all entity types, but may be missing one or two of the variety of encoding variations that redis can use. And it has had limited testing.

Tests
-----

`npm test` parses a (rather small) .rdb file designed to exercise a variety of types and encodings.

How you can help
----------------

If you have a dump that `rdb-parser` cannot handle please report an issue and/or contribute the dump.

License
-------

(The MIT License)

Copyright (c) 2011-2012 Carlos Guerreiro, [perceptiveconstructs.com](http://perceptiveconstructs.com)

Copyright (c) 2012 Igalia S.L.

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
