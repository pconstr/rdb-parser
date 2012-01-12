#!/usr/local/bin/node

/* Copyright 2011-2012 Carlos Guerreiro
 * http://perceptiveconstructs.com
 * Licensed under the MIT license */

var fs = require('fs');
var buffer = require('buffer');
var assert = require('assert');
var rdb = require('../rdb-parser.js');

var expected = [
    [0, 'k1', 'ssssssss'],
    [0, 'k3', 'wwwwwwww'],
    [0, 's1', '.ahaa bit longer and with spaceslonger than 256 characters and trivially compressible --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'],
    [0, 's2', 'now_exists'],
    [0, 'n5b', '1000'],
    [1, 'l10', ['100001', '100002', '100003', '100004']],
    [1, 'l11', ['9999999999', '9999999998', '9999999997']],
    [1, 'l12', ['9999999997', '9999999998', '9999999999']],
    [0, 'b1', new buffer.Buffer([255])],
    [0, 'b2', new buffer.Buffer([0, 255])],
    [0, 'b3', new buffer.Buffer([0, 0, 255])],
    [0, 'b4', new buffer.Buffer([0, 0, 0, 255])],
    [0, 'b5', new buffer.Buffer([0, 0, 0, 0, 255])],
    [4, 'h1', ['c', 'now this is quite a bit longer, but sort of boring....................................................................................................................................................................................................................................................................................................................................................................','a','aha','b','a bit longer, but not very much']],
    [4, 'h2', ['a', '101010']],
    [4, 'h3', ['b', 'b2', 'c', 'c2', 'd', 'd']],
    [1, 'l1', ['yup', 'aha']],
    [2, 'set1', ['c','d','a','b']],
    [1, 'l2', ['something','now a bit longer and perhaps more interesting']],
    [2, 'set2', ['d','a']],
    [0, 'n1', '-6'],
    [1, 'l3', ['this one is going to be longer -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------', 'a bit more']],
    [2, 'set3', ['b']],
    [2, 'set4', ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']],
    [0, 'n2', '501'],
    [1, 'l4', ['b', 'c', 'd']],
    [2, 'set5', ['100000', '100001', '100002', '100003']],
    [0, 'n3', '500001'],
    [1, 'l5', ['c', 'a']],
    [2, 'set6', ['9999999997', '9999999998', '9999999999']],
    [0, 'n4', '1'],
    [1, 'l6', ['b']],
    [0, 'n5', '1000'],
    [1, 'l7', ['a', 'b']],
    [0, 'n6', '1000000'],
    [0, 'n4b', '1'],
    [1, 'l8', ['c', '1', '2', '3', '4']],
    [1, 'l9', ['10001', '10002', '10003', '10004']],
    [0, 'n6b', '1000000'],
    [3, 'z1', ['a', '1', 'c', '13']],
    [3, 'z2', ['1', '1', '2', '2', '3', '3']],
    [3, 'z3', ['10002', '10001', '10003', '10003']],
    [3, 'z4', ['10000000001', '10000000001', '10000000002', '10000000002', '10000000003', '10000000003']],
];

var parser = new rdb.Parser();
    
function check(got, expected) {
    if(typeof got !== 'object') {
	assert.strictEqual(typeof got, typeof expected);
	assert.strictEqual(got, expected);
	return;
    }

    var i;

    if(got.constructor === Array) {
	assert.strictEqual(expected.constructor, Array);
	assert.strictEqual(got.length, expected.length);
	for(i = 0; i < got.length; ++i) {
	    check(got[i], expected[i]);
	}
	return;
    }

    if(typeof expected === 'string') {
	assert.strictEqual(got.toString(), expected);
	return;
    }

    if(typeof expected === 'object' && expected.constructor === buffer.Buffer) {
	assert.strictEqual(typeof got, 'object');
	assert.strictEqual(got.constructor, buffer.Buffer);
	assert.strictEqual(got.length, expected.length);
	for(i = 0; i < got.length; ++i)
	    assert.strictEqual(got[i], expected[i]);
	return;
    }

    assert.strictEqual(got, expected);
}

var expectedIndex = 0;
parser.on('entity', function(o) {
    assert(expectedIndex < expected.length, 'more entities than expected: '+ o);
    check(o, expected[expectedIndex++]);
});

parser.on('error', function(err) {
    console.error('error', err);
    throw err;
});

parser.on('end', function() {
    assert.strictEqual(expectedIndex, expected.length, 'less entities than expected: '+ expected[expectedIndex]);
});

var s = fs.createReadStream('./tests/test.rdb');
s.pipe(parser);
