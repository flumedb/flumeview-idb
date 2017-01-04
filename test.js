
var Flume = require('flumedb')
var Log = require('flumelog-idb')
var Index = require('./')

var db = Flume(Log('test-flumelog-idb_'+Date.now()))
  .use('index', Index(1, function (e) {
    return [e.key]
  }))

require('test-flumeview-index')(db)
