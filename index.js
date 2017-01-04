var pull = require('pull-stream')
var Write = require('pull-write')
var Obv = require('obv')

module.exports = function (version, map) {
  var since = Obv()
  var env = typeof window == 'object' ? window : self;

  return function (log, name) {

    var db
    function initialize () {
      var req = env.indexedDB.open(log.filename+'/'+name, 1)
      req.onsuccess = function (ev) {
        console.log('opened', name, ev)

        db = ev.target.result
        db.transaction(['meta'],'readonly').objectStore('meta')
        .get('meta')
        .onsuccess = function (ev) {
          console.log('loaded', name, ev.target.result)
          if(!ev.target.result) since.set(-1)
          else if(ev.target.result.version === version) {
            since.set(ev.target.result.since)
          } else {
            var tx = db.transaction(['meta', 'data'])
            tx.deleteDatabase('meta')
            tx.deleteDatabase('data')
            tx.onversionchange = initialize
          }
        }
      }

      req.onupgradeneeded = function (ev) {
        console.log('upgrade needed')
        db = ev.target.result
        db.createObjectStore('meta')
        db.createObjectStore('data')
      }

      req.onerror = function (ev) {
        console.log('ERROR')
        throw new Error('could not load indexdb:'+dir)
      }
    }

    initialize()
    since(function (v) {
      console.log("UPDATE sINCE", v)
    })

    return {
      since: since,
      methods: {get: 'async', read: 'source'},
      createSink: function (cb) {
        return Write(function (batch, cb) {
          var tx = db.transaction(['meta', 'data'], 'readwrite')
          tx.onabort = tx.onerror = function (err) { cb(err || error) }

          function onError (_err) {
            error = _err
            tx.abort()
          }
          tx.oncomplete = function () {
            console.log("WRITTEN", batch)
            since.set(batch.meta.since)
            cb(null, batch.meta.since)
          }
          tx.objectStore('meta').put(batch.meta, 'meta').onerror = onError
          var dataStore = tx.objectStore('data')
          batch.data.forEach(function (data) {
            console.log('batch', data)
            dataStore.put(data.value, data.key).onerror = onError
          })
        }, function (batch, data) {
          console.log("Reduce", batch, data)
          if(!batch) batch = {meta: {version: version, since: data.seq}, data: []}
          var index = map(data.value, data.seq)
          if(index)
            batch.data.push()
          var indexed = map(data.value, data.seq)
          batch.data = batch.data.concat(indexed.map(function (key) { return { key: key, value: data.seq}}))
          //batch[.value.since = Math.max(batch[0].value.since, data.seq)
          batch.meta.since = data.seq
          return batch
        })
      },
      get: function (key, cb) {
        var tx = db.transaction(['data'], 'readonly')
        var req = tx.objectStore('data').get(key)
        console.log('get', key)
        req.onsuccess = function (ev) {
          log.get(ev.target.result, cb)
        }
        req.onerror = function () {
          cb(new Error('key not found:'+key))
        }
      },
      read: function () {
        throw new Error('not yet implemented')
      }
    }
  }
}






