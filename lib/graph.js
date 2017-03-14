'use strict'

const Promise = require('bluebird')
Promise.config({
  warnings: {
    wForgottenReturn: false
  }
})

// const Readable = require('stream').Readable
// const FlushWritable = require('flushwritable')
// const crypto = require('crypto')
// const toArray = require('stream-to-array')
const lruCache = require('lru-cache')

const defaultGraphOptions = {
  graphName: 'graph',
  concurrency: 10,
  cacheSize: 400
}

let Graph = function (connOpts, graphOptions) {
  let conf = Object.assign({}, defaultGraphOptions, graphOptions)
  let nodeTable = `${conf.graphName}_nodes`
  let edgeTable = `${conf.graphName}_edges`
  let r = require('rethinkdbdash')(Object.assign({silent: true}, connOpts))
  let isCaching = conf.cacheSize > 0
  let cache

  let initGraph = Promise.coroutine(function *() {
    var promises = []
    var tableList = yield r.tableList().run()

    // Create tables if they don't exist.
    if (tableList.includes(nodeTable) === false) {
      promises.push(r.tableCreate(nodeTable).run())
    }
    if (tableList.includes(edgeTable) === false) {
      promises.push(r.tableCreate(edgeTable).run())
    }

    yield Promise.all(promises)
    promises = []

    let queries = {
      nodeIndexList: r.table(nodeTable).indexList().run(),
      edgeIndexList: r.table(edgeTable).indexList().run()
    }

    var {nodeIndexList, edgeIndexList} = yield Promise.props(queries)

    // Create indexes if they don't exist.
    if (nodeIndexList.includes('type') === false) {
      promises.push(r.table(nodeTable).indexCreate('type', r.row('type')))
    }

    if (edgeIndexList.includes('type') === false) {
      promises.push(r.table(edgeTable).indexCreate('type', r.row('type')))
    }

    yield Promise.all(promises)
    promises = []

    yield Promise.all([
      r.table(nodeTable).indexWait(),
      r.table(edgeTable).indexWait()
    ])

    if (isCaching) {
      cache = lruCache({
        max: conf.cacheSize
      })

      // r.table(nodeTable).changes().run(function (err, cursor) {
      //   if (err) { console.log(err) }
      //   cursor.each(function (err, row) {
      //     if (err) {
      //       // console.log(err)
      //     } else {
      //       let newVal = row.new_val
      //       cache.del(fileKey)
      //     }
      //   })
      // })
      cache.get()
    }
  })

  return Object.freeze({
    initGraph
  })
}

module.exports = Graph
