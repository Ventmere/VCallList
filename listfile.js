"use strict"

const Promise = require('bluebird')
const formidable = require('formidable')
const csv = require('csv')
const db = require('./db')
const async = require('async')

exports.processFiles = function(req) {
  return new Promise((resolve, reject) => {
    const form = new formidable.IncomingForm()
    const tasks = []
    const results = []
    const errors = []
    form.onPart = function(part) {
      if (!part.filename) {
        return form.handlePart(part);
      }

      //handle part
      tasks.push(
        processFile(part)
          .then(result => {
            results.push({
              filename: part.filename,
              result
            })
          }, error => {
            errors.push(error);
          })
      )
    }
    form
      .on('error', reject)
      .on('abort', () => resolve(results))
      .on('end', () => {
        if (errors.length) {
          return reject(errors[0]);
        }

        Promise.all(tasks).then(() => {
          resolve(results)
        }, reject);
      });
    form.parse(req);
  })
}

const BATCH_SIZE = 5000 //insert batch size
function processFile(stream) {
  return new Promise((resolve) => {
    const parser = csv.parse()
    const startTime = process.hrtime()
    const result = {
      type: 'unknown',
      error: null,
      count: 0,
      insert: 0,
      update: 0
    }
    let processFunction = null;
    let done = false //all parsed
    let queue = []
    let working = false //is async.doUntil running?
    const doWork = () => {
      if (working) {
        return
      }
      working = true
      async.doUntil((next) => {
        let batch
        if (queue.length > BATCH_SIZE) {
          batch = queue.slice(0, BATCH_SIZE)
          queue = queue.slice(BATCH_SIZE)
        } else {
          batch = queue
          queue = []
        }
        processFunction(batch, (err, stats) => {
          if (err) {
            return next(err)
          }
          result.count += batch.length
          result.insert += stats.insert
          result.update += stats.update
          //console.log('done batch', batch.length)
          return next()
        })
      }, () => queue.length === 0 || !!result.error, (err) => {
        if (err) {
          return emitError(err)
        }
        if (done) {
          return finish(result)
        }
        working = false
      })
    }

    const finish = () => {
      const diff = process.hrtime(startTime);
      result.time = (diff[0] * 1e9 + diff[1]) / 1e6
      return resolve(result)
    }

    const emitError = (err) => {
      result.error = {
        message: err.message,
        stack: err.stack
      }
      return resolve(result)
    }

    parser.on('readable', function () {
      if (result.error) {
        return
      }

      let record;
      while (record = parser.read()) {
        if (result.type === 'unknown') {
          const type = identifyType(record)
          if (!type) {
            return this.emit('error', new Error('Unknown file format.'))
          } else {
            result.type = type
            processFunction = type === 'list'
              ? processListItem
              : processBlackListItem
          }
        }
        queue.push(record)
      }
      if (queue.length >= BATCH_SIZE) {
        doWork()
      }
    })

    parser.on('error', emitError)

    parser.on('finish', () => {
      done = true
      if (!working && queue.length === 0) {
        return finish(result)
      } else if (!working) {
        doWork()
      }
    })

    stream.pipe(parser).on('error', emitError)
  })
}

function processListItem(records, done) {
  const stats = {
    update: 0,
    insert: 0
  }
  const idSet = new Set()
  const rows = records.map(record => {
    const phone = record[6]
    idSet.add(phone)
    return {
      _id: phone,
      first_name: record[0],
      last_name: record[1],
      address: record[2],
      city: record[3],
      province: record[4],
      postal: record[5],
    }
  })

  async.waterfall([
    (next) => {
      db.collection('listitem').find({ 
        _id: {
          $in: Array.from(idSet.values())
        }
      }, {
        _id: 1
      }).toArray(next)
    },
    (docs, next) => {
      const insertIdSet = new Set()
      const updateIdSet = new Set()

      docs.forEach(doc => {
        updateIdSet.add(doc._id)
      })

      idSet.forEach(id => {
        if (!updateIdSet.has(id)) {
          insertIdSet.add(id)
        }
      })

      stats.insert += insertIdSet.size
      stats.update += updateIdSet.size

      async.parallel([
        (next) => {
          if (insertIdSet.size) {
            db.collection('listitem')
              .insert(rows.filter(row => insertIdSet.has(row._id)), next)
          } else {
            db.collection('listitem')
              .deleteMany({ 
                _id: {
                  $in: Array.from(updateIdSet.values())
                }
              })
              .then(() => 
                db.collection('listitem')
                  .insert(rows.filter(row => updateIdSet.has(row._id)))
              )
              .then(() => next())
              .catch((err) => next(err))
          }
        },
        (next) => {
          next()
        }
      ], (err) => {
        if (err) {
          return next(err)
        }
        return next(null, stats)
      })
    }
  ], done)
}

function processBlackListItem(records, done) {
  const stats = {
    update: 0,
    insert: 0
  }

  const idSet = new Set()
  const rows = records.map(record => {
    const id = record[0] + '' + record[1]
    idSet.add(id)
    return {
      _id: id
    }    
  })

  async.waterfall([
    (next) => {
      db.collection('blacklist').find({ 
        _id: {
          $in: Array.from(idSet.values())
        }
      }, {
        _id: 1
      }).toArray(next)
    },
    (docs, next) => {
      const insertIdSet = new Set()
      const updateIdSet = new Set()

      docs.forEach(doc => {
        updateIdSet.add(doc._id)
      })

      idSet.forEach(id => {
        if (!updateIdSet.has(id)) {
          insertIdSet.add(id)
        }
      })

      stats.insert += insertIdSet.size
      stats.update += updateIdSet.size

      async.parallel([
        (next) => {
          if (insertIdSet.size) {
            db.collection('blacklist')
              .insert(rows.filter(row => insertIdSet.has(row._id)), next)
          } else {
            db.collection('blacklist')
              .deleteMany({ 
                _id: {
                  $in: Array.from(updateIdSet.values())
                }
              })
              .then(() => 
                db.collection('blacklist')
                  .insert(rows.filter(row => updateIdSet.has(row._id)))
              )
              .then(() => next())
              .catch((err) => next(err))
          }
        },
        (next) => {
          next()
        }
      ], (err) => {
        if (err) {
          return next(err)
        }
        return next(null, stats)
      })
    }
  ], done)
}

function identifyType(row) {
  if (!Array.isArray(row)) {
    return null
  }

  if (row.length === 7) {
    return 'list'
  }

  if (row.length === 2) {
    return 'blacklist'
  }

  return null
}