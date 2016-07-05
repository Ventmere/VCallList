"use strict"

const Promise = require('bluebird')
const db = require('./db')
const async = require('async')

exports.list = function(opts) {
  const take = parseInt(opts.take) || 100
  const skip = parseInt(opts.skip) || 0
  const keyword = typeof opts.keyword === 'string' 
    ? opts.keyword.trim() 
    : null

  const filter = keyword
    ? { _id: new RegExp(escapeRegexp(keyword)) }
    : undefined

  return db.collection('listitem')
    .find(filter)
    .sort([['_id', 1]])
    .limit(take)
    .skip(skip)
    .toArray()
    .then(docs => {
      const ids = docs.map(doc => doc._id)
      if (ids.length) {
        return db.collection('blacklist')
          .find({ 
            _id: {
              $in: ids
            }
          })
          .toArray()
          .then(blackDocs => {
            blackDocs.forEach(bdoc => {
              docs.find(doc => doc._id === bdoc._id).is_in_blacklist = true
            })
            return docs
          })
      } else {
        return docs
      }
    })
}

function escapeRegexp(s) {
  return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}