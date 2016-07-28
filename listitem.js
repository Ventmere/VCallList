"use strict"

const Promise = require('bluebird')
const db = require('./db')
const async = require('async')

exports.count = function() {
  return db.collection('listitem').count()
}

exports.list = function(opts) {
  const take = parseInt(opts.take) || 100
  const skip = parseInt(opts.skip) || 0

  const keywordTypes = [
    '_id',
    'first_name',
    'last_name',
    'address',
    'city',
    'province',
    'postal'
  ]

  const keys = opts.keyword_key || '_id'
  const values = opts.keyword
  let filter = undefined

  if (typeof keys === 'string' && typeof values === 'string') {
    const key = keys.trim()
    const value = values.trim()
    if (key && value) {
      if (keywordTypes.indexOf(key) === -1) {
        return Promise.reject(new Error('Invalid keyword key.'))
      }
      filter = key ? {
        [key]: new RegExp(escapeRegexp(value), 'i')
      } : undefined
    }
  } else if (Array.isArray(keys) && Array.isArray(values)) {
    if (keys.length === values.length) {
      const fkeys = keys.map(k => k.trim()).filter(k => !!k && keywordTypes.indexOf(k) !== -1)
      const fvalues = values.map(v => v.trim()).filter(v => !!v).map(v => new RegExp(escapeRegexp(v), 'i'))
      if (fkeys.length !== fvalues.length) {
        return Promise.reject(new Error(`Some keywords are invalid, accepted: ${fkeys.join(', ')}, submitted: ${keys.join(', ')}`))
      }
      filter = fkeys.reduce((f, k, i) => {
        f[k] = fvalues[i]
        return f
      }, {})
    } else {
      return Promise.reject(new Error('Invalid keyword pairs.'))
    }
  }

  return db.collection('listitem')
    .find(filter)
    .sort([['_id', 1]])
    .limit(take)
    .skip(skip)
    .toArray()
    .then(docs => {
      const ids = docs.map(doc => doc._id)
      if (ids.length) {
        return Promise.all([
            db.collection('blacklist')
              .find({ 
                _id: {
                  $in: ids
                }
              })
              .toArray(),
            db.collection('listitem_data')
              .find({ 
                _id: {
                  $in: ids
                }
              })
              .toArray()
          ])
          .then(([blackDocs, dataDocs]) => {
            blackDocs.forEach(bdoc => {
              docs.find(doc => doc._id === bdoc._id).is_in_blacklist = true
            })
            dataDocs.forEach(ddoc => {
              docs.find(doc => doc._id === ddoc._id).data = ddoc.data || {}
            })
            return docs
          })
      } else {
        return docs
      }
    })
}

exports.getData = function(phone) {
  return db.collection('listitem_data')
    .find({_id:phone})
    .limit(1)
    .next()
    .then(doc => {
      return doc ? doc.data || {} : {}
    })
}

exports.putData = function(phone, data) {
  return db.collection('listitem_data')
    .findOneAndReplace({_id:phone}, {data: data || {}}, {upsert:true})
    .then(() => exports.getData(phone))
}

function escapeRegexp(s) {
  return s.replace(/[-\/\\^$*+?.()|[\]{}]/g, '\\$&');
}