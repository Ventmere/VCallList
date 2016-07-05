"use strict"

const MongoClient = require('mongodb').MongoClient
const Url = require('./config.json').db
const Promise = require('bluebird')

let Db = null

exports.connect = function () {
  return new Promise((resolve, reject) => {
    MongoClient.connect(Url, function(err, db) {
      if (err) {
        return reject(err)
      }
      Db = db
      return resolve()
    })
  })
}

exports.close = function() {
  if (Db) {
    Db.close()
    Db = null
  }
}

exports.collection = function(name) {
  return Db ? Db.collection(name) : null
}