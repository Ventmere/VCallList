"use strict"

const config = require('./config')

const express = require('express')
const bodyParser = require('body-parser')
const basicAuth = require('basic-auth-connect')
const cors = require('cors')

const db = require('./db')
const ListFile = require('./listfile')
const ListItem = require('./listitem')

const app = express()

app.use(cors())
app.use(bodyParser.json())
app.use(basicAuth(config.username, config.password))

app.use(express.static('public'))

app.route('/api/listfile')
  .post((req, res, next) => {
    ListFile.processFiles(req)
      .then(result => {
        res.send(result)
      }, err => {
        next(err)
      })
  })

app.route('/api/listitem')
  .get((req, res, next) => {
    Promise.all([
        ListItem.list(req.query),
        ListItem.count()
      ])
      .then(results => {
        res.send({
          data: results[0],
          count: results[1]
        })
      }, err => {
        next(err)
      })
  })

app.route('/api/listitem/:id/data')
  .get((req, res, next) => {
    const phone = req.params.id
    ListItem.getData(phone)
      .then(result => {
        res.send(result)
      }, err => {
        next(err)
      })
  })
  .put((req, res, next) => {
    const phone = req.params.id
    const data = req.body
    ListItem.putData(phone, data)
      .then(result => {
        res.send(result)
      }, err => {
        next(err)
      })
  })

console.log(`Connecting to db...`)
db.connect()
  .then(() => {
    const server = app.listen(3000, (server) => {
      console.log('Listening on port 3000.')
    })
    server.timeout = 1800 * 1000
  })
  .catch(e => {
    console.error(e)
  })