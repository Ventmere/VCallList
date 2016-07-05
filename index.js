"use strict"

const express = require('express')
const bodyParser = require('body-parser')
const cors = require('cors')

const db = require('./db')
const ListFile = require('./listfile')
const ListItem = require('./listitem')

const app = express()

app.use(cors())
app.use(bodyParser.json())

app.get('/', (req, res) => {
  res.send('OK')
})

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
    ListItem.list(req.query)
      .then(result => {
        res.send(result)
      }, err => {
        next(err)
      })
  })

app.route('/api/listitem/:id/note')
  .post((req, res, next) => {

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