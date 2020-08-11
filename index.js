// SPDX-License-Identifier: AGPL-3.0-or-later
const NanoQueue = require('nanoqueue')
const { defer } = require('deferinfer')
const https = require('https')
const { parse } = require('node-html-parser')
const hyperswarm = require('hyperswarm')
const { discoveryKey } = require('hypercore-crypto')
const sbd = require('sbd').sentences
const eos = require('end-of-stream')

const MINGLE_TIMEOUT = 1000 * 5
const DOWNLOAD_TIMEOUT = 1000 * 60
const IDLE_TIMEOUT = 1000 * 60
const REINDEX_THRESHOLD = 1000 * 60 * 60 * 1 // 1hour
const PARALLELIZATION = 6
const PARALLEL_FILES = 5

const MDLINK_EXP = /\[([^\]]+)\]\(([^\)]+)\)/ // eslint-disable-line no-useless-escape

const hyperdrive = require('hyperdrive')
const log = (...args) => console.log(new Date().toJSON(), '[Indexer]', ...args)

async function scrape () {
  const res = await defer(done => https.get('https://userlist.beakerbrowser.com/', done.bind(null, null)))
  log('Connection established')
  const html = await defer(done => {
    let body = ''
    res.on('data', chunk => { body += chunk })
    res.on('end', () => done(null, body))
    res.on('error', err => done(err))
  })
  const root = parse(html)
  // html.match(/<a href="hyper:\/\/([^"]+)".*>/)
  const users = []
  for (const node of root.querySelector('.users.mainlist').childNodes) {
    if (node.nodeType !== 1) continue
    const link = node.querySelector('a')
    const url = link.getAttribute('href')
    const name = link.text
    const peerCount = node.querySelector('.peer-count').getAttribute('data-peer-count')
    const detailsNode = node.querySelector('.details')
    users.push({
      name,
      url,
      peerCount,
      details: detailsNode ? detailsNode.text : null
    })
  }
  return users
}

function resolveKey (url) {
  // dat-dns? What's used in beaker to resolve hyper://?
  if (Buffer.isBuffer(url)) {
    if (url.length === 32) return url
    throw new Error(`Unknown buffer received as URL: ${url.hexSlice()}`)
  }
  const isHexkey = str => typeof str === 'string' && str.length === 64 && str.match(/^[a-f0-9]+$/)
  if (isHexkey(url)) return Buffer.from(url, 'hex')

  const u = new URL(url)
  if (isHexkey(u.host)) return Buffer.from(u.host, 'hex')
  throw new Error('dns lookups not yet implemented', url)
}

class Indexer {
  constructor () {
    this.queue = new NanoQueue(PARALLELIZATION, {
      process: this._process.bind(this),
      oncomplete: log.bind(null, 'Queue finished')
    })
    this._drives = {}
    this._dist = hyperdrive('./dist')
    // this.client = new HyperspaceClient()
    this._lastIndexed = {}
    this._dist.on('ready', async () => {
      console.log(`Database url: hyper://${this._dist.key.hexSlice()}`)
      await this._writeEntry('.nocrawl', 'nocrawl')
      await this._writeEntry('index.json', JSON.stringify({
        title: 'hyperdex database',
        description: 'Contains pre-indexed records ready to be looked up'
      }))
    })
  }

  replicate () {
    const topic = discoveryKey(this._dist.key)
    const swarm = hyperswarm({
      ephemeral: false
      // maxServerSockets: 0
    })
    swarm.on('connection', (socket, info) => {
      log('Connection on distribution topic', socket.remoteAddress)
      const dstream = this._dist.replicate(info.client, { encrypt: true })
      dstream.on('error', err => {
        console.warn('Distribution to peer failed', err)
      })
      dstream.on('end', log.bind(null, 'redistribution complete')) // TODO: add socket.remoteAddr to log?
      dstream.pipe(socket).pipe(dstream)
    })
    swarm.join(topic, { lookup: false, announce: true }, () => {
      log(`Distribution topic joined: hyper://${this._dist.key.hexSlice()}`)
    })
  }

  async ready () { } // return this.client.ready() }

  index (url, force = false) {
    // TODO: if !force refuse to process drives that already were indexed during the last 6 hours
    this.queue.push(url)
  }

  _getDrive (key) {
    let drive = this._drives[key.toString('hex')]
    if (drive) return drive
    const storage = `_cache/${key.toString('hex')}`
    drive = this._drives[key.toString('hex')] = hyperdrive(storage, key, { sparse: true })
    drive.on('error', console.error.bind(null, 'HyperdriveError'))
    // drive.on('ready', console.error.bind(null, 'HyperdriveReady'))
    return drive
  }

  async _releaseDrive (drive) {
    const key = drive.key
    try {
      // await defer(d => drive.destroyStorage(d))
      await defer(d => drive.close(d))
    } catch (err) {
      log('Failed closing drive', err)
    } finally {
      delete this._drives[key.toString('hex')]
    }
  }

  _process (url, done) {
    this._joinSwarm(url)
      .then(done.bind(null, null))
      .catch(err => {
        console.error('Failed processing', err)
        done()
      })
  }

  async _joinSwarm (url) {
    const key = resolveKey(url)
    if (!key) throw new Error('KeyResolutionFailure')
    // TODO: index only if hasn't been indexed within 2hours, use the `about/{drive.key}` index for lookup.
    const stat = await defer(d => this._dist.lstat(`about/${key.hexSlice()}`, (_, s) => d(null, s)))
    if (stat && new Date().getTime() - stat.mtime.getTime() < REINDEX_THRESHOLD) return
    log('Start processing drive', url.toString())

    const drive = this._getDrive(key)
    await defer(d => drive.ready(d))
    const topic = discoveryKey(key)
    const swarm = hyperswarm({
      multiplex: false,
      ephemeral: true
      // maxServerSockets: 0
    })

    const sockets = []
    const uniquePeers = {}
    swarm.on('peer', peer => {
      uniquePeers[peer.host] = true
    })
    let unlock = null
    const lock = defer(d => { unlock = d })

    swarm.on('connection', (socket, info) => {
      log('Connection established', info.peer && info.peer.host)
      sockets.push(socket)
      eos(socket, () => {
        sockets.splice(sockets.indexOf(socket), 1)
      })
      // const { type, client, topics, peer } = info
      // const { host, local, referrer, topic } = peer
      const dstream = drive.replicate(info.client, { live: true, encrypt: true })
      dstream.on('error', err => {
        if (err.message === 'Resource is closed') return
        console.warn('!!! Replication failure', err)
      })
      dstream.on('end', log.bind(null, 'replication finished'))
      dstream.pipe(socket).pipe(dstream)
      // TODO: fetch list of files,
    })

    swarm.join(topic, { lookup: true, announce: false }, () => {
      log('topic joined')
      unlock()
    })
    let about = null
    try {
      let state = 0
      setTimeout(() => {
        if (!state) throw new Error('IDLE_TIMEOUT')
      }, IDLE_TIMEOUT)

      await lock
      state++
      // Not sure if mingling is necessary, trying to avoid empty drive.readdir()
      await defer(d => setTimeout(d, MINGLE_TIMEOUT))
      state++

      // Crawl content
      about = await this._crawlDrive(drive)
      state++
      // Store descriptor
      if (about) {
        about.peers = Object.keys(uniquePeers).length
        about.version = drive.version
        const akey = `about/${drive.key.hexSlice()}`
        let forceWrite = true
        try {
          const prev = await defer(d => this._dist.readFile(`about/${drive.key.hexSlice()}`, d))
          forceWrite = about.version > JSON.parse(prev.toString()).version
        } catch (err) {
          log('No previous entry?', akey, err)
        }
        await this._writeEntry(`about/${drive.key.hexSlice()}`, JSON.stringify(about), forceWrite)
      }
    } catch (err) {
      if (err.message !== 'IDLE_TIMEOUT' || err.message !== 'READDIR_TIMEOUT') throw err
    } finally {
      // Free resources
      await defer(done => swarm.leave(topic, done))
      await this._releaseDrive(drive)
      for (const socket of sockets) socket.end()
    }

    // TODO: write about index.json + nNpeers
    if (about) log('Drive complete and closed', url.toString(), about)
    else log('empty drive skipped')
  }

  async _crawlDrive (drive) {
    log('fetching file-list')
    const list = await defer(done => {
      const timer = setTimeout(() => done(new Error('READDIR_TIMEOUT')), IDLE_TIMEOUT)
      drive.readdir('/', { recursive: true, noMounts: true }, (err, l) => {
        clearTimeout(timer)
        done(err, l)
      })
    })
    log('Filelist received:', list)
    if (!list || !list.length) return null

    // Give people a chance to opt-out of indexing
    if (list.find(file => file.match(/\.nocrawl$/))) return null

    const aggr = { size: 0, files: 0, updatedAt: -1 }
    await defer(indexingDone => {
      const que = new NanoQueue(PARALLEL_FILES, {
        process (task, done) {
          task()
            .then(() => {
              log('Indexing task succeeded')
              done()
            })
            .catch(err => {
              log('Indexing task failed', err)
              done()
            })
        },
        oncomplete () {
          log('All drive tasks complete')
          indexingDone()
        }
      })
      que.push(() => this._writeEntry(`filelists/${drive.key.hexSlice()}`, list.join('\n')))
      for (const file of list) {
        log('Processing file', file)
        if (file.match(/\.(md|txt)$/i)) que.push(() => this._analyzeText(drive, file))
        if (file.match(/^index.json$/i)) que.push(() => this._indexJson(aggr, drive, file))
        // TODO: analyze html + find links
        que.push(() => this._appendUpdates(aggr, drive, file))
      }
    })
    return aggr
  }

  async _indexJson (aggr, drive, file) {
    const { body } = await readRemoteFile(drive, file)
    Object.assign(aggr, JSON.parse(body.toString('utf8')))
  }

  async _analyzeText (drive, file) {
    const { fstat, body } = await readRemoteFile(drive, file)
    if (!body) return
    const text = body.toString('utf8')
    let nLinks = 0
    for (const link of text.matchAll(new RegExp(MDLINK_EXP, 'g'))) {
      if (link[2].match(/^\.?\.?\//)) link[2] = `hyper://${drive.key.hexSlice()}/${link[2]}`
      let url = null
      try {
        url = new URL(link[2])
      } catch (err) {
        // TODO: I __really__ hope this error is generated by a relative path like `href="/about"`
        console.warn('skipping link', err)
      }
      if (!url) continue
      if (url.protocol !== 'hyper:') continue // Ignore non hyperspace linking.
      const key = `backlinks/${url.host}/${drive.key.hexSlice()}_${file.replace(/\//g, '+')}`
      const written = await this._writeEntry(key, `${drive.version}`)
      if (written) nLinks++
      this.index(url)
    }
    let nTokens = 0
    // Analyze and store n-gram index
    const sentences = sbd(text, { sanitize: true })
    for (const sentence of sentences) {
      for (const token of edgeNgram(sentence.replace(MDLINK_EXP, '$1'))) {
        const key = 'ngrams/' + token.split('').join('/') + `/${drive.key.hexSlice()}_${file.replace(/\//g, '+')}`
        const data = {
          date: fstat.mtime.getTime(),
          version: drive.version,
          file: file,
          match: sentence
        }
        const written = await this._writeEntry(key, JSON.stringify(data))
        if (written) nTokens++
      }
    }
    if (nLinks || nTokens) log('Analyzing text: ', nLinks, nTokens, file)
  }

  async _appendUpdates (aggr, drive, file) {
    const fstat = await defer(d => drive.lstat(file, { wait: true }, d))
    aggr.files++
    aggr.size += fstat.size
    if (aggr.updatedAt < fstat.mtime.getTime()) aggr.updatedAt = fstat.mtime.getTime()
    const key = `updates/${fstat.mtime.getTime()}_${drive.key.hexSlice()}_${file.replace(/\//g, '+')}`
    await this._writeEntry(key, `${drive.version}`)
  }

  async _writeEntry (key, value, force = false) {
    if (!force) {
      const entryStat = await defer(d => this._dist.lstat(key, (_, stat) => d(null, stat)))
      if (entryStat) return false
    }
    if (!Buffer.isBuffer(value)) value = Buffer.from(value)
    await defer(d => this._dist.writeFile(key, value, d))
    log('Entry written', key)
    return true
  }
}

async function readRemoteFile (drive, file) {
  const fstat = await defer(d => drive.lstat(file, { wait: true }, d))
  if (!fstat.size) return { fstat, body: null }
  const body = await defer(d => {
    log('downloading...', file)
    const timer = setTimeout(() => d(new Error('DOWNLOAD_TIMEOUT')), DOWNLOAD_TIMEOUT)

    drive.download(file, err => {
      clearTimeout(timer)
      log('download complete', file)
      if (err) return d(err)
      drive.readFile(file, (err, chunk) => {
        if (err) return d(err)
        d(null, chunk.toString('utf8'))
      })
    })
  })
  return { fstat, body }
}

function * ngram (text, min = 3, max = 8) {
  let size = min
  do {
    for (let i = 0; i < text.length - size; i++) {
      const n = text.substr(i, size).toLowerCase()
      if (!n.match(/^[\d\w]+$/)) continue
      yield n
    }
  } while (size++ < max)
}

function * edgeNgram (text, min = 3, max = 8) {
  for (let size = min; size <= max; size++) {
    const exp = new RegExp(`\\b([\\w\\d]{${size}})`, 'g')
    for (const m of text.matchAll(exp)) {
      yield m[1].toLowerCase()
    }
  }
}

module.exports = {
  scrape,
  Indexer,
  ngram,
  edgeNgram
}

// when `node index.js`
if (require.main === module) {
  const { writeFileSync } = require('fs')
  const idxr = new Indexer()
  idxr._dist.on('ready', () => {
    idxr.replicate()
    writeFileSync('database.url', `hyper://${idxr._dist.key.hexSlice()}`)
  })

  if (!process.argv[2]) {
    let drives = []
    const fetchUserDirectory = () => {
      scrape()
        .then(d => {
          drives = d
          drives.sort((a, b) => Math.random() - 0.5)
          // writeFileSync('drives.json', JSON.stringify(drives))
          // await idxr.ready()
          for (const drive of drives) {
            idxr.index(drive.url)
          }
        })
        .catch(err => {
          console.error('Indexing failed', err)
        })

      setTimeout(fetchUserDirectory, 6 * 60 * 60 * 1000)
    }
    fetchUserDirectory()

    const crawl = () => {
      drives.sort((a, b) => Math.random() - 0.5)
      for (const drive of drives) {
        idxr.index(drive.url)
      }
      setTimeout(crawl, 30 * 60 * 1000)
    }
    setTimeout(crawl, 60 * 1000)
  } else if (process.argv[2] !== 'seed') {
    idxr.index(process.argv[2], true)
  }
}
