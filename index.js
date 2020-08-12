// SPDX-License-Identifier: AGPL-3.0-or-later
const NanoQueue = require('nanoqueue')
const { defer } = require('deferinfer')
const https = require('https')
const { parse } = require('node-html-parser')
const hyperswarm = require('hyperswarm')
const { discoveryKey } = require('hypercore-crypto')
const eos = require('end-of-stream')
const analyzeText = require('./lib/analyzer_text.js')

const MINGLE_TIMEOUT = 1000 * 5
const DOWNLOAD_TIMEOUT = 1000 * 60
const IDLE_TIMEOUT = 1000 * 60
const REINDEX_THRESHOLD = 1000 * 60 * 60 * 1 // 1hour
const PARALLELIZATION = 6
const PARALLEL_FILES = 5

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

      // Store descriptor (done here because of uniquePeers availablility)
      if (about) {
        about.peers = Object.keys(uniquePeers).length
        about.version = drive.version
        await this._writeEntry(`about/${drive.key.hexSlice()}`, JSON.stringify(about), true)
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

    // Ignore drives which whoose version has already been processed.
    try {
      const prevState = await defer(d => this._dist.readFile(`about/${drive.key.hexSlice()}`, d))
      if (drive.version <= JSON.parse(prevState.toString()).version) {
        log('=== Drive is up-to-date', drive.key.hexSlice())
        return null
      }
    } catch (err) {
      log('No previous entry?', drive.key.hexSlice(), err)
    }

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

      const context = {
        drive,
        log,
        readRemoteFile,
        driveInfo: aggr,
        writeEntry: this._writeEntry.bind(this),
        index: this.index.bind(this)
      }

      for (const file of list) {
        log('Processing file', file)
        const boundCtx = {
          ...context,
          setPreview: this._setPreview.bind(this, drive, file)
        }
        if (file.match(/^index.json$/i)) que.push(() => this._indexJson(aggr, drive, file))

        // Perform analysis based on file-extension
        if (file.match(/\.(md|txt)$/i)) que.push(() => analyzeText(boundCtx, file))
        // if (file.match(/\.html?$/i)) que.push(() => analyzeHTML(context, file))
        // if (file.match(/\.(png|jpe?g|gif)$/i)) que.push(() => analyzeImage(context, file))
        // if (file.match(/\.svg$/i)) que.push(() => analyzeVectorImage(context, file))

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

  async _appendUpdates (aggr, drive, file) {
    const fstat = await defer(d => drive.lstat(file, { wait: true }, d))
    aggr.files++
    aggr.size += fstat.size
    const remoteTime = fstat.mtime.getTime()
    const subPath = [fstat.mtime.getFullYear(), fstat.mtime.getMonth(), fstat.mtime.getDate()].join('/')
    const key = `updates/${subPath}/${drive.key.hexSlice()}_${file.replace(/\//g, '+')}`
    if (remoteTime > new Date().getTime()) throw new Error('Drive contains files from the future', fstat.mtime, key)
    if (aggr.updatedAt < remoteTime) aggr.updatedAt = remoteTime
    // await this._writeEntry(key, `${drive.version}`)
    await this._symlink(previewPath(drive, file, drive.version), key)
  }

  async _writeEntry (key, value, force = false) {
    if (!force) {
      const entryStat = await defer(d => this._dist.lstat(key, (_, stat) => d(null, stat)))
      if (entryStat) return false
    }
    if (!Buffer.isBuffer(value)) value = Buffer.from(value)
    await defer(d => this._dist.writeFile(key, value, d))
    log('+++ Entry written', key)
    return true
  }

  async _symlink (target, linkname, force = false) {
    const entryStat = await defer(d => this._dist.lstat(linkname, (_, stat) => d(null, stat)))
    if (entryStat) {
      if (!force) return false
      await defer(cb => this._dist.unlink(linkname, cb))
    }
    await defer(cb => this._dist.symlink(target, linkname, cb))
    log(`+++ symlinked ${linkname} => ${target}`)
    return true
  }

  async _setPreview (drive, file, data) {
    const link = previewPath(drive, file)
    const content = previewPath(drive, file, drive.version)
    // Use symlink hack to store version information and avoid having to compare contents.
    // any data added to the drive is forever stuck in history anyway so no point to clean up old-versions.
    const written = await this._writeEntry(content, data)
    if (written) {
      const linked = await this._symlink(content, link, true)
      return linked
    } else return false
  }
}

function previewPath (drive, file, version = null) {
  const k = `previews/${drive.key.hexSlice(0, 1)}/${drive.key.hexSlice(1, 2)}/${drive.key.hexSlice(2)}/${file}`
  if (version !== null) return `${k}_${version}`
  return k
}

async function readRemoteFile (drive, file, maxSize = 1024 * 1024 * 10) {
  const fstat = await defer(d => drive.lstat(file, { wait: true }, d))
  if (!fstat.size || fstat.size > maxSize) return { fstat, body: null }
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

module.exports = {
  scrape,
  Indexer
}

// when `node index.js`
if (require.main === module) {
  const { writeFileSync } = require('fs')
  const idxr = new Indexer()
  const distribute = () => {
    idxr._dist.on('ready', () => {
      idxr.replicate()
      writeFileSync('database.url', `hyper://${idxr._dist.key.hexSlice()}`)
    })
  }

  if (!process.argv[2]) {
    distribute()
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
  } else if (process.argv[2] === 'readdir') {
    idxr._dist.readdir(process.argv[3], { recursive: true, noMounts: true }, (err, res) => {
      if (err) return log('readdir failed!', err)
      for (const line of res) console.log(line)
    })
  } else if (process.argv[2] === 'scrape') {
    const drives = scrape()
    writeFileSync('drives.json', JSON.stringify(drives))
  } else if (process.argv[2] === 'seed') {
    distribute()
  } else {
    distribute()
    idxr.index(process.argv[2], true)
  }
}
