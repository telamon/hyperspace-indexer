// SPDX-License-Identifier: AGPL-3.0-or-later
const NanoQueue = require('nanoqueue')
const { defer } = require('deferinfer')
const https = require('https')
const { parse } = require('node-html-parser')
const hyperswarm = require('hyperswarm')
const { discoveryKey } = require('hypercore-crypto')
const eos = require('end-of-stream')
const moment = require('moment')
const analyzeText = require('./lib/analyzer_text.js')
const analyzeImage = require('./lib/analyzer_image.js')

const MINGLE_TIMEOUT = 1000 * 5
const DOWNLOAD_TIMEOUT = 1000 * 60 * 2 // Wait 2 minutes for download to succeed
const IDLE_TIMEOUT = 1000 * 30
const REINDEX_THRESHOLD = 1000 * 60 * 20 // 1hour, not really needed due to createDiffStream()
const PARALLELIZATION = 5
const PARALLEL_FILES = 10

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
    if (this._drives[key.toString('hex')]) return log('already being processed')
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
  }

  async _crawlDrive (drive) {
    // TODO: add diffStream rollback to retry failed files

    try {
      // read remote will fetch fstat and hopefully quickreturn,
      // if the instruction suceeds then a .nocrawl file exists
      // we'll crawl into another direction.
      await readRemoteFile(drive, '.nocrawl')
      log('Drive politely skipped due to ".nocrawl" request')
      return null
    } catch (err) {
      if (err.code !== 'ENOENT') throw err
    }

    log('fetching file-list')
    const prevState = {
      version: 0,
      size: 0,
      files: 0,
      updatedAt: -1
    }
    try {
      const info = await defer(d => this._dist.readFile(`about/${drive.key.hexSlice()}`, d))
      Object.assign(prevState, JSON.parse(info.toString()))
      if (drive.version <= prevState.version) {
        log('=== Drive is up-to-date', drive.key.hexSlice())
        return null
      }
    } catch (err) {
      if (err.code === 'ENOENT') log('New drive detected', drive.key.hexSlice())
      else throw err
    }

    const list = []
    const stream = drive.createDiffStream(prevState.version, '/')
    let timer = null
    const readdir = defer(d => {
      // Let the race begin
      timer = setTimeout(d.bind(null, new Error('READDIR_TIMEOUT')), IDLE_TIMEOUT)
      eos(stream, d)
    })

    stream.on('data', entry => {
      clearTimeout(timer)
      // console.log(entry.seq, entry.type, entry.name, entry.value.size)
      // TODO: .goto format has 0 size, don't know how it works yet but it's gonna be skipped here.
      if (entry.type === 'put' && entry.value.isFile() && entry.value.size) list.push(entry)
    })
    await readdir
    log('Filelist received:', list.map(e => e.name))
    if (!list || !list.length) return null

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

      const context = {
        drive,
        log,
        readRemoteFile,
        driveInfo: prevState,
        writeEntry: this._writeEntry.bind(this),
        index: this.index.bind(this)
      }

      for (const entry of list) {
        const file = entry.name
        const seq = entry.seq
        log('Processing file', file)

        const boundCtx = {
          ...context,
          seq,
          setPreview: this._setPreview.bind(this, drive, file, seq)
        }
        if (file.match(/^index.json$/i)) que.push(() => this._indexJson(prevState, drive, file))

        // Perform analysis based on file-extension
        if (file.match(/\.(md|txt)$/i)) que.push(() => analyzeText(boundCtx, file))
        // if (file.match(/\.html?$/i)) que.push(() => analyzeHTML(context, file))
        if (file.match(/\.(png|jpe?g|svg|webp)$/i)) que.push(() => analyzeImage(boundCtx, file))
        // if (file.match(/\.svg$/i)) que.push(() => analyzeVectorImage(context, file))

        // TODO: analyze html + find links
        que.push(() => this._appendUpdates(boundCtx, file))
      }
    })

    // TODO: have to put readdir('/') op back for this..
    // filelists/ index indexes all files available, not just those analyzed with previews.
    // que.push(() => this._writeEntry(`filelists/${drive.key.hexSlice()}`, list.map(n => n.name).join('\n')))
    return prevState
  }

  async _indexJson (aggr, drive, file) {
    const { body } = await readRemoteFile(drive, file)
    const { version, size, files, updatedAt } = aggr
    Object.assign(
      aggr,
      JSON.parse(body.toString('utf8')),
      { version, size, files, updatedAt } // prevent accidental owerwrite
    )
  }

  async _appendUpdates ({ driveInfo, drive, seq }, file) {
    const fstat = await defer(d => drive.lstat(file, { wait: true }, d))
    driveInfo.files++
    driveInfo.size += fstat.size
    // We're only interested in media atm.
    if (!file.match(/\.(md|html|txt|png|jpe?g|svg|webm|gif|mpe?g|ogg|mp3)$/i)) return // Skip update-records for non-analyzed files.
    const remoteTime = fstat.mtime.getTime()
    // const subPath = moment(fstat.mtime).format('YYYY/MM/DD') // Skipping this for now.
    const key = `updates/${remoteTime}_${drive.key.hexSlice()}_${file.replace(/\//g, '+')}` // TODO: prolly safer to use \u0001 instead of '+'
    if (remoteTime > new Date().getTime()) throw new Error('Drive contains files from the future', fstat.mtime, key)
    if (driveInfo.updatedAt < remoteTime) driveInfo.updatedAt = remoteTime
    await this._writeEntry(key, `${seq}`)
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

  async _setPreview (drive, file, seq, data) {
    const link = previewPath(drive, file)
    const content = previewPath(drive, file, seq)
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

async function readRemoteFile (drive, file, maxSize = 1024 * 1024 * 5, raw = false) {
  // Fetch the stat first
  const fstat = await defer(d => {
    const timer = setTimeout(() => d(new Error('DOWNLOAD_TIMEOUT')), DOWNLOAD_TIMEOUT)
    drive.lstat(file, { wait: true }, (err, stat) => {
      clearTimeout(timer)
      d(err, stat)
    })
  })

  // Return stat if body is empty, oversize body is treated as empty file: "ignored".
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
        d(null, raw ? chunk : chunk.toString('utf8'))
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
      // drives.sort((a, b) => Math.random() - 0.5)
      for (const drive of drives) {
        idxr.index(drive.url)
      }
      setTimeout(crawl, 30 * 60 * 1000)
    }
    setTimeout(crawl, 60 * 1000)
    // All the additional CLI commands here were added in hindsight. todo: refactor.
  } else if (process.argv[2] === 'ls') {
    idxr._dist.readdir(process.argv[3] || '/', { recursive: true, noMounts: true }, (err, res) => {
      if (err) return log('readdir failed!', err)
      for (const line of res) console.log(line)
      console.log('Drive version', idxr._dist.version)
    })
  } else if (process.argv[2] === 'cat') {
    idxr._dist.readFile(process.argv[3], (err, res) => {
      if (err) return log('cat failed!', err)
      console.log(res.toString('utf8'))
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
