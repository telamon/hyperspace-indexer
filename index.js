// SPDX-License-Identifier: AGPL-3.0-or-later
const NanoQueue = require('nanoqueue')
const { defer } = require('deferinfer')
const https = require('https')
const { parse } = require('node-html-parser')
const hyperswarm = require('hyperswarm')
const { discoveryKey } = require('hypercore-crypto')
const eos = require('end-of-stream')
// const moment = require('moment')
const analyzeText = require('./lib/analyzer_text.js')
const analyzeImage = require('./lib/analyzer_image.js')
const OFFLINE = process.env.OFFLINE || false
/*
 * Amount of drives to index in parallel.
 * This setting trips up OS-resource limits fast.
 * ( Set it to `1` during debugging to avoid hair-loss )
 */
const PARALLELIZATION = 9

/*
 * Amount of files to be analyze in parallel for each drive.
 */
const PARALLEL_FILES = 10

/*
 * Maximum amount of errors to tolerate before skipping a drive
 */
const DRIVE_FAIL_THRESHOLD = 10

/*
 * Wait time to discover new drive versions on topic join.
 */
const MINGLE_TIMEOUT = OFFLINE ? 0 : 1000 * 20

/*
 * Wait msecs before giving up on downloading a file.
 */
const DOWNLOAD_TIMEOUT = 1000 * 120

/*
 * How long to wait for peer discovery and connection
 */
const IDLE_TIMEOUT = 1000 * 60

/*
 * Once a drive has been indexed, give it REINDEX_THRESHOLD time to cool off
 * before allowing it to be checked for updates again.
 */
const REINDEX_THRESHOLD = 1000 * 60 * 20

/*
 * Maximum amount of time to spend indexing one drive.
 */
const DRIVE_TIMEOUT = 1000 * 60 * 5

const hyperdrive = require('hyperdrive')
const log = (...args) => console.log(new Date().toJSON(), '[Indexer]', ...args)

class Indexer {
  constructor () {
    this.queue = new NanoQueue(PARALLELIZATION, {
      process: this._process.bind(this),
      oncomplete: this._queDone.bind(this)
    })

    this._drives = {}
    this._progressOffset = 0
    this._progressEnqueued = 0
    this._progressProcessed = 0
    this._initializedAt = new Date().getTime()
    this._dist = hyperdrive('./dist')
    this._dist.on('ready', async () => {
      console.log(`Database url: hyper://${this._dist.key.hexSlice()}`)
      await this._writeEntry('.nocrawl', 'nocrawl')
      await this._writeEntry('index.json', JSON.stringify({
        title: 'hyperdex database',
        description: 'Contains pre-indexed records ready to be looked up'
      }))
    })

    // Initialize swarm
    const swarm = hyperswarm({
      ephemeral: true,
      maxPeers: 50,
      // maxServerSockets: 0
      multiplex: true
    })
    swarm.on('peer', this._onSwarmPeer.bind(this))
    swarm.on('connection', this._onSwarmConnection.bind(this))
    this._swarmHandlers = {}
    // § this._sockets = []
    this.swarm = swarm
  }

  logProgress () {
    if (!this._startedAt) return log('[PROGRESS] indexing session not active')
    const total = this._progressEnqueued - this._progressOffset
    let progress = -1
    if (total && !Number.isNaN(total)) {
      const nDone = this._progressProcessed - this._progressOffset
      progress = Math.round((nDone / total) * 10000) / 100
    }

    const sessionDuration = Math.round((new Date().getTime() - this._startedAt.getTime()) / (1000 * 60) * 100) / 100
    const uptime = Math.round((new Date().getTime() - this._initializedAt) / (1000 * 60) * 100) / 100

    log(`[PROGRESS] ${progress}% (${this._progressProcessed}/${this._progressEnqueued}), db-version: ${this._dist.version}, session time: ${sessionDuration}min, uptime: ${uptime}min`)
  }

  _queDone () {
    const minutes = (new Date().getTime() - this._startedAt.getTime()) / (1000 * 60)
    this._startedAt = null
    this.logProgress()
    log('Queue finished', this._dist.version, `took ${minutes} minutes`)
  }

  _joinTopic (topic, handlers) {
    if (OFFLINE) return Promise.resolve()
    this._swarmHandlers[topic.hexSlice()] = handlers
    return defer(unlock => {
      const idleTimer = setTimeout(unlock.bind(null, new Error('IDLE_TIMEOUT')), IDLE_TIMEOUT)
      this.swarm.join(topic, { lookup: true, announce: false }, () => {
        clearTimeout(idleTimer)
        unlock()
      })
    })
  }

  _leaveTopic (topic) {
    if (OFFLINE) return Promise.resolve()
    delete this._swarmHandlers[topic.hexSlice()]
    return defer(done => this.swarm.leave(topic, err => {
      log('Topic left', topic.hexSlice(), err)
      // § for (const socket of sockets) socket.end()
      done(err)
    }))
  }

  _onSwarmPeer (peer) {
    const handlers = this._swarmHandlers[peer.topic.hexSlice()]
    if (!handlers) throw new Error('NoHandlersFound')
    if (typeof handlers.onpeer === 'function') handlers.onpeer(peer)
  }

  _onSwarmConnection (socket, info) {
    // const { host, local, referrer, topic } = peer
    if (info.peer.host === '35.209.63.38' || info.peer.host === '35.209.182.2') return // Ignore hyperdivision dht-crawlers
    if (!info.client) {
      log('BUG! We should be in client-mode')
      process.exit(1)
    }
    log('Connection established', info.peer && info.peer.host)
    // § sockets.push(socket)
    eos(socket, err => {
      log('Socket Closed', err && err.message === 'Readable stream closed before ending' ? 'read error' : err)
      // § const idx = sockets.indexOf(socket)
      // § if (~idx) sockets.splice(idx, 1)
    })

    const uniqueTopics = Object.values(info.topics.reduce((h, t) => {
      h[t.hexSlice()] = t
      return h
    }, {}))

    for (const topic of uniqueTopics) {
      const handlers = this._swarmHandlers[topic.hexSlice()]
      if (handlers && typeof handlers.onconnect === 'function') handlers.onconnect(socket, info.peer)
    }
  }

  /* This method distributes the database
   * by joining the database's topic,
   * if you want to create a replication stream for the database
   * use indexer._dist.replicate() for now.
   */
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

  async ready () {
    return defer(d => this._dist.ready(d))
  }

  index (url, force = false) {
    // TODO: if !force refuse to process drives that already were indexed during the last 6 hours
    if (!this._startedAt) {
      this._startedAt = new Date()
      this._progressOffset = this._progressProcessed
    }
    this._progressEnqueued++
    this.queue.push(url)
  }

  _getDrive (key) {
    let drive = this._drives[key.toString('hex')]
    if (drive) return drive
    const storage = `_cache/${key.toString('hex')}`
    drive = this._drives[key.toString('hex')] = hyperdrive(storage, key, { sparse: true })
    drive.on('error', console.error.bind(null, 'HyperdriveError'))
    return defer(d => drive.ready(d))
      .then(() => drive)
  }

  async _releaseDrive (drive) {
    const key = drive.key
    try {
      // await defer(d => drive.destroyStorage(d))
      await defer(d => drive.close(d))
    } catch (err) {
      return log('Failed closing drive', err)
    } finally {
      delete this._drives[key.toString('hex')]
    }
    log('Drive closed', key.hexSlice())
  }

  _process (url, done) {
    log('^ SLOT allocated', this._dist.version, url)
    this.logProgress()
    this._processURL(url)
      .then(() => {
        log('$ SLOT freed success', this._dist.version, url)
        this._progressProcessed++
        this.logProgress()
      })
      .then(done.bind(null, null))
      .catch(err => {
        console.error('$ SLOT freed due to err', this._dist.version, err)
        this._progressProcessed++
        this.logProgress()
        done()
      })
  }

  async _processURL (url) {
    const key = resolveKey(url)
    if (!key) throw new Error('KeyResolutionFailure')
    // TODO: index only if hasn't been indexed within 2hours, use the `about/{drive.key}` index for lookup.
    const stat = await defer(d => this._dist.lstat(`about/${key.hexSlice()}`, (_, s) => d(null, s)))
    if (stat && new Date().getTime() - stat.mtime.getTime() < REINDEX_THRESHOLD) return log('Drive is cooling')
    log('Start processing drive', url.toString())
    if (this._drives[key.toString('hex')]) return log('already being processed')
    const drive = await this._getDrive(key)
    const topic = discoveryKey(key)
    const uniquePeers = {}
    const swarmJoinedLock = this._joinTopic(topic, {
      onpeer (peer) {
        uniquePeers[peer.host] = true
      },
      onconnect (socket, peer) {
        log('onconnect', peer.host)
        const dstream = drive.replicate(true, { live: false, encrypt: true })
        dstream.on('error', err => {
          if (err.message === 'Resource is closed') log('!!! Replication failure', peer.host, err.message)
          else console.warn('!!! Replication failure', url.toString(), peer.host, err)
        })
        dstream.on('close', log.bind(null, 'drive-replication stream closed'))
        dstream.pipe(socket).pipe(dstream)
      }
    })

    log('Swarm Initialized, waiting for peers', url)
    // Clean up resources on failure.
    let crawlError = null
    try {
      await swarmJoinedLock

      // Not sure if mingling is necessary, trying to avoid empty drive.readdir()
      log(`topic joined, waiting for updates ${MINGLE_TIMEOUT / 1000}s`, topic.toString('hex'))
      await defer(d => setTimeout(d, MINGLE_TIMEOUT))

      // Crawl content
      log('crawlDrive() start', url)
      const about = await this._crawlDrive(drive)

      // Store descriptor (done here because of uniquePeers availablility)
      if (about) {
        about.peers = Object.keys(uniquePeers).length
        about.version = drive.version
        await this._writeEntry(`about/${drive.key.hexSlice()}`, JSON.stringify(about), true)
        log('+++ Drive finished new state written', about.version, url.toString(), about)
      }
    } catch (err) {
      // const ignoredErrors = ['IDLE_TIMEOUT', 'READDIR_TIMEOUT', 'DOWNLOAD_TIMEOUT']
      // if (ignoredErrors.find(m => err.message === m)) return
      log('crawlDrive() failed', url, err)
      crawlError = err
    } finally {
      // Free resources
      await this._leaveTopic(topic)
      await this._releaseDrive(drive)
    }
    log('Drive resources released', url.toString())
    if (crawlError) throw crawlError
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

    log('Comparing version to cached state')
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

    log('fetching file-list', prevState.version, '=>', drive.version)
    const list = []
    const stream = drive.createDiffStream(prevState.version, '/')
    const readdir = defer(d => {
      // Let the race begin
      const timer = setTimeout(() => {
        stream.destroy(new Error('READDIR_TIMEOUT'))
      }, IDLE_TIMEOUT)

      const onEnd = err => {
        log('diffStream ended', err || '')
        clearTimeout(timer)
        d(err)
      }
      stream.on('error', onEnd)
      stream.on('close', onEnd)
    })

    stream.on('data', entry => {
      log(entry.seq, entry.type, entry.name, entry.value && entry.value.size)
      // TODO: .goto format has 0 size, don't know how it works yet but it's gonna be skipped here.
      if (entry.type === 'put' && entry.value.isFile() && entry.value.size) list.push(entry)
    })

    log('Waiting for list', IDLE_TIMEOUT / 1000, 'seconds')
    await readdir
    log('Filelist received:', list.map(e => e.name))
    if (!list || !list.length) return null

    await defer(indexingDone => {
      // This is the main drive-file processing queue,
      // to abort a drive indexing is to kill the queue between tasks.
      let abortProcess = null
      let nErr = 0
      setTimeout(() => {
        abortProcess = new Error('DRIVE_TIMEOUT')
        indexingDone(abortProcess) // unsure about this line
      }, DRIVE_TIMEOUT)

      const que = new NanoQueue(PARALLEL_FILES, {
        process (task, done) {
          if (abortProcess) return done()
          task()
            .then(() => {
              log('Indexing task succeeded')
              done()
            })
            .catch(err => {
              if (nErr++ > DRIVE_FAIL_THRESHOLD) {
                abortProcess = new Error('DRIVE_FAIL_THRESHOLD')
                indexingDone(abortProcess) // unsure about this line
              }
              log('Indexing task failed', err)
              done()
            })
        },
        oncomplete () {
          if (abortProcess) return indexingDone()
          log('All drive tasks', abortProcess ? 'Aborted!' : 'Complete')
          indexingDone(abortProcess)
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
        const stat = entry.value
        log('Processing file', file)

        const boundCtx = {
          ...context,
          seq,
          stat,
          setPreview: this._setPreview.bind(this, drive, file, seq, stat)
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

  async _writeEntry (key, value, force = false, metadata) {
    if (!force) {
      const entryStat = await defer(d => this._dist.lstat(key, (_, stat) => d(null, stat)))
      if (entryStat) return false
    }
    if (!Buffer.isBuffer(value)) value = Buffer.from(value)
    const opts = {}
    if (metadata) {
      opts.metadata = {}
      for (const mk in metadata) {
        if (!Buffer.isBuffer(metadata[mk])) opts.metadata[mk] = Buffer.from(JSON.stringify(metadata[mk]))
        else opts.metadata[mk] = metadata[mk]
      }
    }
    await defer(d => this._dist.writeFile(key, value, opts, d))
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

  async _setPreview (drive, file, seq, stat, data) {
    const link = previewPath(drive, file)
    const content = previewPath(drive, file, seq)
    // Use symlink hack to store version information and avoid having to compare contents.
    // any data added to the drive is forever stuck in history anyway so no point to clean up old-versions.
    const written = await this._writeEntry(content, data, false, { remoteStat: { mtime: stat.mtime, size: stat.size } })
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
  // make the stackTrace a bit more interesting
  let pickledError = null
  try { throw new Error('DOWNLOAD_TIMEOUT') } catch (err) { pickledError = err }
  pickledError.file = file

  // Fetch the stat first
  const stat = await defer(d => {
    const timer = setTimeout(() => {
      d(pickledError)
    }, DOWNLOAD_TIMEOUT)
    drive.lstat(file, { wait: true }, (err, stat) => {
      clearTimeout(timer)
      d(err, stat)
    })
  })
  // Return stat if body is empty, oversize body is treated as empty file: "ignored".
  if (!stat.size || stat.size > maxSize) return { stat, body: null }

  // check if file is available
  const accessError = await defer(d =>
    drive.access(file, { wait: false }, d.bind(null, null))
  )
  if (accessError) {
    // Wait for file to download
    await defer(d => {
      log('downloading...', file)
      const timer = setTimeout(() => d(new Error('DOWNLOAD_TIMEOUT')), DOWNLOAD_TIMEOUT)
      drive.download(file, err => {
        clearTimeout(timer)
        log('download complete', file, err || '')
        d(err)
      })
    })
  }
  // buffer file content into memory.
  const body = await defer(d => {
    drive.readFile(file, (err, chunk) => {
      if (err) return d(err)
      d(null, raw ? chunk : chunk.toString('utf8'))
    })
  })
  return { stat, body }
}

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

module.exports = {
  scrape,
  Indexer
}

// when `node index.js`
if (require.main === module) {
  process.on('beforeExit', log.bind(null, '[Node] beforeExit'))
  process.on('exit', log.bind(null, '[Node] exit'))
  process.on('multipleResolves', log.bind(null, '[Node] multipleResolves'))
  process.on('unhandledRejection', log.bind(null, '[Node] unhandledRejection'))
  process.on('uncaughtException', log.bind(null, '[Node] uncaughtException'))
  process.on('warning', log.bind(null, '[Node] warning'))

  const { writeFileSync } = require('fs')
  const idxr = new Indexer()
  const distribute = () => {
    return idxr.ready()
      .then(() => {
        idxr.replicate()
        writeFileSync('database.url', `hyper://${idxr._dist.key.hexSlice()}`)
      })
  }

  if (!process.argv[2]) {
    log('Booting up indexer, normal crawl mode')
    let drives = []
    const updateDrives = () => {
      log('Scraping userlist')
      return scrape()
        .then(d => {
          drives = d
          drives.sort((a, b) => Math.random() - 0.5)
        })
    }
    const crawlDrives = () => {
      log('Enqueing drives', drives.length)
      drives.sort((a, b) => Math.random() - 0.5)
      for (const drive of drives) {
        idxr.index(drive.url)
      }
    }

    distribute()
      .then(updateDrives)
      .then(crawlDrives)
      .then(() => { // start timers
        // Recrawl timer
        setInterval(crawlDrives, 30 * 60 * 1000) // Randomly requeue userlist every 30mins

        // Rescrape timer
        setInterval(() => {
          return updateDrives()
            .catch(err => {
              console.error('Indexing failed', err)
            })
        }, 1000 * 60 * 60) // Rescrape every hour
      })
      .catch(err => log('Failed booting the indexer', err))

    // All the additional CLI commands here were added in hindsight. todo: refactor.
  } else if (process.argv[2] === 'ls') {
    idxr._dist.readdir(process.argv[3] || '/', { recursive: true, noMounts: true }, (err, res) => {
      if (err) return log('readdir failed!', err)
      let nFiles = 0
      for (const line of res) {
        console.log(line)
        nFiles++
      }
      console.log('Drive version', idxr._dist.version, `Path contains ${nFiles} records`)
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
      .then(() => idxr.index(process.argv[2], true))
  }
}
