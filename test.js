const test = require('tape')
const { scrape, Indexer } = require('.')
const { writeFileSync, readFileSync } = require('fs')

test.skip('scrape user-directory', async t => {
  try {
    const drives = await scrape()
    writeFileSync('drives.json', JSON.stringify(drives))
  } catch (error) { t.error(error) }
  t.end()
})

test('Index drive', async t => {
  try {
    // '6900789c2dba488ca132a0ca6d7259180e993b285ede6b29b464b62453cd5c39' contains content.
    const drives = JSON.parse(readFileSync('drives.json').toString('utf8'))
    const idxr = new Indexer()
    await idxr.ready()
    for (const drive of drives) {
      idxr.index(drive.url)
    }
  } catch (err) { t.error(err) }
  t.end()
})

test.skip('edge ngram tokenzier', t => {
  const text = 'Bob has a funny sprinkler'
  const expected = 'bob,has,fun,funn,funny,spr,spri,sprin,sprink,sprinkl,sprinkler'
  const tokens = []
  for (const token of edgeNgram(text)) tokens.push(token)
  t.equal(tokens.join(','), expected)
})
