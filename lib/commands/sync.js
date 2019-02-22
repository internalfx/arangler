
let _ = require('lodash')
let Promise = require('bluebird')
let inquirer = require('inquirer')
// let co = require('co')
let colors = require('colors')
// let asyncEach = require('../asyncEach')
let compareValues = require('../compareValues')
let moment = require('moment')
let arangojs = require('arangojs')
let aql = arangojs.aql
let createConnection = require('../arango.js')

let HELPTEXT = `

  Arangler Sync
  ==============================

  Sync two ArangoDB databases.

  Usage:
    arangler sync [options]
    arangler sync --sh host[:port] --th host[:port] --sd dbName --td dbName
    arangler sync -h | --help

  Options:
    --sh, --sourceHost=<host[:port]>        Source host, defaults to 'localhost:8529'
    --th, --targetHost=<host[:port]>        Target host, defaults to 'localhost:8529'
    --sd, --sourceDB=<dbName>               Source database
    --td, --targetDB=<dbName>               Target database

    --pc, --pickCollections=<coll1,coll2>   Comma separated list of collections to sync (whitelist)
    --oc, --omitCollections=<coll1,coll2>   Comma separated list of collections to ignore (blacklist)
                                            Note: '--pc' and '--oc' are mutually exclusive options.

    --user                                  Source and Target username
    --password                              Source and Target password

    --su                                    Source username, overrides --user
    --sp                                    Source password, overrides --password

    --tu                                    Target username, overrides --user
    --tp                                    Target password, overrides --password

`

module.exports = async function (argv) {
  let startTime
  let sHost = argv.sh ? argv.sh : argv.sourceHost ? argv.sourceHost : 'http://localhost:8529'
  let tHost = argv.th ? argv.th : argv.targetHost ? argv.targetHost : 'http://localhost:8529'
  // let sourceHost = _.first(sHost.split(':'))
  // let targetHost = _.first(tHost.split(':'))
  // let sourcePort = parseInt(_.last(sHost.split(':')), 10) || 8529
  // let targetPort = parseInt(_.last(tHost.split(':')), 10) || 8529
  let sourceDB = argv.sd ? argv.sd : argv.sourceDB ? argv.sourceDB : null
  let targetDB = argv.td ? argv.td : argv.targetDB ? argv.targetDB : null
  let pickCollections = argv.pt ? argv.pt : argv.pickCollections ? argv.pickCollections : null
  let omitCollections = argv.ot ? argv.ot : argv.omitCollections ? argv.omitCollections : null
  let sourceUser = argv.su ? argv.su : argv.user ? argv.user : 'root'
  let sourcePassword = argv.sp ? argv.sp : argv.password ? argv.password : ''
  let targetUser = argv.tu ? argv.tu : argv.user ? argv.user : 'root'
  let targetPassword = argv.tp ? argv.tp : argv.password ? argv.password : ''

  // Setup source and target connections
  let sa = await createConnection({ url: sHost, username: sourceUser, password: sourcePassword })
  let ta = await createConnection({ url: tHost, username: targetUser, password: targetPassword })

  let getCursors = function (collection) {
    return Promise.all([
      sa.q(aql`
        FOR x IN ${sa.collection(collection)}
          SORT x._key ASC
          RETURN { _key: x._key, hash: SHA512(UNSET(x, '_rev')) }
      `),
      ta.q(aql`
        FOR x IN ${ta.collection(collection)}
          SORT x._key ASC
          RETURN { _key: x._key, hash: SHA512(UNSET(x, '_rev')) }
      `)
    ])
  }

  pickCollections = _.isString(pickCollections) ? pickCollections.split(',') : null
  omitCollections = _.isString(omitCollections) ? omitCollections.split(',') : null

  if (argv.h || argv.help) {
    console.log(HELPTEXT)
    return
  }

  if (pickCollections && omitCollections) {
    console.log('pickCollections and omitCollections are mutually exclusive options.')
    return
  }

  if (!sourceDB || !targetDB) {
    console.log('Source and target databases are required!')
    console.log(HELPTEXT)
    return
  }

  if (sHost === tHost && sourceDB === targetDB) {
    console.log('Source and target databases must be different if syncing on same server!')
    return
  }

  // get sourceDBList
  let sourceDBList = await sa.listDatabases()
  sa.useDatabase(sourceDB)

  // get sourceCollectionList
  let sourceCollectionList = (await sa.listCollections()).map(i => i.name)

  if (!sourceDBList.includes(sourceDB)) {
    console.log('Source DB does not exist!')
    return
  }

  if (pickCollections && !_.every(pickCollections, (collection) => sourceCollectionList.includes(collection))) {
    console.log(colors.red('Not all the collections specified in --pickCollections exist!'))
    return
  }

  if (omitCollections && !_.every(omitCollections, (collection) => sourceCollectionList.includes(collection))) {
    console.log(colors.red('Not all the collections specified in --omitCollections exist!'))
    return
  }

  let confMessage = `
    ${colors.green('Ready to synchronize!')}
    The database '${colors.yellow(sourceDB)}' on '${colors.yellow(sHost)}' will be synchronized to the '${colors.yellow(targetDB)}' database on '${colors.yellow(tHost)}'
    This will modify records in the '${colors.yellow(targetDB)}' database on '${colors.yellow(tHost)}' if it exists!
  `

  if (pickCollections) {
    confMessage += `  ONLY the following collections will be synchronized: ${colors.yellow(pickCollections.join(','))}\n`
  }
  if (omitCollections) {
    confMessage += `  The following collections will NOT be synchronized: ${colors.yellow(omitCollections.join(','))}\n`
  }

  console.log(confMessage)

  let answer = await inquirer.prompt([{
    type: 'confirm',
    name: 'confirmed',
    message: 'Proceed?',
    default: false
  }])

  if (!answer.confirmed) {
    console.log(colors.red('ABORT!'))
    return
  }

  startTime = moment()

  let collectionsToSync
  if (pickCollections) {
    collectionsToSync = pickCollections
  } else if (omitCollections) {
    collectionsToSync = _.difference(sourceCollectionList, omitCollections)
  } else {
    collectionsToSync = sourceCollectionList
  }

  let targetDBList = await ta.listDatabases()
  if (targetDBList.includes(targetDB) === false) {
    console.log('Target DB does not exist, creating...')
    await ta.createDatabase(targetDB)
  }
  ta.useDatabase(targetDB)

  let targetCollectionList = (await ta.listCollections()).map(i => i.name)

  await Promise.map(collectionsToSync, async function (collection) {
    if (!targetCollectionList.includes(collection)) {
      console.log(`Collection '${colors.yellow(collection)}' does not exist on target, creating...`)

      let sColl = sa.collection(collection)
      let tColl = ta.collection(collection)

      let sProps = await sColl.properties()

      await tColl.create({
        waitForSync: sProps.waitForSync
      })
    }
  }, { concurrency: 99 })

  await Promise.map(collectionsToSync, async function (collection) {
    let sColl = sa.collection(collection)
    let tColl = ta.collection(collection)

    let sourceIndexes = (await sColl.indexes()).filter(idx => idx.type !== 'primary')
    let targetIndexes = (await tColl.indexes()).filter(idx => idx.type !== 'primary')

    for (let sIndex of sourceIndexes) {
      let hasIndex = !!targetIndexes.find(function (tIndex) {
        return (
          tIndex.type === sIndex.type &&
          _.isEqual(tIndex.fields, sIndex.fields) &&
          tIndex.unique === sIndex.unique &&
          tIndex.sparse === sIndex.sparse &&
          tIndex.deduplicate === sIndex.deduplicate
        )
      })

      if (hasIndex === false) {
        console.log(`Index '${colors.yellow(`${sIndex.type} - ${sIndex.fields}`)}' does not exist on '${colors.yellow(collection)}' collection, creating...`)
        await tColl.createIndex(_.pick(sIndex, 'type', 'fields', 'unique', 'sparse', 'deduplicate'))
      }
    }
  }, { concurrency: 99 })

  for (let collection of collectionsToSync) {
    console.log(` `)
    let sColl = sa.collection(collection)
    let tColl = ta.collection(collection)

    let totalRecords = await sa.qNext(aql`
      FOR x IN ${sColl}
        COLLECT WITH COUNT INTO count
        RETURN count
    `)
    let statusInterval = 500
    let created = 0
    let updated = 0
    let deleted = 0
    // let queue = blockingQueue()
    let perfStat = []
    let recordsProcessed = 0
    let lastRecordsProcessed = 0
    let pc = 0
    let step = 'scan'

    let incCount = function () {
      recordsProcessed += 1
    }

    console.log(`${colors.yellow(totalRecords)} records in ${colors.yellow(collection)}`)

    process.stdout.write(` Getting cursors...                    \r`)
    let [ sCursor, tCursor ] = await getCursors(collection)
    let changes = {
      creates: [],
      updates: [],
      deletes: []
    }

    var getNextIdx = async function (cursor, idx) {
      if (idx._key !== Infinity) {
        idx = await cursor.next()

        if (idx == null) {
          idx = {
            hash: '',
            _key: Infinity
          }
        }
      }
      return idx
    }

    let si = {}
    let ti = {}

    si = await getNextIdx(sCursor, si)
    ti = await getNextIdx(tCursor, ti)

    let printStats = function () {
      if (step === 'scan') {
        perfStat.unshift(recordsProcessed - lastRecordsProcessed)
        while (perfStat.length > 30) {
          perfStat.pop()
        }
        let rps = (_.reduce(perfStat, (a, b) => a + b) / (perfStat.length * (statusInterval / 1000))).toFixed(1)
        pc = ((recordsProcessed / totalRecords) * 100).toFixed(1)
        if (totalRecords === 0 && recordsProcessed === 0) { pc = 100 }
        process.stdout.write(` Scanning collections... : ${recordsProcessed} | ${rps} sec. | %${pc} \r`)
        lastRecordsProcessed = recordsProcessed
      } else if (step === 'sync') {
        process.stdout.write(` Synchronizing changes... : created ${created > 0 ? colors.green(created) : created} | updated ${updated > 0 ? colors.yellow(updated) : updated} | deleted ${deleted > 0 ? colors.red(deleted) : deleted}                    \r`)
      }
    }

    let stats = async function () {
      while (true) {
        if (step === 'done') { break }
        printStats()
        await Promise.delay(statusInterval)
      }
    }

    stats()

    while (si._key !== Infinity || ti._key !== Infinity) {
      const cmp = compareValues(si._key, ti._key)

      if (cmp === 0) { // si.id === ti.id  ->  check hashes
        if (si.hash !== ti.hash) {
          changes.updates.push(si._key)
        }
        si = await getNextIdx(sCursor, si)
        incCount()
        ti = await getNextIdx(tCursor, ti)
      } else if (cmp < 0) { // si.id < ti.id  ->  copy si
        changes.creates.push(si._key)
        si = await getNextIdx(sCursor, si)
        incCount()
      } else if (cmp > 0) { // si.id > ti.id  ->  delete ti
        changes.deletes.push(ti._key)
        ti = await getNextIdx(tCursor, ti)
      }
    }

    step = 'sync'

    let batchList = function (list) {
      return list.reduce(function (acc, item) {
        if (_.last(acc) == null || _.last(acc).length > 200) {
          acc.push([])
        }
        _.last(acc).push(item)
        return acc
      }, [])
    }

    await Promise.map(batchList(changes.deletes), async function (batch) {
      await ta.q(aql`
        FOR x IN ${tColl}
          FILTER x._key IN ${batch}
          REMOVE x IN ${tColl}
      `)
      deleted += batch.length
    }, { concurrency: 2 })

    await Promise.map(batchList(changes.updates), async function (batch) {
      let records = await sa.qAll(aql`
        FOR x IN ${sColl}
          FILTER x._key IN ${batch}
          RETURN x
      `)
      await ta.q(aql`
        FOR x IN ${records}
          REPLACE x INTO ${tColl}
      `)
      updated += records.length
    }, { concurrency: 2 })

    await Promise.map(batchList(changes.creates), async function (batch) {
      let records = await sa.qAll(aql`
        FOR x IN ${sColl}
          FILTER x._key IN ${batch}
          RETURN x
      `)
      await ta.q(aql`
        FOR u IN ${records}
          INSERT u INTO ${tColl}
      `)
      created += records.length
    }, { concurrency: 2 })

    printStats()
    step = 'done'
  }

  console.log(``)
  console.log(colors.green(`DONE! Completed in ${startTime.fromNow(true)}`))
}
