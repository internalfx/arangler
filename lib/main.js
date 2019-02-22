
let _ = require('lodash')
let requireAll = require('require-all')

/* global VERSION */

let commands = requireAll({
  dirname: `${__dirname}/commands`
})

let HELPTEXT = `

  Arangler ${VERSION}
  ==============================

  An ArangoDB command line tool.

  Commands:
    arangler sync            Synchronize differences between two databases.
    arangler -h | --help     Show this screen.

`

// Some notes --> process.stdout.write(" RECORDS INSERTED: Total = #{records_processed} | Per Second = #{rps} | Percent Complete = %#{pc}          \r");

module.exports = async function (argv) {
  let command = _.first(argv['_'])
  argv['_'] = argv['_'].slice(1)
  if (commands[command]) {
    await commands[command](argv)
  } else {
    console.log(HELPTEXT)
  }

  process.exit()
}
