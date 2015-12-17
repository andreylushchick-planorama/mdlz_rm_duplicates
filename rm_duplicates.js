var iconv = require('iconv-lite');
var fs = require('fs');
var readline = require('readline');
var crypto = require('crypto');
// After this call all Node basic primitives will understand iconv-lite encodings.
iconv.extendNodeEncodings();

var linesCount = 0;
// Data structure to host all record hashes, to be able to search whether
// any given line was already processed. Record hash is created from a line string
// without 1st column (record_id), which is a key. The value is always 1
var records = {};
// Duplicate lines
var duplicates = [];
// store the header line
var headerLine;
var outFileName = 'export_cleaned_' + getTimeString() + '.csv';
var duplicatesFileName = 'duplicates_' + getTimeString() + '.csv';

// script entry point
function main(inputFile) {
  if (!inputFile) throw new Error('No input file');

  var instream = fs.createReadStream(inputFile)
    .pipe(iconv.decodeStream('iso-8859-1'));

  var outstream = fs.createWriteStream(outFileName, { defaultEncoding: 'iso-8859-1' });

  var rl = readline.createInterface({
      input: instream,
      output: outstream,
      terminal: false
  });

  // read the file line-by-line
  rl.on('line', onLine.bind(null, rl));

  // drain fires when we can continue writing to outstream
  rl.output.on('drain', function() {
    rl.resume();
  });

  rl.on('close', function() {
    if (duplicates.length === 0) {
      return outputResults();
    }

    fs.writeFile(duplicatesFileName, duplicates.join('\n'), 'iso-8859-1', function(err) {
      if (err) return console.error(err);
      outputResults();
    });
  });
}

function onLine(rl, line) {
  var splitted = line.split(';');
  var recordId = splitted.shift();
  var isDeleteRecordState = splitted[0] === 'D';
  var restOfLine = splitted.join(';');
  var hash = crypto.createHash('md5')
    .update(restOfLine)
    .digest('hex');

  // store the header line
  if (linesCount === 0) headerLine = line;

  linesCount += 1;

  // if record already processed AND its RECORD_STATE is not 'D' (deletion) - we have a duplicate line
  if (records[hash] && !isDeleteRecordState) {
    // remember duplicate line
    duplicates.push(line);
  } else {
    records[hash] = 1;
    // write unique line to the new file
    if (false === rl.output.write(line + '\n')) {
      // pause the reading of our file until the data gets written
      rl.pause();
    }
  }

  if (linesCount % 5e4 === 0) console.log('Processed ' + linesCount + ' lines');
}

function outputResults() {
  var out = [
    'All done.',
    'Total lines: ' + linesCount,
    'Duplicates: ' + duplicates.length,
    'Files created:',
    '\t' + outFileName
  ];

  if (duplicates.length > 0) {
    out.push('\t' + duplicatesFileName);
  }

  console.log(out.join('\n'));
}

function getTimeString() {
  function pad(n) {
    return (n < 10) ? ('0' + n) : n;
  }

  var d = new Date();
  var timeString = [
    d.getUTCDate(), d.getUTCMonth() + 1, d.getUTCFullYear(), '_',
    d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()
  ].map(pad).join('-');
  return timeString;
}

main(process.argv[2]);
