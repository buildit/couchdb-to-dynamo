function checkOptions(validUrl, options) {
  if (!options.couch || !validUrl(options.couch) || !options.dynamo || !validUrl(options.dynamo)) {
    throw new Error('invalid options');
  }
}

function showHelp(getUsage, logger, optionsDefinitions) {
  const sections = [
    {
      header: 'CouchDB |> DynamoDb',
      content: 'Migrates data [italic]{from} CouchDB to DynamoDB.',
    },
    {
      header: 'Options',
      optionList: optionsDefinitions,
    },
  ];
  const usage = getUsage(sections);
  return logger.info(usage);
}

function normalizeOptions(R, options) {
  return Object.entries(options).reduce((object, entry) =>
    R.merge(object, { [entry[0]]: entry[1].endsWith('/') ? entry[1] : `${entry[1]}/` }), {});
}

function getCouchContents(pouch, couchUrl) {
  return async function getDB(dbName) {
    const db = pouch(`${couchUrl}${dbName}`);
    const docs = await db.allDocs({ include_docs: true });
    return docs;
  };
}

async function getCouchData(request, pouch, R, options) {
  const databasesToIgnore = { _replicator: true, _users: true };
  const rawDbNames = (await request(`${options.couch}_all_dbs`)).body;
  const dbNames = JSON.parse(rawDbNames).filter(dbName => !databasesToIgnore[dbName]);
  const dbData = await Promise.all(dbNames.map(getCouchContents(pouch, options.couch)));
  return dbNames.reduce((object, name, index) => R.merge(object, { [name]: dbData[index] }), {});
}

function createAwsDatabase(dynasty, dbName) {
  return dynasty.drop(dbName)
  .catch(() => undefined)
  .then(() => dynasty.create(dbName, { key_schema: { hash: ['_id', 'string'] } }));
}

function putRowToAmazon(dynasty, deepReplaceInObject, dbName) {
  const table = dynasty.table(dbName);
  return (row) => {
    const toBeInserted = deepReplaceInObject('', null, row.doc);
    console.log(toBeInserted);
    return table.insert(toBeInserted);
  };
}

async function putAwsData(dynasty, deepReplaceInObject, [id, data]) {
  await createAwsDatabase(dynasty, id);
  return Promise.all(data.rows.map(putRowToAmazon(dynasty, deepReplaceInObject, id)));
}

async function main() {
  const asyncRequest = require('request-async');
  const commandLineArgs = require('command-line-args');
  const commandLineUsage = require('command-line-usage');
  const log4js = require('log4js');
  const urlValidator = require('valid-url').isUri;
  const ramda = require('ramda');
  const PouchDB = require('pouchdb');
  const deepReplaceInObject = require('deep-replace-in-object');
  const logger = log4js.getLogger();
  const optionsDefinitions = [
    { name: 'help', alias: 'h', description: 'Print this usage guide', type: Boolean },
    { name: 'couch', alias: 'c', description: '(required) Url to access the Couch database', type: String },
    { name: 'dynamo', alias: 'd', description: '(required) Url to access the Dynamo database', type: String },
  ];
  try {
    const unsanitizedOptions = commandLineArgs(optionsDefinitions);
    checkOptions(urlValidator, unsanitizedOptions);
    const options = normalizeOptions(ramda, unsanitizedOptions);
    const couchData = await getCouchData(asyncRequest, PouchDB, ramda, options);
    const dynastyCredentials = {
      accessKeyId: 'some key',
      region: 'us-west-2',
      secretAccessKey: 'some secret',
    };
    const dynasty = require('dynasty')(dynastyCredentials, options.dynamo);
    await Promise.all(Object.entries(couchData)
        .map(entry => putAwsData(dynasty, deepReplaceInObject, entry)));
    return logger.info('Migrated!');
  } catch (error) {
    logger.error('error?', error);
    return showHelp(commandLineUsage, logger, optionsDefinitions);
  }
}

module.exports = {
  checkOptions,
  showHelp,
  normalizeOptions,
  getCouchContents,
  getCouchData,
  createAwsDatabase,
  putRowToAmazon,
  putAwsData,
  main,
};
