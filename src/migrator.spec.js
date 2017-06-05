/* eslint-disable no-unused-expressions */
const { dissoc } = require('ramda');
const { expect } = require('chai');
const sinon = require('sinon');
const {
  checkOptions,
  showHelp,
  normalizeOptions,
  getCouchContents,
  getCouchData,
  createAwsDatabase,
  putRowToAmazon,
} = require('./migrator.js');
const ramda = require('ramda');

describe('checkOptions', () => {
  function validOptions() {
    return {
      couch: 'http://localhost:5984',
      dynamo: 'http://localhost:8000',
    };
  }

  it('throws an error if there is no couch url', () => {
    const missingCouch = dissoc('couch', validOptions());
    expect(() => checkOptions(() => undefined, missingCouch)).to.throw('invalid options');
  });

  it('throws an error if the couch url is not valid', () => {
    function validator(url) {
      return !url.includes('5984');
    }
    expect(() => checkOptions(validator, validOptions())).to.throw('invalid options');
  });

  it('throws an error if there is no dynamo url', () => {
    const missingCouch = dissoc('dynamo', validOptions());
    expect(() => checkOptions(() => undefined, missingCouch)).to.throw('invalid options');
  });

  it('throws an error if the dynamo url is not valid', () => {
    function validator(url) {
      return !url.includes('8000');
    }
    expect(() => checkOptions(validator, validOptions())).to.throw('invalid options');
  });
});

describe('showHelp', () => {
  it('logs out usage information', () => {
    const logger = {
      info: sinon.spy(),
    };
    function getUsage(sections) {
      return sections.reduce((prev, section) => `${prev} ${section.header}`, '');
    }
    showHelp(getUsage, logger, {});
    expect(logger.info.getCall(0).args[0]).to.equal(' CouchDB |> DynamoDb Options');
  });
});

describe('normalizeOptions', () => {
  function validOptions() {
    return {
      couch: 'http://localhost:5984',
      dynamo: 'http://localhost:8000',
    };
  }

  it('does not modify the parameter', () => {
    const options = validOptions();
    normalizeOptions(ramda, options);
    expect(options).to.deep.equal(validOptions());
  });

  it('adds a slash to the end of all options if needed', () => {
    const resultingOptions = normalizeOptions(ramda, validOptions());
    Object.entries(resultingOptions).forEach((option) => {
      expect(option[1].endsWith('/')).to.be.true;
    });
  });

  it('does not add the slash if it already exists', () => {
    const firstPass = normalizeOptions(ramda, validOptions());
    const secondPass = normalizeOptions(ramda, firstPass);
    expect(secondPass).to.deep.equal(firstPass);
  });
});

describe('getCouchContents', () => {
  it('returns a function', () => {
    expect(getCouchContents()).to.be.a('function');
  });

  describe('the returned function', () => {
    let returnedFunction;

    beforeEach(() => {
      const pouch = () => ({
        allDocs() {
          return Promise.resolve('all of the docs');
        },
      });
      returnedFunction = getCouchContents(pouch, 'some url');
    });

    it('returns the result of the allDocs({ include_docs: true })', async () => {
      const docs = await returnedFunction('some name');
      expect(docs).to.equal('all of the docs');
    });
  });
});

describe('getCouchData', () => {
  function request() {
    return Promise.resolve({
      body: JSON.stringify(['_replicator', '_users', 'table1', 'table2', 'table3']),
    });
  }

  function pouch(name) {
    return {
      allDocs() {
        return [{ docType1: name }, { docType1: name }, { docType1: name }];
      },
    };
  }

  it('ignores the _replicator and _users table', async () => {
    const ignoreThese = {
      _replicator: true,
      _users: true,
    };

    const results = await getCouchData(request, pouch, ramda, { couch: '' });
    expect(ramda.all(key => !ignoreThese[key], ramda.keys(results))).to.be.true;
  });

  it('returns an object of the tables', async () => {
    const results = await getCouchData(request, pouch, ramda, { couch: '' });
    expect(results).to.deep.equal({
      table1: [{ docType1: 'table1' }, { docType1: 'table1' }, { docType1: 'table1' }],
      table2: [{ docType1: 'table2' }, { docType1: 'table2' }, { docType1: 'table2' }],
      table3: [{ docType1: 'table3' }, { docType1: 'table3' }, { docType1: 'table3' }],
    });
  });
});

describe('createAwsDatabase', () => {
  it('catches an error if there are no tables to drop', () => {
    const dynasty = {
      drop() {
        return Promise.reject('table does not exist');
      },
      create() {
        return Promise.resolve();
      },
    };
    expect(() => createAwsDatabase(dynasty, 'whatever')).not.to.throw;
  });
});

describe('putRowToAmazon', () => {
  let insert = sinon.stub().resolves();
  let dynasty;
  let deepReplaceInObject = sinon.stub().returns({ an: 'object' });

  beforeEach(() => {
    insert = sinon.stub();
    insert.resolves();
    dynasty = {
      table() {
        return {
          insert,
        };
      },
    };
    deepReplaceInObject = sinon.stub().returns({ an: 'object' });
  });

  it('returns a function', () => {
    expect(putRowToAmazon(dynasty, deepReplaceInObject)).to.be.a('function');
  });

  describe('returned function', () => {
    it('calls insert', () => {
      const fn = putRowToAmazon(dynasty, deepReplaceInObject, 'name');
      fn('some row');
      expect(insert.getCall(0).args[0]).to.deep.equal({ an: 'object' });
    });
  });
});
