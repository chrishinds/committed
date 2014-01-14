// Generated by CoffeeScript 1.6.2
var applyToRegistered, async, commit, execute, rollback, _db, _drainQueues, _enqueueOrCreateAndEnqueue, _pushTransactionError, _queueCountersCollection, _queuePosition, _queues, _quietly, _registry, _softwareVersion, _state, _transactionsCollection, _updateTransactionStatus,
  __slice = [].slice;

async = require('async');

_state = 'stopped';

_queues = null;

_db = null;

_registry = {};

_softwareVersion = null;

_queueCountersCollection = null;

_transactionsCollection = null;

exports.start = function(db, softwareVersion, done) {
  var tasks;

  if (_state !== 'stopped') {
    return done(new Error("committed has already been started"));
  }
  _softwareVersion = softwareVersion;
  _db = db;
  tasks = [];
  tasks.push(function(done) {
    return _db.createCollection('transactions', {
      w: 1,
      journal: true
    }, function(err, collection) {
      if (err != null) {
        return done(err, null);
      }
      _transactionsCollection = collection;
      return _db.ensureIndex('transactions', {
        status: 1
      }, {
        w: 1,
        journal: true
      }, done);
    });
  });
  tasks.push(function(done) {
    return _db.createCollection('queueCounters', {
      w: 1,
      journal: true
    }, function(err, collection) {
      if (err != null) {
        return done(err, null);
      }
      _queueCountersCollection = collection;
      return _db.ensureIndex('queueCounters', {
        queue: 1
      }, {
        w: 1,
        journal: true
      }, done);
    });
  });
  tasks.push(function(done) {
    return _transactionsCollection.find({
      status: 'Processing'
    }, {
      snapshot: true
    }).sort('position', 1, function(err, transactions) {
      if (err != null) {
        return done(err, null);
      }
      return async.eachSeries(transactions, _quietly(rollback), done);
    });
  });
  tasks.push(function(done) {
    return _transactionsCollection.find({
      status: 'Queued'
    }, {
      snapshot: true
    }).sort('position', 1, function(err, transactions) {
      return async.eachSeries(transactions, _quietly(commit), done);
    });
  });
  return async.series(tasks, function(err, results) {
    if (err != null) {
      return done(err);
    }
    _queues = {
      GlobalLock: async.queue(commit, 1)
    };
    _queues.GlobalLock.drain = function() {
      return _state = 'started';
    };
    _state = 'started';
    return done(null);
  });
};

exports.stop = function(done) {
  var name;

  if (_state !== 'started') {
    return done(new Error("committed is not currently started"));
  }
  _state = 'stopped';
  return _drainQueues((function() {
    var _results;

    _results = [];
    for (name in _queues) {
      _results.push(name);
    }
    return _results;
  })(), done);
};

_drainQueues = function(queues, done) {
  var name, tasks, _fn, _i, _len;

  tasks = [];
  _fn = function(name) {
    return tasks.push(function(done) {
      if (_queues[name].length() === 0) {
        return done(null, null);
      } else {
        return _queues[name].drain = function() {
          return done(null, null);
        };
      }
    });
  };
  for (_i = 0, _len = queues.length; _i < _len; _i++) {
    name = queues[_i];
    _fn(name);
  }
  return async.parallel(tasks, function(err, results) {
    var _j, _len1;

    for (_j = 0, _len1 = queues.length; _j < _len1; _j++) {
      name = queues[_j];
      _queues[name].drain = void 0;
    }
    return done(err);
  });
};

exports.register = function(name, fnOrObj) {
  var ancestor, ancestors, key, parent, _i, _len;

  parent = _registry;
  ancestors = name.split('.');
  key = ancestors.pop();
  for (_i = 0, _len = ancestors.length; _i < _len; _i++) {
    ancestor = ancestors[_i];
    parent = parent[ancestor];
    if (parent == null) {
      throw new Error("during register: path " + name + " doesn't exist in registry: " + (JSON.stringify(_registry)));
    }
  }
  if (parent[key] != null) {
    throw new Error("during register: path " + name + " already exists in registry: " + (JSON.stringify(_registry)));
  }
  return parent[key] = fnOrObj;
};

_queuePosition = function(queueName, done) {
  return _queueCountersCollection.findAndModify({
    queue: queueName
  }, {}, {
    $inc: {
      nextPosition: 1
    }
  }, {
    upsert: true,
    w: 1,
    journal: true
  }, function(err, doc) {
    var _ref;

    if (err != null) {
      return done(err, null);
    }
    return done(null, (_ref = doc.nextPosition) != null ? _ref : 0);
  });
};

_enqueueOrCreateAndEnqueue = function(queueName, transaction, done) {
  if (_queues[queueName] == null) {
    _queues[queueName] = async.queue(commit, 1);
  }
  return _queues[queueName].push(transaction, done);
};

exports.sequentially = function(transaction, done) {
  var enqueue, _ref;

  transaction.enqueuedAt = new Date();
  enqueue = function() {
    if (_state === 'started') {
      return _enqueueOrCreateAndEnqueue(transaction.queue, transaction, done);
    } else {
      return _updateTransactionStatus(transaction, transaction.status, 'Failed', function(err) {
        return done(err, 'Failed');
      });
    }
  };
  if (transaction.queue === "GlobalLock") {
    return done(new Error("Can't queue a transaction for GlobalLock using the committed.sequentially function"), null);
  }
  if (!((_ref = transaction.queue) != null ? _ref.length : void 0)) {
    return done(new Error("must have a transaction.queue parameter to use committed.sequentially"), null);
  }
  return setImmediate(function() {
    transaction.status = _state === 'started' ? "Queued" : "Failed";
    return _queuePosition(transaction.queue, function(err, position) {
      transaction.position = position;
      if (transaction.constructor != null) {
        return enqueue();
      } else {
        return _transactionsCollection.insert(transaction, function(err, docs) {
          if (err != null) {
            return done(err, null);
          }
          return enqueue();
        });
      }
    });
  });
};

exports.immediately = function(transaction, done) {
  var go;

  transaction.enqueuedAt = new Date();
  go = function() {
    if (_state === 'started') {
      return commit(transaction, done);
    } else {
      return _updateTransactionStatus(transaction, transaction.status, 'Failed', function(err) {
        return done(err, 'Failed');
      });
    }
  };
  if (transaction.queue != null) {
    return done(new Error("Can't call committed.immediately on a transaction which has a queue defined for it"));
  }
  return setImmediate(function() {
    transaction.status = _state === 'started' ? "Queued" : "Failed";
    transaction.position = -1;
    if (transaction.constructor != null) {
      return go();
    } else {
      return _transactionsCollection.insert(transaction, function(err, docs) {
        if (err != null) {
          return done(err, null);
        }
        return go();
      });
    }
  });
};

exports.withGlobalLock = function(transaction, done) {
  var enqueue;

  if (transaction.queue !== "GlobalLock") {
    return done(new Error("Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"), null);
  }
  transaction.enqueuedAt = new Date();
  enqueue = function() {
    var name;

    return _drainQueues((function() {
      var _results;

      _results = [];
      for (name in _queues) {
        if (name !== 'GlobalLock') {
          _results.push(name);
        }
      }
      return _results;
    })(), function(err) {
      if (err != null) {
        return done(err, null);
      }
      return _queues['GlobalLock'].push(transaction, done);
    });
  };
  transaction.status = _state !== 'stopped' ? "Queued" : "Failed";
  return _queuePosition(transaction.queue, function(err, position) {
    var oldState;

    transaction.position = position;
    if (transaction.constructor != null) {
      return enqueue();
    } else {
      if (_state !== 'stopped') {
        oldState = _state;
        _state = 'locked';
      }
      return _transactionsCollection.insert(transaction, function(err, docs) {
        if (err != null) {
          _state = oldState;
          return done(err, null);
        }
        if (_state === 'stopped') {
          return done(null, 'Failed');
        } else {
          return enqueue();
        }
      });
    }
  });
};

applyToRegistered = function(name, fnArgs) {
  var done, found, key, _i, _len, _ref;

  found = _registry;
  _ref = name.split('.');
  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
    key = _ref[_i];
    found = found[key];
    if (found == null) {
      done = fnArgs[fnArgs.length - 1];
      return done(new Error("during applyToRegistered: path " + name + " doesn't exist in registry: " + (JSON.stringify(_registry))));
    }
  }
  return found.apply(this, fnArgs);
};

execute = function(instructions, data, done) {
  var iterator, result;

  result = true;
  iterator = function(instruction, iteratorDone) {
    var fnArgs;

    fnArgs = [_db, data];
    if (instruction["arguments"] != null) {
      fnArgs = fnArgs.concat(instruction["arguments"]);
    }
    fnArgs.push(function(err, iteratorResult) {
      if (iteratorResult === false) {
        result = false;
      }
      return iteratorDone(err);
    });
    return applyToRegistered(instruction.name, fnArgs);
  };
  return async.each(instructions, iterator, function(err) {
    return done(err, result);
  });
};

_quietly = function(fn) {
  return function() {
    var args, done, quietDone, _i;

    args = 2 <= arguments.length ? __slice.call(arguments, 0, _i = arguments.length - 1) : (_i = 0, []), done = arguments[_i++];
    quietDone = function(err, result) {
      if (err != null) {
        console.log("error suppressed, probably during committed.start: " + (JSON.stringify(err)));
      }
      return done(null, result);
    };
    return fn.apply(null, __slice.call(args).concat([quietDone]));
  };
};

_pushTransactionError = function(transaction, error, done) {
  transaction.errors.push(error);
  return _transactionsCollection.update({
    _id: transaction._id
  }, {
    $push: {
      errors: error.toString()
    }
  }, {
    w: 1,
    journal: true
  }, function(err, updated) {
    if (err != null) {
      console.log("error saving error to transaction: " + err);
    }
    return done();
  });
};

_updateTransactionStatus = function(transaction, fromStatus, toStatus, done) {
  return _transactionsCollection.update({
    _id: transaction._id,
    status: fromStatus
  }, {
    $set: {
      status: toStatus,
      softwareVersion: _softwareVersion,
      lastUpdate: new Date(),
      enqueuedAt: transaction.enqueuedAt,
      startedAt: transaction.startedAt
    }
  }, {
    w: 1,
    journal: true
  }, function(err, updated) {
    var updateError;

    switch (false) {
      case err == null:
        return _pushTransactionError(transaction, err, function() {
          return done(err);
        });
      case updated === 1:
        updateError = new Error('exactly one transaction must be updated when moving to Processing status');
        return _pushTransactionError(transaction, updateError, function() {
          return done(updateError);
        });
      default:
        return done(null);
    }
  });
};

rollback = function(transaction, failStatus, done) {
  if (transaction.status !== 'Processing') {
    return done(new Error("can't rollback a transaction that isn't at 'Processing' status"), null);
  }
  return execute(transaction.rollback, transaction.data, function(rollbackErr, result) {
    var failed, newStatus;

    failed = (rollbackErr != null) || result === false;
    switch (false) {
      case !(failed && transaction.errors.length > 0):
        newStatus = 'CatastropheCommitErrorRollbackError';
        break;
      case !(failed && transaction.errors.length === 0):
        newStatus = 'CatastropheCommitFailedRollbackError';
        break;
      case !(!failed && transaction.errors.length > 0):
        newStatus = 'FailedCommitErrorRollbackOk';
        break;
      case !(!failed && transaction.errors.length === 0):
        newStatus = 'Failed';
    }
    return _updateTransactionStatus(transaction, 'Processing', newStatus, function(statusErr) {
      if (statusErr != null) {
        return done(statusErr, newStatus);
      } else if (rollbackErr != null) {
        return _pushTransactionError(transaction, rollbackErr, function() {
          return done(rollbackErr, newStatus);
        });
      } else {
        return done(null, newStatus);
      }
    });
  });
};

commit = function(transaction, done) {
  var doCommit;

  doCommit = function(err) {
    if (err != null) {
      return done(err, null);
    }
    return execute(transaction.instructions, transaction.data, function(err, result) {
      if (err != null) {
        return _pushTransactionError(transaction, err, function() {
          return rollback(transaction, err, done);
        });
      } else if (result) {
        return _updateTransactionStatus(transaction, 'Processing', 'Committed', function(err) {
          return done(err, 'Committed');
        });
      } else {
        return rollback(transaction, err, done);
      }
    });
  };
  if (transaction.status !== 'Queued') {
    return done(new Error("can't begin work on a transaction which isn't at 'Queued' status (" + transaction.status + ")"), null);
  }
  transaction.status = 'Processing';
  transaction.startedAt = new Date();
  if (transaction.constructor != null) {
    return transaction.constructor(transaction.data, function(err, instructions, rollback) {
      var instruction, _i, _j, _len, _len1;

      if (err != null) {
        return done(err, null);
      }
      for (_i = 0, _len = instructions.length; _i < _len; _i++) {
        instruction = instructions[_i];
        transaction.instructions.push(instruction);
      }
      for (_j = 0, _len1 = rollback.length; _j < _len1; _j++) {
        instruction = rollback[_j];
        transaction.rollback.push(instruction);
      }
      return _transactionsCollection.insert(transaction, doCommit);
    });
  } else {
    return _updateTransactionStatus(transaction, 'Queued', 'Processing', doCommit);
  }
};

exports.db = {
  updateOne: function(db, transactionData, collectionName, selector, values, done) {
    return db.collection(collectionName, {
      strict: true
    }, function(err, collection) {
      var options;

      if (err != null) {
        return done(err, false);
      }
      options = {
        w: 1,
        journal: true,
        upsert: false,
        multi: false,
        serializeFunctions: false
      };
      return collection.update(selector, values, options, function(err, updated) {
        if (err != null) {
          return done(err, null);
        }
        if (updated !== 1) {
          return done(null, false);
        }
        return done(null, true);
      });
    });
  },
  updateOneField: function(db, transactionData, collectionName, selector, field, value, done) {
    return db.collection(collectionName, {
      strict: true
    }, function(err, collection) {
      var options, setter;

      if (err != null) {
        return done(err, false);
      }
      setter = {};
      setter[field] = value;
      options = {
        w: 1,
        journal: true,
        upsert: false,
        multi: false,
        serializeFunctions: false
      };
      return collection.update(selector, {
        $set: setter
      }, options, function(err, updated) {
        if (err != null) {
          return done(err, null);
        }
        if (updated !== 1) {
          return done(null, false);
        }
        return done(null, true);
      });
    });
  },
  updateOneFields: function(db, transactionData, collectionName, selector, fieldValues, done) {
    return db.collection(collectionName, {
      strict: true
    }, function(err, collection) {
      var fieldValue, options, setter, _i, _len;

      if (err != null) {
        return done(err, false);
      }
      setter = {};
      for (_i = 0, _len = fieldValues.length; _i < _len; _i++) {
        fieldValue = fieldValues[_i];
        setter[fieldValue.field] = fieldValue.value;
      }
      options = {
        w: 1,
        journal: true,
        upsert: false,
        multi: false,
        serializeFunctions: false
      };
      return collection.update(selector, {
        $set: setter
      }, options, function(err, updated) {
        if (err != null) {
          return done(err, null);
        }
        if (updated !== 1) {
          return done(null, false);
        }
        return done(null, true);
      });
    });
  },
  insert: function(db, transactionData, collectionName, documents, done) {
    return db.collection(collectionName, {
      strict: true
    }, function(err, collection) {
      var options;

      if (err != null) {
        return done(err, false);
      }
      options = {
        w: 1,
        journal: true,
        serializeFunctions: false
      };
      return collection.insert(documents, options, function(err, objects) {
        if (err != null) {
          return done(err, null);
        }
        return done(null, true);
      });
    });
  }
};

exports.transaction = function(queueName) {
  var transaction;

  return transaction = {
    softwareVersion: _softwareVersion,
    queue: queueName,
    position: 1,
    startedAt: null,
    enqueuedAt: null,
    enqueuedBy: "username",
    status: "Queued",
    errors: [],
    data: {},
    instructions: [],
    rollback: [],
    constructor: null
  };
};
