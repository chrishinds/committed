async = require 'async'
ObjectID = require('mongodb').ObjectID

_state = 'stopped'
_queues = null
_db = null
_registry = {}

_softwareVersion = null
_queueCountersCollection = null
_transactionsCollection = null
_pkFactory = null

class DefaultPkFactory
    createPk: () ->
        new ObjectID()

#optional params for start are [softwareVersion, pkFactory]
#PkFactories work the same as those in node-mongodb-native, this is necessary
#for the insert instruction, which assumes presence of _id before giving the
#document over to the mongo driver

exports.start = (db, optionals..., done) ->
    if _state isnt 'stopped'
        return done( new Error("committed has already been started") )
    _softwareVersion = optionals[0]
    _pkFactory = if optionals[1]? then new optionals[1]() else new DefaultPkFactory()
    _db = db

    tasks = []

    tasks.push (done) ->
        #create the transactionsCollection
        _db.createCollection 'transactions', {w:1, journal: true}, (err, collection) ->
            if err?
                return done(err, null)
            _transactionsCollection = collection
            _db.ensureIndex 'transactions', {status:1}, {w:1, journal: true}, done


    tasks.push (done) ->
        #create the transactionsCollection
        _db.createCollection 'queueCounters', {w:1, journal: true}, (err, collection) ->
            if err?
                return done(err, null)
            _queueCountersCollection = collection
            _db.ensureIndex 'queueCounters', {queue:1}, {w:1, journal: true}, done

    tasks.push (done) ->
        #find any tasks in the transaction queues which are stuck at
        #'Processing' status and roll them back rollback should then leave the
        #transaction at a 'Failed' status, which is at least safe we'll sort
        #by queue position, different queues could be interspliced, but queues
        #should be semantically independent so that isnt problematic, what
        #important is that the sequence of each transaction is respected
        #within the queue take the transactions in ascending sequence order.
        #Any transactions resulting from a committed.immediately will have a
        #position of -1 so will be attempted first
        _transactionsCollection.find({status: 'Processing'}, {snapshot: true}).sort 'position', 1, (err, transactions) ->
            if err? then return done(err, null)
            #do the rollback quietly, we don't want an error to interrupt the async.series
            async.eachSeries transactions, _quietly(rollback), done


    tasks.push (done) ->
        #restart the queues, actually it's easier just to execute all
        #transactions in series. it would be possible to form new queues and
        #do this asynchronously. if startup is slow then look at upgrading
        #this. again, committed.immediately transactions with position = -1
        #will be done first
        _transactionsCollection.find({status: 'Queued'}, {snapshot: true}).sort 'position', 1, (err, transactions) ->
            #similar to above, do the commit quietly so that errors don't abort the async.series.
            async.eachSeries transactions, _quietly(commit), done


    #run the start-up tasks
    async.series tasks, (err, results) ->
        if err? then return done(err)
        #re-create the _queue object, initially this should just contain the
        #GlobalLock queue, others will be created as needed. We create the
        #GlobalLock queue here at the end of start because stop will reset the
        #drain (for good reason) and so start needs to put it back.
        _queues =
            GlobalLock: async.queue(commit, 1)
        _queues.GlobalLock.drain = () ->
            #if our global lock queue is now empty, make sure to restart the other queues
            _state = 'started'
        #now start the queues for the first time
        _state = 'started'
        return done(null)


#to end it all: we should stop accepting new transactions and drain off all the
#queues this should include draining the GlobalLock queue, and we should be
#careful to respect how _state is used by GlobalLock
exports.stop = (done) ->
    if _state isnt 'started'
        return done( new Error("committed is not currently started") )
    #stop accepting new transactions
    _state = 'stopped'
    #drain every queue we have available (even the GlobalLock)
    _drainQueues ( name for name of _queues ), done


_drainQueues = (queues, done) ->
    #create a task to drain each queue
    tasks = []
    for name in queues
        do (name) ->
            tasks.push (done) ->
                if _queues[name].length() is 0
                    #if queue is empty then nothing more to do
                    return done(null, null)
                else
                    #add a drain to the queue to callback after the last task is completed
                    _queues[name].drain = () ->
                        done(null, null)
    #run the drain tasks and wait for the queues to empty
    async.parallel tasks, (err, results) ->
        #unset the drain of every queue
        _queues[name].drain = undefined for name in queues
        done(err)


#can throw Error
exports.register = (name, fnOrObj) ->
    parent = _registry
    #find the parent within the registry for the given (possibly dot-separated) name
    ancestors = name.split('.')
    key = ancestors.pop() #modifies ancestors, obv
    for ancestor in ancestors
        parent = parent[ancestor]
        if not parent?
            throw new Error("during register: path #{name} doesn't exist in registry: #{JSON.stringify(_registry)}")
    if parent[key]?
        throw new Error("during register: path #{name} already exists in registry: #{JSON.stringify(_registry)}")
    #set the registry to record the given function or Object
    parent[key] = fnOrObj


_queuePosition = (queueName, done) ->
    #findAndModify will return the old object, not the updated one
    _queueCountersCollection.findAndModify(
        {queue: queueName},
        {},
        {$inc:{nextPosition: 1}},
        {upsert: true, w: 1, journal: true},
        (err, doc) ->
            if err? then return done(err, null)
            done(null, doc.nextPosition ? 0)
    )


_enqueueOrCreateAndEnqueue = (queueName, transaction, done) ->
    if not _queues[queueName]?
        #the commit fn will be the queue worker; only one worker per queue for sequentiality
        _queues[queueName] = async.queue(commit, 1)
        #note: we could add a drain to this queue to delete on completion. this
        #would minimise memory at the expense of speed.
    _queues[queueName].push transaction, done


exports.enqueue = (transaction, done) ->
    transaction.enqueuedAt = new Date()
    enqueue = () ->
        if _state is 'started'
            return _enqueueOrCreateAndEnqueue transaction.queue, transaction, done
        else
            _updateTransactionStatus transaction, transaction.status, 'Failed', (err) ->
                return done(err, 'Failed')
    #check we've got a valid queuename in the transaction.
    if transaction.queue is "GlobalLock"
        return done( new Error("Can't queue a transaction for GlobalLock using the committed.enqueue function"), null)
    if not transaction.queue?.length
        return done( new Error("must have a transaction.queue parameter to use committed.enqueue"), null)
    if transaction.status? and transaction.status isnt 'Queued'
        return done( new Error("Can't queue a transaction which is at a status other than Queued (or null)"))

    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        transaction.status = if _state is 'started' then "Queued" else "Failed"

        #if we have got here then we intend at least to save the transaction, so
        #set some essential values. This includes setting the queue position
        _queuePosition transaction.queue, (err, position) ->
            transaction.position = position
            if transaction.constructor?
                return enqueue()
            else
                return _transactionsCollection.insert transaction, (err, docs) ->
                    if err? then return done(err, null)
                    #transaction has been written to the db, we're ready to enqueue, or bail if we've failed
                    return enqueue()


exports.immediately = (transaction, done) ->
    #no queue necessary, commit straight away, useful for inserts, but where you need a transaction as an audit record.
    # but as above, respect _state, and fail otherwise.
    transaction.enqueuedAt = new Date()
    go = () ->
        if _state is 'started'
            return commit transaction, done
        else
            _updateTransactionStatus transaction, transaction.status, 'Failed', (err) ->
                return done(err, 'Failed')

    if transaction.queue?
        return done( new Error("Can't call committed.immediately on a transaction which has a queue defined for it"))
    if transaction.status? and transaction.status isnt 'Queued'
        return done( new Error("Can't queue a transaction which is at a status other than Queued (or null)"))

    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        transaction.status = if _state is 'started' then "Queued" else "Failed"
        #if we set the position to -1 then during start up if we want to process an immediate task it will get done first.
        transaction.position = -1
        if transaction.constructor?
            return go()
        else
            _transactionsCollection.insert transaction, (err, docs) ->
                if err? then return done(err, null)
                #transaction has been written to the db, it can be executed, immediately
                return go()


exports.withGlobalLock = (transaction, done) ->
    #useful if you want to safely update fixtures, or do aggregate calculations.
    # magic "GlobalLock" queue, with special drain function, that's set up during start
    if transaction.queue isnt "GlobalLock"
        return done( new Error("Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"), null)
    if transaction.status? and transaction.status isnt 'Queued'
        return done( new Error("Can't queue a transaction which is at a status other than Queued (or null)"))

    transaction.enqueuedAt = new Date()
    enqueue = () ->
        #drain all non-GlobalLock queues
        _drainQueues ( name for name of _queues when name isnt 'GlobalLock' ), (err) ->
            if err? then return done(err, null)
            #we will rely on the special drain set up on GlobalLock made
            #during committed.start, to restore business as usual
            _queues['GlobalLock'].push transaction, done

    #set essential values so the transaction can be saved
    transaction.status = if _state isnt 'stopped' then "Queued" else "Failed"
    #this includes setting the queue position
    _queuePosition transaction.queue, (err, position) ->
        transaction.position = position
        if transaction.constructor?
            return enqueue()
        else
            # lock down the other queues before doing the DB operation as that
            # will take some time
            if _state isnt 'stopped'
                oldState = _state
                _state = 'locked'

            return _transactionsCollection.insert transaction, (err, docs) ->
                if err?
                    # Put the state back the way we found it
                    _state = oldState
                    return done(err, null)
                if _state is 'stopped'
                    return done(null, 'Failed')
                else
                    return enqueue()



applyToRegistered = (name, fnArgs) ->
    found = _registry
    for key in name.split('.')
        found = found[key]
        if not found?
            done = fnArgs[fnArgs.length - 1]
            return done( new Error("during applyToRegistered: path #{name} doesn't exist in registry: #{JSON.stringify(_registry)}") )
    # db is always the first argument for any registered function
    found.apply(@, fnArgs)


#done(err, result)
execute = (instructions, data, done) ->
    result = true
    iterator = (instruction, iteratorDone) ->
        #assemble the arguments for the call of one of our registered functions
        fnArgs = [_db, data]
        if instruction.arguments?
            fnArgs = fnArgs.concat(instruction.arguments)
        #add the callback
        fnArgs.push (err, iteratorResult) ->
            if iteratorResult is false then result = false
            iteratorDone(err)
        #call the function itself
        applyToRegistered instruction.name, fnArgs
    #run instructions in parallel
    async.each instructions, iterator, (err) ->
        done(err, result)


_quietly = (fn) ->
    return (args..., done) ->
        quietDone = (err, result) ->
            if err?
                #this needs to go out to some kind of error notification...
                console.log "error suppressed, probably during committed.start: #{err}"
            done(null, result)
        return fn(args..., quietDone)

_pushTransactionError = (transaction, error, done) ->
    # builtin errors don't serialise to json nicely
    if error.name? and error.stack? and error.message?
        serialisedError = 
            name: error.name
            message: error.message
            stack: error.stack
    else
        serialisedError = error.toString()
    transaction.errors.push serialisedError
    _transactionsCollection.update {_id: transaction._id}, {$push: {errors: serialisedError}}, {w:1, journal: true}, (err, updated) ->
        #log these errors but don't pass them up
        if err? then console.log "error saving error to transaction: #{err}"
        done()

_updateTransactionStatus = (transaction, fromStatus, toStatus, done) ->
    _transactionsCollection.update {_id: transaction._id, status: fromStatus},
        {$set: {
            status: toStatus
            softwareVersion: _softwareVersion
            enqueuedAt: transaction.enqueuedAt
            startedAt: transaction.startedAt
        }},
        {w:1, journal: true}, (err, updated) ->
            switch
                when err?
                    _pushTransactionError transaction, err, () ->
                        return done(err)
                when updated isnt 1
                    #then the transaction in the database doesn't match the one we have in memory; we're in bad shape so bail out
                    updateError = new Error('exactly one transaction must be updated when moving to Processing status')
                    _pushTransactionError transaction, updateError, () ->
                        return done(updateError)
                else
                    transaction.softwareVersion = _softwareVersion
                    transaction.status = toStatus
                    return done(null)


#if rollback succeeds then set transaction.status = Failed; if rollback fails set to failStatus
rollback = (transaction, failStatus, done) ->
    if transaction.status isnt 'Processing'
        return done( new Error("can't rollback a transaction that isn't at 'Processing' status"), null)
    execute transaction.rollback, transaction.data, (rollbackErr, result) ->
        failed = rollbackErr? or result is false
        switch
            when failed and transaction.errors.length > 0
                newStatus = 'CatastropheCommitErrorRollbackError'
            when failed and transaction.errors.length is 0
                newStatus = 'CatastropheCommitFailedRollbackError'
            when not failed and transaction.errors.length > 0
                newStatus = 'FailedCommitErrorRollbackOk'
            when not failed and transaction.errors.length is 0
                newStatus = 'Failed'

        #we used the length of transaction.errors above to tell whether
        #there were errors during a commit, now that's done we can add any
        #errors implied by the rollback
        _updateTransactionStatus transaction, 'Processing', newStatus, (statusErr) ->
            if statusErr?
                done(statusErr, newStatus)
            else if rollbackErr?
                _pushTransactionError transaction, rollbackErr, () ->
                    done(rollbackErr, newStatus)
            else
                done(null, newStatus)


commit = (transaction, done) ->
    doCommit = (err) ->
        if err? then return done(err, null)
        #transaction is now at 'Processing' status, we can now execute its instructions.
        execute transaction.instructions, transaction.data, (err, result) ->
            if err?
                _pushTransactionError transaction, err, () ->
                    #if there's been an error in execution then we need to rollback everything
                    rollback transaction, err, done
            else if result
                #then the transaction has succeeded.
                _updateTransactionStatus transaction, 'Processing', 'Committed', (err) ->
                    return done(err, 'Committed')
            else
                #the transaction has (legitimately) failed
                rollback transaction, err, done
    #is this transaction at queued status?
    if transaction.status isnt 'Queued'
        return done( new Error("can't begin work on a transaction which isn't at 'Queued' status (#{transaction.status})"), null )
    transaction.status = 'Processing'
    transaction.startedAt = new Date()
    if transaction.constructor?
        #then this is a pessimistic transaction with a transaction-producing function
        transaction.constructor transaction.data, (err, instructions, rollback) ->
            if err? then return done(err, null)
            #push instructions and rollaback onto whatever's already on the transaction
            transaction.instructions.push(instruction) for instruction in instructions
            transaction.rollback.push(instruction) for instruction in rollback
            return _transactionsCollection.insert transaction, doCommit
    else
        return _updateTransactionStatus transaction, 'Queued', 'Processing', doCommit


#takes an object (which can be serialised in mongo) and converts it to one
#containing real mongo operations by putting '$' on the front of every key
#that begins with the characters '__'
_mongolize = (operations) ->
    if Array.isArray operations
        return operations
    if typeof operations is 'object'
        mongoOps = {}
        for key, value of operations
            if key.indexOf('__') is 0 #then the string starts with '__', so it's a mongo operator
                mongoOps['$'+key.slice(2)] = _mongolize(value)
            else
                mongoOps[key] = _mongolize(value)
        return mongoOps
    else 
        return operations 

_updateOneOptions =
    w:1
    journal: true
    upsert:false
    multi: false
    serializeFunctions: false

_revisionedUpdateChecks = (revisionName, newPartialDocument, oldPartialDocument) ->
    if not (newPartialDocument.revision? and newPartialDocument.revision[revisionName]? and newPartialDocument._id?)
        return new Error("unable to do a revisioned update with a newPartialDocument which does not contain the specified revision key (#{revisionName}), or is missing _id")
    if not (oldPartialDocument.revision? and oldPartialDocument.revision[revisionName]? and oldPartialDocument._id?)
        return new Error("unable to do a revisioned update with a oldPartialDocument which does not contain the specified revision key (#{revisionName}), or is missing _id")
    if oldPartialDocument.revision[revisionName] isnt newPartialDocument.revision[revisionName]
        return new Error("revision numbers between old and newPartialDocument must match (#{oldPartialDocument.revision[revisionName]} isnt #{newPartialDocument.revision[revisionName]}")
    if typeof oldPartialDocument.revision[revisionName] isnt 'number'
        return new Error("unable to do a revisioned update, revision.#{revisionName} isn't of type number")
    return null


# should be possible to do committed.register('db', committed.db) to use these fns
# we will need to review the list the operations in due course, once they've matured a bit.
exports.db =
    updateOne: (db, transactionData, collectionName, selector, values, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            collection.update selector, values, _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done( null, false) #the instruction has failed
                return done(null, true)

    updateOneField: (db, transactionData, collectionName, selector, field, value, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            setter = {}
            setter[field] = value
            collection.update selector, {$set: setter}, _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done( null, false) #the instruction has failed
                return done(null, true)

    # like updateOneField, but takes an array of key-value pairs to update
    updateOneFields: (db, transactionData, collectionName, selector, fieldValues, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            setter = {}
            for fieldValue in fieldValues
                setter[fieldValue.field] = fieldValue.value
            collection.update selector, {$set: setter}, _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done( null, false) #the instruction has failed
                return done(null, true)

    #--- the following are all paired instructions ---

    insert: (db, transactionData, collectionName, documents, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            if not documents.length? then documents = [documents]
            for d in documents
                if not d._id? then d._id = _pkFactory.createPk()
            options =
                w:1
                journal: true
                serializeFunctions: false
            collection.insert documents, options, (err, objects) ->
                if err? then return done(err, null)
                return done(null, true)

    insertRollback: (db, transactionData, collectionName, documents, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            options =
                w:1
                journal: true
                serializeFunctions: false
            if not documents.length? then documents = [documents]
            iterator = (item, itemDone) ->
                if not item._id? then return itemDone(new Error("unable to rollback an insert of document which doesn't have an _id field, this could be unsafe"))
                #remove justOne item, but only if it's an exact match for the inserted document
                collection.remove item, options, (err, result) ->
                    itemDone(err)
            async.each documents, iterator, (err) ->
                done(err, true) #rollbacks only fail if there's an error

    updateOneOp: (db, transactionData, collectionName, selector, updateOps, rollbackOps, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            collection.update _mongolize(selector), _mongolize(updateOps), _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done(null, false) #the instruction has failed
                return done(null, true)

    updateOneOpRollback: (db, transactionData, collectionName, selector, updateOps, rollbackOps, done) ->
        #Rollback for an updateOneOp is simply an update using the rollbackOps
        return exports.db.updateOneOp(db, transactionData, collectionName, selector, rollbackOps, {}, done)

    revisionedUpdate: (db, transactionData, collectionName, revisionName, newPartialDocument, oldPartialDocument, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            fail = _revisionedUpdateChecks(revisionName, newPartialDocument, oldPartialDocument)
            if fail? then return done(fail)

            revisionKey = "revision.#{revisionName}"
            #create a mongo selector to find the exact revision given in newPartialDocument
            mongoSelector = 
                _id: newPartialDocument._id
            mongoSelector[revisionKey]= oldPartialDocument.revision[revisionName]
            #however, we don't want to actually $set ANY revision values, so create a fields object without the revision subdoc
            fields = {}
            fields[key] = value for key,value of newPartialDocument when key isnt 'revision' and key isnt '_id'
            #now create mongo operations to $increment the revision and $set the given fields
            mongoOps = 
                $set: fields
                $inc: {}
            mongoOps.$inc[revisionKey] = 1
            collection.update mongoSelector, mongoOps, _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done(null, false) #the instruction has failed
                return done(null, true)

    revisionedUpdateRollback: (db, transactionData, collectionName, revisionName, newPartialDocument, oldPartialDocument, done) ->
        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            fail = _revisionedUpdateChecks(revisionName, newPartialDocument, oldPartialDocument)
            if fail? then return done(fail)

            revisionKey = "revision.#{revisionName}"
            #create a document with only the revision for this partial document, we must rollback ONLY our revision number
            oldFields = {}
            oldFields[revisionKey] = oldPartialDocument.revision[revisionName]
            oldFields[key] = value for key,value of oldPartialDocument when key isnt 'revision' and key isnt '_id'

            #create a mongo selector which will match the document only if our original update operation was successful
            newFields = {}
            newFields[revisionKey] = newPartialDocument.revision[revisionName] + 1 #if we succeeded before then we'll have incremented this value
            newFields[key] = value for key,value of newPartialDocument when key isnt 'revision'

            #find the exact document we would have set if the update had been successful, and update fields to be exactly what we had before
            collection.update newFields, { $set: oldFields }, _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                # doesn't matter whether we updated anything, the original operation may have failed due to collision
                return done(null, true)



exports.transaction = (queueName, username) ->
    transaction =
        softwareVersion: _softwareVersion
        queue: queueName
        position: 1
        startedAt: null
        enqueuedAt: null
        enqueuedBy: username
        status: "Queued"
        errors: []
        data: {}
        instructions: []
        rollback: []
        constructor: null