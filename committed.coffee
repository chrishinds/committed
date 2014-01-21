async = require 'async'
ObjectID = require('mongodb').ObjectID

_state = 'stopped'
_queueLength = {}
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
        _queueLength.GlobalLock = 0
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
    if _state is 'stopped'
        return done( new Error("committed is not currently started") )
    #stop accepting new transactions
    _state = 'stopped'
    #drain every queue we have available (even the GlobalLock)
    _drainQueues ( name for name of _queues ), done


_drainQueues = (queues, done) ->
    # wait until every _queue's _queueLength is 0, check every 10ms
    async.until(
        () ->
            (_queueLength[name] is 0 for name in queues).every((x) -> x)
        , (untilBodyDone) ->
            setTimeout untilBodyDone, 10
        , (err) ->
            done(err)
    )


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
        _queueLength[queueName] = 0
    #we're about to add an item to the queue, so increment the queue count
    _queueLength[queueName] += 1
    _queues[queueName].push transaction, (etc...) ->
        _queueLength[queueName] -= 1
        done(etc...)

_checkTransaction = (transaction) ->
    if transaction.status? and transaction.status isnt 'Queued'
        return new Error("Can't queue a transaction which is at a status other than Queued (or null)")
    if not _instructionsExistFor(transaction)
        return new Error("Can't queue a transaction for which either its instructions, rollback instructions, or implied auto rollback instructions don't exist in the registry")
    if transaction.rollback? and transaction.rollback.length isnt transaction.instructions.length
        return new Error("Can't queue a transaction with an explicit rollback instructions whose length is not the same as its instructions array")


_instructionsExistFor = (transaction) ->
    if transaction.rollback? #if there's an explicit rollback array then check that all its instructions are good
        for i in transaction.instructions.concat(transaction.rollback)
            if not _registryFind(i.name)? then return false
    else #if there's no explicit rollback array then find implicit rollback instructions.
        for i in transaction.instructions
            if not _registryFind(i.name)? then return false
            if not _registryFind(i.name+'Rollback')? then return false
    return true


_setUpTransaction = (transaction) ->
    #clone the instructions for any implicit rollback before the transaction hits
    #the db to ensure that inadvertant changes to the instructions params during
    #instruction execution dont prejudice the params rollbacks
    if not transaction.rollback?
        transaction.rollback = []
        for i in transaction.instructions
            iRoll = _clone i
            iRoll.name = iRoll.name+'Rollback'
            transaction.rollback.push iRoll
    #every instruction gets its own object in which to hold state, eg _ids for insert
    transaction.execution.state = []
    transaction.execution.state.push {} for i in transaction.instructions


exports.enqueue = (transaction, done) ->
    transaction.enqueuedAt = new Date()
    enqueue = () ->
        if _state is 'started'
            return _enqueueOrCreateAndEnqueue transaction.queue, transaction, done
        else
            #we're at state 'locked' or 'stopped', either way fail the
            #transaction and save it to the db so we can see our failures
            transaction.execution.info.push "unable to execute instruction, committed was at state '#{_state}'"
            _updateTransactionStatus transaction, transaction.status, 'Failed', (err) ->
                return done(err, 'Failed')
    #check we've got a valid queuename in the transaction.
    if transaction.queue is "GlobalLock"
        return done( new Error("Can't queue a transaction for GlobalLock using the committed.enqueue function"), null)
    if not transaction.queue?.length
        return done( new Error("must have a transaction.queue parameter to use committed.enqueue"), null)
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 

    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        transaction.status = if _state is 'started' then "Queued" else "Failed"

        #if we have got here then we intend at least to save the transaction, so
        #set some essential values. This includes setting the queue position
        _queuePosition transaction.queue, (err, position) ->
            transaction.position = position
            _setUpTransaction(transaction)
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
            transaction.execution.info.push "unable to execute instruction, committed was at state '#{_state}'"
            _updateTransactionStatus transaction, transaction.status, 'Failed', (err) ->
                return done(err, 'Failed')

    if transaction.queue?
        return done( new Error("Can't call committed.immediately on a transaction which has a queue defined for it"))
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 

    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        transaction.status = if _state is 'started' then "Queued" else "Failed"
        #if we set the position to -1 then during start up if we want to process an immediate task it will get done first.
        transaction.position = -1
        _setUpTransaction(transaction)
        _transactionsCollection.insert transaction, (err, docs) ->
            if err? then return done(err, null)
            #transaction has been written to the db, it can be executed, immediately
            return go()


exports.withGlobalLock = (transaction, done) ->
    #useful if you want to safely update fixtures, or do aggregate calculations.
    # magic "GlobalLock" queue, with special drain function, that's set up during start
    if transaction.queue isnt "GlobalLock"
        return done( new Error("Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"), null)
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 

    transaction.enqueuedAt = new Date()
    enqueue = () ->
        #drain all non-GlobalLock queues
        _drainQueues ( name for name of _queues when name isnt 'GlobalLock' ), (err) ->
            if err? then return done(err, null)
            #we will rely on the special drain set up on GlobalLock made
            #during committed.start, to restore business as usual
            #GlobalLock is created at start, so it won't be created with the following:
            _enqueueOrCreateAndEnqueue transaction.queue, transaction, done

    #set essential values so the transaction can be saved
    transaction.status = if _state isnt 'stopped' then "Queued" else "Failed"
    #this includes setting the queue position
    _queuePosition transaction.queue, (err, position) ->
        transaction.position = position
        _setUpTransaction(transaction)
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


_registryFind = (name) ->
    found = _registry
    for key in name.split('.')
        found = found[key]
        if not found?
            return null
    return found 

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
execute = (instructions, state, transaction, done) ->
    results = []
    iterator = ([instruction, state], iteratorDone) ->
        #assemble the arguments for the call of one of our registered functions
        args = if instruction.arguments? then instruction.arguments else []
        if not Array.isArray(args) then args = [args]
        fnArgs = [_db, transaction, state, args]
        #add the callback
        fnArgs.push (err, iteratorResult) ->
            #if an instruction has 'failed' by returning false, interrupt the series execution by returning an error
            if not err? and iteratorResult is false
                err = 'instructionFailed'
            results.push iteratorResult
            return iteratorDone(err)
        #call the instruction's function
        applyToRegistered instruction.name, fnArgs
    #execute all the instructions in series
    instructionsWithState = ([instruction, state[i]] for instruction, i in instructions )
    async.eachSeries instructionsWithState, iterator, (err) ->
        #we may have terminated series by returning 'instructionFailed', however, it's necesary to separate failure from error
        if err is 'instructionFailed' then err = null
        return done(err, results)


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
    transaction.execution.errors.push serialisedError
    _transactionsCollection.update {_id: transaction._id}, {$push: {'execution.errors': serialisedError}}, {w:1, journal: true}, (err, updated) ->
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
            lastUpdatedAt: new Date()
            execution: transaction.execution
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

_updateTransactionState = (transaction, done) ->
    _transactionsCollection.update { _id: transaction._id }, {$set: 'executions.state': transaction.execution.state}, _updateOneOptions, (err,updated) ->
        if not err? and updated isnt 1 then err = new Error("transaction not correctly updated with instruction state before insert")
        done(err)

_clone = (obj, mongolize=false) ->


    if not obj? or typeof obj isnt 'object'
        return obj

    if obj instanceof Array
        return ( _clone(x, mongolize) for x in obj )

    if obj instanceof Date
        return new Date(obj.getTime()) 

    if obj instanceof RegExp
        flags = ''
        flags += 'g' if obj.global?
        flags += 'i' if obj.ignoreCase?
        flags += 'm' if obj.multiline?
        flags += 'y' if obj.sticky?
        return new RegExp(obj.source, flags) 

    newInstance = new obj.constructor()
    for key,value of obj
        if mongolize and key.indexOf('__') is 0 #then the string starts with '__', so it's a mongo operator
            newInstance['$'+key.slice(2)] = _clone(value, mongolize)
        else
            newInstance[key] = _clone(value, mongolize)

    return newInstance


#takes an object (which can be serialised in mongo) and converts it to one
#containing real mongo operations by putting '$' on the front of every key
#that begins with the characters '__'
_mongolize = (obj) ->
    _clone(obj, true)


#if rollback succeeds then set transaction.status = Failed
rollback = (transaction, results, done) ->
    if transaction.status isnt 'Processing'
        return done( new Error("can't rollback a transaction that isn't at 'Processing' status"), null)
    #we may not have executed everything in the original list of instructions
    rollbackInstructions = transaction.rollback.slice(0, results.length)
    rollbackState = transaction.execution.state.slice(0, results.length)
    #and we need to rollback in reverse order
    rollbackInstructions.reverse()
    rollbackState.reverse()
    execute rollbackInstructions, rollbackState, transaction, (rollbackErr, results) ->
        failed = rollbackErr? or not results.every( (x) -> x )
        switch
            when failed and transaction.execution.errors.length > 0
                newStatus = 'CatastropheCommitErrorRollbackError'
            when failed and transaction.execution.errors.length is 0
                newStatus = 'CatastropheCommitFailedRollbackError'
            when not failed and transaction.execution.errors.length > 0
                newStatus = 'FailedCommitErrorRollbackOk'
            when not failed and transaction.execution.errors.length is 0
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
        execute transaction.instructions, transaction.execution.state, transaction, (err, results) ->
            if err?
                return _pushTransactionError transaction, err, () ->
                    #if there's been an error in execution then we need to rollback everything
                    return rollback transaction, results, done
            else if results.length is transaction.instructions.length and results.every( (x) -> x )
                #then the transaction has succeeded.
                return _updateTransactionStatus transaction, 'Processing', 'Committed', (err) ->
                    return done(err, 'Committed')
            else
                #the transaction has (legitimately) failed
                failureInfo = "legitimate transaction failure, instruction results: #{JSON.stringify results}"
                transaction.execution.info.push failureInfo
                return _transactionsCollection.update {_id: transaction._id}, {$push: {'execution.info': failureInfo}}, {w:1, journal: true}, (err, updated) ->
                    #log these errors but don't pass them up
                    if err? then console.log "error saving error to transaction: #{err}"
                    return rollback transaction, results, done

    #is this transaction at queued status?
    if transaction.status isnt 'Queued'
        return done( new Error("can't begin work on a transaction which isn't at 'Queued' status (#{transaction.status})"), null )
    transaction.status = 'Processing'
    transaction.startedAt = new Date()
    return _updateTransactionStatus transaction, 'Queued', 'Processing', doCommit




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
#note: values passed to instructions should generally be considered immutable,
#as they'll be cloned after commit in the event of implicit rollback
#instructions must return done(err, result) where result is true for success and false for failure
exports.db =
    pass: (db, transaction, state, args, done) ->
        done(null, true)

    insert: (db, transaction, state, [collectionName, documents, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to insert"))
        if not (collectionName? and documents?) then return done(new Error("null or missing argument to insert command"))

        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            if not Array.isArray(documents) then documents = [documents]
            #inserts should _id every document, otherwise accurate rollback won't be possible, moreover these ids need to be saved somewhere
            state.ids = []
            for d in documents
                if d._id? 
                    state.ids.push d._id
                else
                    d._id = _pkFactory.createPk()
                    state.ids.push d._id
            options =
                w:1
                journal: true
                serializeFunctions: false
            _updateTransactionState transaction, (err) ->
                if err? then return done(err, null)
                collection.insert documents, options, (err, objects) ->
                    if err? then return done(err, null)
                    return done(null, true)

    insertRollback: (db, transaction, state, [collectionName, documents, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to insertRollback"))
        if not (collectionName? and documents?) then return done(new Error("null or missing argument to insertRollback command"))

        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            options =
                w:1
                journal: true
                serializeFunctions: false
            if not state.ids?
                #then we never got to the insert, so there's nothing for the rollback to do
                return done(null, true)
            else
                #where documents lack an _id, we have to fill this in from what was generated during the insert instruction. 
                if not Array.isArray(documents) then documents = [documents]
                if documents.length isnt state.ids.length
                    return done(new Error("during rollback, number of documents to insert doesnt match the number of ids generated during insert"), false)
                for d,i in documents
                    if not d._id? then d._id = state.ids[i]
                iterator = (item, itemDone) ->
                    #remove justOne item, but only if it's an exact match for the inserted document
                    collection.remove item, options, (err, result) ->
                        itemDone(err)
                return async.each documents, iterator, (err) ->
                    return done(err, true) #rollbacks only fail if there's an error

    updateOneOp: (db, transaction, state, [collectionName, selector, updateOps, rollbackOps, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneOp"))
        if not (collectionName? and selector? and updateOps? and rollbackOps?) then return done(new Error("null or missing argument to updateOneOp command"))

        db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            collection.update _mongolize(selector), _mongolize(updateOps), _updateOneOptions, (err, updated) ->
                if err? then return done(err, null)
                if updated isnt 1 then return done(null, false) #the instruction has failed
                return done(null, true)

    updateOneOpRollback: (db, transaction, state, [collectionName, selector, updateOps, rollbackOps, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneOp"))
        if not (collectionName? and selector? and updateOps? and rollbackOps?) then return done(new Error("null or missing argument to updateOneOp command"))
        #Rollback for an updateOneOp is almost an update using the rollbackOps, except that it always results in true
        return exports.db.updateOneOp db, transaction, state, [collectionName, selector, rollbackOps, {}], (err, result) ->
            done(err, true)

    revisionedUpdate: (db, transaction, state, [collectionName, revisionName, newPartialDocument, oldPartialDocument, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to revisionedUpdate"))
        if not (collectionName? and revisionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to revisionedUpdate command"))

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

    revisionedUpdateRollback: (db, transaction, state, [collectionName, revisionName, newPartialDocument, oldPartialDocument, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to revisionedUpdateRollback"))
        if not (collectionName? and revisionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to revisionedUpdateRollback command"))

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


exports.transaction = (queueName, username, implicitRollbackInstructions=true) ->
    transaction =
        softwareVersion: _softwareVersion
        queue: queueName
        position: 1
        startedAt: null
        enqueuedAt: null
        lastUpdatedAt: null
        enqueuedBy: username
        status: "Queued"
        data: {}
        instructions: []
        rollback: if implicitRollbackInstructions then null else []
        execution:
            state: []
            errors: []
            info: []