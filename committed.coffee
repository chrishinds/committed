async = require 'async'
ObjectID = require('mongodb').ObjectID

_state = 'stopped'
_queueLength = {}
_immediateCounter = 0
_queues = null
_db = null
_registry = {}
_pending = []
_pendingImmediate = []

_softwareVersion = null
_queueCountersCollection = null
_transactionsCollection = null
_pkFactory = null
_config = {}

class DefaultPkFactory
    createPk: () ->
        new ObjectID()

_inSeries = (cursor, fn, done) ->
    finished = false
    return async.doUntil(
        (itemDone) ->
            cursor.nextObject (err, item) ->
                if err? 
                    return itemDone(err)
                if not item?
                    finished = true
                    return itemDone()
                else
                    return fn(item, itemDone)
        , () -> 
            finished
        , (err) ->
            done(err)
        )

#config must contain {db} this is a db connection which will be used for the
#queueCounters and transactions collections. config may also contain
# {softwareVersion, pkFactory}. PkFactories work the same as those in node-
#mongodb-native, this is necessary for the insert instruction, which assumes
#presence of _id before giving the document over to the mongo driver. config
#will be passed to every instruction that gets executed so put whatever you
#want in there

exports.start = (config, done) ->
    if not config?.db? 
        return done( new Error("committed must be supplied with at least a config.db during start"))
    if _state isnt 'stopped'
        return done( new Error("committed has already been started") )
    _softwareVersion = config.softwareVersion
    #supply an object that can be used as a factory, or we instantiate our own class for this
    _pkFactory = if config.pkFactory? then config.pkFactory else new DefaultPkFactory()
    _db = config.db
    _config = config
    #reset this state too during start
    _queueLength = {}
    _immediateCounter = 0
    _pending = []
    _pendingImmediate = []

    #re-create the _queue object, initially this should just contain the
    #GlobalLock queue, others will be created as needed. 
    _queues =
        #globallock has a special worker function
        GlobalLock: async.queue(commitWithGlobalLock, 1)
    _queueLength.GlobalLock = 0
    #it also has a special drain
    _queues.GlobalLock.drain = () ->
        #if our global lock queue is now empty, make sure to restart the other queues
        _state = 'started'
        #insert pending transaction into their queues
        while _pending.length isnt 0
            [transaction, done] = _pending.shift()
            _enqueueOrCreateAndEnqueue(transaction.queue, transaction, done)
        #execute any pending immediates in parallel
        while _pendingImmediate.length isnt 0
            [transaction, done] = _pendingImmediate.shift()
            _executeImmediate(transaction, done)

    tasks = []

    tasks.push (done) ->
        #create the transactionsCollection
        _db.createCollection 'transactions', {w:1, journal: true}, (err, collection) ->
            if err? then return done(err, null)
            _transactionsCollection = collection
            _db.ensureIndex 'transactions', {status:1}, {w:1, journal: true}, (err) ->
                if err? then return done(err, null)
                _db.ensureIndex 'transactions', {'after.0.status':1}, {w:1, journal: true}, (err) ->
                    return done(err, null)


    tasks.push (done) ->
        #create the queueCounters collection
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
        #position of -1 so will be attempted first. Execution is stricly sequential.
        _inSeries( 
            _transactionsCollection.find({status: 'Processing'}, {sort: position: 1})
            , (transaction, transactionDone) ->
                rollback transaction, (err, result) ->
                    #eat the errors to ensure we continue to progress through the collection
                    if err? then console.error err.name, err.message, err.stack
                    return transactionDone()
            , done
            )

    tasks.push (done) ->
        #there is a small chance that a chain has stalled: if there was a
        #crash after a transaction was written back to the db as 'Committed'
        #and before it is shifted so that the next transaction is in the head
        #position. Find such chains, shift them, write them back, execute in
        #the next task
        _inSeries( 
            _transactionsCollection.find({status:'Committed', "after.0.status":'Queued'}, {snapshot: true})
            , (transaction, transactionDone) ->
                _shiftChain(transaction)
                return _transactionsCollection.save transaction, {w:1, journal: true}, (err, result) ->
                    if err? then console.error err.name, err.message, err.stack
                    return transactionDone()
            , done
            )

    tasks.push (done) ->
        #restart the queues, actually it's easier just to execute all
        #transactions in series. it would be possible to form new queues and
        #do this asynchronously. if startup is slow then look at upgrading
        #this. again, committed.immediately transactions with position = -1
        #will be done first
        _inSeries(
            _transactionsCollection.find({status: 'Queued'}, {sort: position: 1})
            , (transaction, transactionDone) ->
                commit transaction, (err, result) ->
                    #eat any error so we continue
                    if err? then console.error err.name, err.message, err.stack, JSON.stringify(transaction, null, 2)
                    transactionDone()
            , done
            )

    #run the start-up tasks
    async.series tasks, (err, results) ->
        if err? then return done(err)
        #now start the queues for the first time
        _state = 'started'
        return done(null)


#to end it all: we should stop accepting new transactions and drain off all the
#queues this should include draining the GlobalLock queue, and we should be
#careful to respect how _state is used by GlobalLock
exports.stop = (done) ->
    if _state is 'stopped'
        return done( new Error("committed is not currently started") )
    #stop accepting new transactions, but yield to eventloop first to give
    #anything recently queued a chance to get going
    setImmediate () ->
        _state = 'stopped'
        #drain every queue we have available (even the GlobalLock)
        _drainQueues ( name for name of _queues ), done


_drainQueues = (queues, done) ->
    # wait until every _queue's _queueLength is 0, and there are no immediate transactions in progress, check every 10ms
    async.until(
        () ->
            (_queueLength[name] is 0 for name in queues).every((x) -> x) and _immediateCounter is 0
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
    if transaction.after? and not transaction.after.every( (x) -> x.status? and x.status is 'Queued' )
        return new Error("Can't queue a transaction chain where one transaction is at a status other than Queued")


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


#internal enqueue function which skips the _state='stopped' check so that chains can proceed to completion after stop is called
_enqueue = (transaction, done) ->
    transaction.enqueuedAt = new Date()
    if transaction.queue is "GlobalLock"
        return done( new Error("Can't queue a transaction for GlobalLock using the committed.enqueue function"), null)
    if not transaction.queue?.length
        return done( new Error("must have a transaction.queue parameter to use committed.enqueue"), null)
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 

    transaction.status = 'Queued'
    #important not to involve 'stopped' or 'started' in this conditional
    if _state isnt 'locked'
        return _enqueueOrCreateAndEnqueue transaction.queue, transaction, done
    else
        _pending.push [transaction, done]


exports.enqueue = (transaction, done) ->
    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run (eg. there's one on the GlobalLock queue)
    setImmediate () ->
        if _state is 'stopped'
            transaction.status = 'Failed'
            transaction.execution.info.push "unable to enquue transaction, committed was at state '#{_state}'"
            return done(null, 'Failed')
        else
            return _enqueue(transaction, done)


exports.immediately = (transaction, done) ->
    #no queue necessary, commit straight away, useful for inserts, but where you need a transaction as an audit record.
    # but as above, respect _state, and fail otherwise.
    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        if _state is 'stopped'
            transaction.status = 'Failed'
            transaction.execution.info.push "unable to execute transaction immediately, committed was at state '#{_state}'"
            return done(null, 'Failed')
        else
            return _immediately(transaction, done)


_executeImmediate = (transaction, done) ->
    _immediateCounter += 1
    return commit transaction, (etc...) ->
        _immediateCounter -= 1
        done(etc...)

_immediately = (transaction, done) ->
    transaction.enqueuedAt = new Date()
    if transaction.queue?
        return done( new Error("Can't call committed.immediately on a transaction which has a queue defined for it") )
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 

    if _state isnt 'locked'
        transaction.status = 'Queued'
        transaction.position = -1
        _setUpTransaction(transaction)
        return _executeImmediate(transaction, done)
    else
        _pendingImmediate.push [transaction, done]


_withGlobalLock = (transaction, done) ->
    if transaction.queue isnt "GlobalLock"
        return done( new Error("Can't call committed.withGlobalLock for a transaction that names a queue other than 'GlobalLock'"), null)
    error = _checkTransaction(transaction)
    if error? then return done( error, null ) 
    transaction.enqueuedAt = new Date()
    return _enqueueOrCreateAndEnqueue transaction.queue, transaction, done



exports.withGlobalLock = (transaction, done) ->
    #useful if you want to safely update fixtures, or do aggregate calculations.
    # magic "GlobalLock" queue, with special drain function, that's set up during start
    #because exports.enqueue will yield to the event loop before pushing to
    #async.queue, we should also, otherwise we may inadvertently overtake a
    #previous call. setImmediate will preserve order. 
    setImmediate () ->
        if _state is 'stopped' 
            #committed is 'stopped'
            transaction.status = "Failed"
            transaction.execution.info.push "unable to enqueue transaction withGlobalLock, committed was at state '#{_state}'"
            return done(null, 'Failed')
        else
            #then this transaction is good to proceed
            transaction.status = "Queued" 
            _state = 'locked'
            return _withGlobalLock(transaction, done)


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
        fnArgs = [_config, transaction, state, args]
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


_pushTransactionError = (transaction, error, optional..., done) ->
    if optional?
        [results, rollbackResults] = optional
    # builtin errors don't serialise to json nicely
    if error.name? and error.stack? and error.message?
        serialisedError = 
            name: error.name
            message: error.message
            stack: error.stack
    else
        serialisedError = error.toString()
    mongoOps = {$push: {'execution.errors': serialisedError}}
    transaction.execution.errors.push serialisedError
    if results?
        transaction.execution.results = results
        if not mongoOps.$set?
            mongoOps.$set = {}
        mongoOps.$set['execution.results'] = results
    if rollbackResults?
        transaction.execution.rollback = rollbackResults
        if not mongoOps.$set?
            mongoOps.$set = {}
        mongoOps.$set['execution.rollback'] = rollbackResults

    _transactionsCollection.update {_id: transaction._id}, mongoOps, {w:1, journal: true}, (err, updated) ->
        #log these errors but don't pass them up
        if err? then console.log "error saving error to transaction: #{err}"
        done()

_updateTransactionStatus = (transaction, fromStatus, toStatus, optional..., done) ->
    if optional?
        [results, rollbackResults] = optional
    updatedAt = new Date()
    if results?
        transaction.execution.results = results
    if rollbackResults?
        transaction.execution.rollback = rollbackResults
    _transactionsCollection.update {_id: transaction._id, status: fromStatus},
        {$set: {
            status: toStatus
            softwareVersion: _softwareVersion
            enqueuedAt: transaction.enqueuedAt
            startedAt: transaction.startedAt
            lastUpdatedAt: updatedAt
            execution: transaction.execution
        }},
        {w:1, journal: true}, (err, updated) ->
            switch
                when err?
                    _pushTransactionError transaction, err, () ->
                        return done(err)
                when updated isnt 1
                    #then the transaction in the database doesn't match the one we have in memory; we're in bad shape so bail out
                    updateError = new Error("exactly one transaction must be updated when moving from #{fromStatus} to #{toStatus} status")
                    _pushTransactionError transaction, updateError, () ->
                        return done(updateError)
                else
                    transaction.softwareVersion = _softwareVersion
                    transaction.status = toStatus
                    transaction.lastUpdatedAt = updatedAt
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
rollback = (transaction, done) ->
    if transaction.status isnt 'Processing'
        return done( new Error("can't rollback a transaction that isn't at 'Processing' status"), null)
    #have we got a list of results from instruction execution to work with? if so we don't need to execute every rollback instruction
    #if not we must execute every rollback instruction to be safe. 
    rollbackLength = if transaction.execution.results? then transaction.execution.results.length else transaction.instructions.length
    #we may not have executed everything in the original list of instructions
    rollbackInstructions = transaction.rollback.slice(0, rollbackLength)
    rollbackState = transaction.execution.state.slice(0, rollbackLength)
    #and we need to rollback in reverse order
    rollbackInstructions.reverse()
    rollbackState.reverse()
    execute rollbackInstructions, rollbackState, transaction, (rollbackErr, rollbackResults) ->
        failed = rollbackErr? or not rollbackResults.every( (x) -> x )
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
        _updateTransactionStatus transaction, 'Processing', newStatus, null, rollbackResults, (statusErr) ->
            if statusErr?
                done(statusErr, newStatus)
            else if rollbackErr?
                _pushTransactionError transaction, rollbackErr, () ->
                    done(rollbackErr, newStatus)
            else
                done(null, newStatus)


#worker function for the GlobalLock queue
commitWithGlobalLock = (transaction, done) ->
    return _drainQueues ( name for name of _queues when name isnt 'GlobalLock' ), (err) ->
        if err? then return done(err, null)
        commit(transaction, done)


_requeue = (transaction, done) ->
    #call the internal queue inserts, to ensure whole chain is finished before a stop
    if transaction.queue is 'GlobalLock'
        return _withGlobalLock(transaction, done)
    else
        return _enqueue(transaction, done)

_hasAfter = (transaction) ->
    transaction.after? and transaction.after.length isnt 0

_hasBefore = (transaction) ->
    transaction.before? and transaction.before.length isnt 0

#shift this transaction onto the before array, and copy the properties of the
#first transaction on after into this one. _shiftChain MUST NOT alter
#transaction's _id, otherwise we'll not be able to find it in the db.
_shiftChain = (transaction) ->
    nextTransaction = transaction.after.shift()
    copyTransaction = {}
    for key, value of transaction 
        if key not in ['before', 'after', '_id']
            copyTransaction[key] = value 
            transaction[key] = undefined
    transaction.before.push copyTransaction
    for key, value of nextTransaction
        if key not in ['before', 'after', '_id']
            transaction[key] = value

_retry = (transaction, retryFn, failFn) ->



#worker function for regular queues; manages transaction chains, delegates
#commit of the individual transaction to _commitCore
commit = (transaction, done, retries=2) ->
    _commitCore transaction, (err, status) ->
        if not (_hasBefore(transaction) or _hasAfter(transaction))
            #then this transaction isnt a chain, so callback directly
            return done(err, status)
        else 
            #this transaction is a chain
            switch status
                when 'Committed'
                    if _hasAfter(transaction)
                        #then there are transaction yet to execute, we should move onto the next item in the chain
                        _shiftChain(transaction)
                        return _requeue(transaction, done)
                    else
                        return done(err, status)
                when 'Failed', 'FailedCommitErrorRollbackOk'
                    if not _hasBefore(transaction)
                        #then we are the first in the chain, so simply callback and let the client retry if they wish
                        return done(err, status)
                    else
                        #we are not the chain's first transaction, so retry may be worthwhile; wait about a second
                        if retries > 0 
                            setTimeout( 
                                () -> 
                                    _updateTransactionStatus transaction, transaction.status, 'Queued', null, null, "#{status} #{retries} retries remaining", (statusErr) ->
                                        if statusErr? then return done(statusErr, transaction.status)
                                        commit(transaction, done, retries-1)
                                , 500 + Math.floor(1000*Math.random())
                            )
                            return null
                        else
                            #we have run out of retries, change the status to
                            #'ChainFailed' so that the client knows that their
                            #first transaction Committed, but a subsequent one
                            #failed. keep the original status on the
                            #execution.info array
                                return _updateTransactionStatus transaction, transaction.status, 'ChainFailed', null, null, transaction.status, (statusErr) ->
                                    if statusErr? then return done(statusErr, transaction.status)
                                    return done(err, 'ChainFailed')
                else
                    #there's been a catastrophe retry is not appropriate
                    if not _hasBefore(transaction)
                        #then we're the first transaction in the chain, return directly
                        return done(err, status)
                    else
                        #we're not the first transaction, so keep the failing status in execution.info and return 'ChainFailed'
                        return _updateTransactionStatus transaction, transaction.status, 'ChainFailed', null, null, transaction.status, (statusErr) ->
                            if statusErr? then return done(statusErr, transaction.status)
                            return done(err, 'ChainFailed') 


#commits a single transaction
_commitCore = (transaction, done) ->
    #is this transaction at queued status?
    if transaction.status isnt 'Queued'
        return done( new Error("can't begin work on a transaction which isn't at 'Queued' status (#{transaction.status})"), null )
    #the transaction is ready to execute
    # give the transaction a queue position
    return _queuePosition transaction.queue, (err, position) ->
        transaction.position = position
        transaction.status = 'Processing'
        transaction.startedAt = new Date()
        _setUpTransaction(transaction)
        # Need to save the transaction in case of crash, this has to be an
        # upsert because the transaction may or may not already be there. New
        # transactions need a fresh id. During a chain or a restart, the id
        # will already be present, but a save is still necessary to reflect
        # status change, and/or _shiftChain
        if not transaction._id?
            #collision seems unlikely (https://github.com/mongodb/js-bson/blob/master/lib/bson/objectid.js#L106)
            transaction._id = _pkFactory.createPk()
        return _transactionsCollection.update {_id: transaction._id}, transaction, {upsert:true, w:1}, (err, changed) ->
            if not err? and changed isnt 1 then err = new Error("upsert transaction must effect exactly one document")
            if err? then return done(err, null)
            #transaction has been written to the db, we're ready to execute instructions
            execute transaction.instructions, transaction.execution.state, transaction, (err, results) ->
                if err?
                    return _pushTransactionError transaction, err, results, () ->
                        #if there's been an error in execution then we need to rollback everything
                        return rollback transaction, done
                else if results.length is transaction.instructions.length and results.every( (x) -> x )
                    #then the transaction has succeeded.
                    return _updateTransactionStatus transaction, 'Processing', 'Committed', results, (err) ->
                        if err?
                            _transactionsCollection.findOne {_id:transaction._id}, (err, doc) ->
                        return done(err, 'Committed')
                else
                    #the transaction has (legitimately) failed, write the results, and prepare for rollback 
                    _updateTransactionStatus transaction, transaction.status, transaction.status, results, null, "legitimate transaction failure", (statusErr) ->
                        #log these errors but don't pass them up
                        if err? then console.error "error saving transaction status change on legitimate failure: #{err}"
                        return rollback transaction, done


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

_checkInstructionRevisionConfig = (instructionName, config, collectionName, revisions) ->
    if revisions.length isnt 0 and (not config.revisions? or not config.revisions[collectionName]?)
        return new Error("can't #{instructionName} documents with revisions #{revisions} when config.revisions has been supplied at committed.start for #{collectionName}")
    for revisionName in revisions
        if revisionName not in config.revisions[collectionName]
            return new Error("can't #{instructionName} documents with a revision '#{revisionName}' not specified in the config.revisions given at committed.start")
    if revisions.length is 0 and config.revisions? and config.revisions[collectionName]?
        return new Error("can't #{instructionName} documents without an explicit revision parameter given the revision config #{config.revision[collectionName]}")
    return null

_processUpdateRevisions = (revisions) ->
    #revisions can be null, string, array, or object
    if not revisions? then revisions = []
    if typeof revisions is 'string' then revisions = [revisions]
    if Array.isArray(revisions)
        revisionNames = revisions
        revisionNumbers = null
    else
        revisionNames = ( key for key of revisions )
        revisionNumbers = revisions
    return [revisionNames, revisionNumbers]


# should be possible to do committed.register('db', committed.db) to use these fns
#note: values passed to instructions should generally be considered immutable,
#as they'll be cloned after commit in the event of implicit rollback
#instructions must return done(err, result) where result is true for success and false for failure

# a revision is a counter, incremented during each update. Prior to each
# update the id and revision number of each document that'll be changed is
# recorded in the instruction state. Warning: for updates that effect millions
# of documents, this will result in millions of ids in the transaction.

# revisioning needs to be explicitly setup during start, as config.revision, eg
# { collectionName1: [subSchema1, subSchema2] } would be interpreted as:
#  - collectionName1 had two sub-schemas, so at insert collection1Doc.revision is {subSchema1: 0 subSchema2: 0}
#  - collectionName2 is not a revisioned collection and so will not have a .revision property.

exports.db =
    pass: (config, transaction, state, args, done) ->
        done(null, true)

    # an insert instruction. documents can be a single document or an array
    # thereof. revisions is optional, if present should be an array containing
    # the subschemas which are to be established in each document's .revision
    # subdoc. revisions may only contain subschemas mentioned in the
    # config.revisions.collectionName object. 
    insert: (config, transaction, state, [collectionName, documents, revisions, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to insert"))
        if not (collectionName? and documents?) then return done(new Error("null or missing argument to insert command"))
        if not Array.isArray(documents) then documents = [documents]

        if not revisions? then revisions = []
        #check the revisions param is consistent with any config.revisions settings
        error = _checkInstructionRevisionConfig 'insert', config, collectionName, revisions
        if error?
            return done(error)
        #check the docs dont have any revision info if we're intending to automatically set revision info
        if revisions.length > 0 and documents.some((d) -> d.revision?)
            return done(new Error("can't insert documents with revisions #{revisions} when some of the supplied documents already have a revision property"))


        config.db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            #inserts should _id every document, otherwise accurate rollback won't be possible, moreover these ids need to be saved somewhere
            state.ids = []
            for d in documents
                if d._id? 
                    state.ids.push d._id
                else
                    d._id = _pkFactory.createPk()
                    state.ids.push d._id
                #while we're looking at documents, check to see if we should create some revision info
                if revisions.length > 0 and not d.revision?
                    d.revision = {}
                    for revisionName in revisions
                        d.revision[revisionName] = 0
            options =
                w:1
                journal: true
                serializeFunctions: false
            _updateTransactionState transaction, (err) ->
                if err? then return done(err, null)
                collection.insert documents, options, (err, objects) ->
                    if err? then return done(err, null)
                    return done(null, true)

    insertRollback: (config, transaction, state, [collectionName, documents, revisions, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to insertRollback"))
        if not (collectionName? and documents?) then return done(new Error("null or missing argument to insertRollback command"))

        config.db.collection collectionName, {strict: true}, (err, collection) ->
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


    #updateOneOp is an update operation on one document using mongo
    #operations. revisions is optional, if  present then it needs to be either 
    # 1. a string containing revisions names which we should increment
    # 2. an array of such strings
    # 3. an object whose key,value are a revision name and revision number respectively
    # if you supply revision numbers, that saves a read+write. 
    updateOneOp: (config, transaction, state, [collectionName, selector, updateOps, rollbackOps, revisions, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneOp"))
        if not (collectionName? and selector? and updateOps? and rollbackOps?) then return done(new Error("null or missing argument to updateOneOp command"))

        [revisionNames, revisionNumbers] = _processUpdateRevisions revisions
        #check the revisions param is consistent with any config.revisions settings
        error = _checkInstructionRevisionConfig 'updateOneOp', config, collectionName, revisionNames
        if error?
            return done(error)
        #can't proceed if we've already got an $inc updateOps that involves the revisions 
        if updateOps.__inc? and revisionNames.some((name) -> updateOps.__inc["revision.#{name}"])
            return done(new Error("can't updateOneOp with revisions #{revisionNames} when there are already $inc operations on the revision sub-document"))
        #sames true of the selector if we're going to match an exact version number, this isnt really that precise a check, but it's at least a basic check
        if revisionNumbers? and revisionNames.some((name) -> selector["revision.#{name}"])
            return done(new Error("can't updateOneOp with revisions #{revisionNumbers} with a selector that already matches on the revision subdoc"))

        config.db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)

            #irrespective of whether we've been given required revision numbers, we need their current values written into the transaction right away
            mongoSelector = _mongolize(selector)
            mongoProjector = _id: 1
            for revisionName of revisionNames
                mongoProjector["revision.#{revisionName}"] = 1
            #we're going to read this into memory, it's got to go back out again to the transaction collection. 
            collection.find(mongoSelector, mongoProjector).toArray (err, docs) ->
                if err? then return done(err, null)
                if docs.length isnt 1 then return done(null, false) #the instruction has failed, we must find exactly one doc
                #if we have a document to update, we should save its id and exact revision into the transaction
                state.updated = docs[0]
                _updateTransactionState transaction, (err) ->
                    if err? then return done(err, null)
                    #now we're safe for a rollback 

                    executeUpdate = () ->
                        mongoUpdateOps = _mongolize(updateOps)
                        if revisionNames.length > 0
                            if not mongoUpdateOps.$inc? then mongoUpdateOps.$inc = {}
                            mongoUpdateOps.$inc["revision.#{revisionName}"] = 1 for revisionName in revisionNames
                        collection.update mongoSelector, mongoUpdateOps, _updateOneOptions, (err, updated) ->
                            if err? then return done(err, null)
                            if updated isnt 1 then return done(null, false) #the instruction has failed
                            return done(null, true)

                    if revisionNumbers?
                        #then we should check to see if what we just got meets the requirements
                        matches = ( state.updated[revisionName] is revisionNumber for revisionName, revisionNumber of revisionNumbers )
                        if not matches.every((x)->x)
                            #we have failed to meet the required version number
                            return done(null, false)
                        else
                            #revision requirements met, proceed with update
                            #change the updateOps to actually make the revision increments
                            executeUpdate()
                    else
                        #we can just go for the update
                        executeUpdate()


    updateOneOpRollback: (config, transaction, state, [collectionName, selector, updateOps, rollbackOps, revisions, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneOp"))
        if not (collectionName? and selector? and updateOps? and rollbackOps?) then return done(new Error("null or missing argument to updateOneOp command"))

        [revisionNames, revisionNumbers] = _processUpdateRevisions revisions

        config.db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)

            if not state.updated?
                #then we never got started on this transaction, so nothing to rollback
                return done(null, true)
            else
                #then we wrote ids and revisions before the update, can we still find these documents?
                revisionSelector = _id: state.updated._id
                for revisionName, revisionNumber of state.updated.revision
                    revisionSelector = revisionSelector["revision.#{revisionName}"] = revisionNumber
                collection.count revisionSelector, (err, count) ->
                    if count is 1
                        #then we have not made an update, so there's nothing to rollback
                        return done(null, true)
                    else if count is 0
                        #then we must have updated, so we must rollback. This
                        #is true because we wrote our state.updated values
                        #from a within-queue read.
                        mongoRollbackOps = _mongolize(rollbackOps)
                        if revisionNames.length > 0
                                if not mongoUpdateOps.$inc? then mongoUpdateOps.$inc = {}
                                mongoUpdateOps.$inc["revision.#{revisionName}"] = -1 for revisionName in revisionNames

                        return collection.update _mongolize(selector), mongoRollbackOps, _updateOneOptions, (err, updated) ->
                            if err? then return done(err, false)
                            if updated isnt 1 then return done(null, false) #the rollback has failed
                            return done(null, true)
                    else
                        #general rollback failure
                        return done(null, false)                            


    updateOneDoc: (config, transaction, state, [collectionName, newPartialDocument, oldPartialDocument, revisionName, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to revisionedUpdate"))
        if not (collectionName? and revisionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to revisionedUpdate command"))

        if not revisionName? or not config.revision?
            return done( new Error("to use updateOneDoc config.revision info must be given during committed.start"))

        config.db.collection collectionName, {strict: true}, (err, collection) ->
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

    updateOneDocRollback: (config, transaction, state, [collectionName, revisionName, newPartialDocument, oldPartialDocument, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to revisionedUpdateRollback"))
        if not (collectionName? and revisionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to revisionedUpdateRollback command"))

        config.db.collection collectionName, {strict: true}, (err, collection) ->
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


exports.transaction = (queueName, username, instructions, rollback) ->
    transaction =
        softwareVersion: _softwareVersion
        queue: queueName
        position: null
        startedAt: null
        enqueuedAt: null
        lastUpdatedAt: null
        enqueuedBy: username
        status: "Queued"
        instructions: if instructions? then instructions else []
        rollback: rollback
        execution:
            state: []
            errors: []
            info: []
            results: null
            rollback: null

# Create a chain from the supplied array of transactions. This is to encourage
# you not to create nested chains - these wont be executed. A chain is a list
# of transactions which will be executed in sequence. Each could be for a
# different queue, including GlobalLock. If one 'Fails' the chain is broken,
# the failed transaction is rolled back, but those before are not, and those
# after are not attempted.

# non-initial transactions within a chain must be pessimistic. if a non-
# initial transaction fails, it will be retried twice more at 1000ms
# intervals. Failure will return 'FailedChain' to the caller. 'Committed' will
# be returned to the caller once all transactions in the chain are finished.

exports.chain = (transactions) ->
    if not Array.isArray(transactions) or transactions.length is 0
        throw new Error("to chain transactions an array of transactions must be provided")
    first = transactions[0]
    first.before = []
    first.after = transactions[1..]
    return first