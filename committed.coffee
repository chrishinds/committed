async = require 'async'
ObjectID = require('mongodb').ObjectID
MongoObjects = ( require('mongodb')[t] for t in ['ObjectID', 'Binary', 'Code', 'DBRef', 'Double', 'MinKey', 'MaxKey', 'Symbol', 'Timestamp', 'Long'] )
BuiltInObjects = [ Array, Boolean, Date, Number, String, RegExp ]

_state = 'stopped'
_queueLength = {}
_immediateCounter = 0
_chainCounter = 0
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
            _enqueueAndHandleChains(transaction, done)
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
                _enqueueAndHandleChains transaction, (err, status, results...) ->
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
        _drainQueues [], done


_drainQueues = (exceptQueues, done) ->
    # wait until every _queue's _queueLength is 0, and there are no immediate transactions in progress, check every 10ms
    async.until(
        () ->
            ( count is 0 for name, count of _queueLength when name not in exceptQueues).every((x) -> x) and _immediateCounter is 0 
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

_enqueueAndHandleChains = (transaction, done) ->
    isChain = transaction?.after? and transaction?.before?
    results = []
    lastStatus = null
    chainGuard = null
    decrementQueueCounter = (queueName) ->
        if _queueLength[queueName]? 
            _queueLength[queueName] -= 1
            if _queueLength[queueName] is 0 
                delete _queueLength[queueName] #free the memory

    async.doWhilst(
        (bodyDone) ->
            #enqueue the transaction, if the right queue doesn't exist, create it first
            if not _queues[transaction.queue]?
                #the commit fn will be the queue worker; only one worker per queue for sequentiality
                _queues[transaction.queue] = async.queue(commit, 1)
                #when the queue's empty, free up memory
                _queues[transaction.queue].drain = () ->
                    delete _queues[transaction.queue]
                    # delete _queueLength[transaction.queue]   DO THIS ELSEWHERE
            _queueLength[transaction.queue] ?= 0
            #we're about to add an item to the queue, so increment the queue count
            _queueLength[transaction.queue] += 1
            if chainGuard?
                #then decrement a stretched queue counter
                decrementQueueCounter(chainGuard)
                chainGuard = null
            return _queues[transaction.queue].push transaction, (err, newTransaction, status, newResults...) ->
                originalTransaction = transaction

                if transaction.fnType is 'writer'
                    #then we may have produced a new transaction object
                    transaction = newTransaction
                    #so re-evaluate isChain
                    isChain = transaction?.after? and transaction?.before?

                #execution of this transaction is now finished, if the
                #transaction isnt a chain then its safe to revise the queue
                #counter
                if not isChain
                    decrementQueueCounter(originalTransaction.queue)
                else
                    #we need to stretch the queue counter's protection until after we start executing the next transaction
                    chainGuard = originalTransaction.queue
                #accumulate any results from the transaction
                results.push(result) for result in newResults
                #remember the status 
                lastStatus = status
                #yield to the event-loop to let any drain run, then see if we need to repeat 
                setImmediate bodyDone, err
        , () ->
            #this is the loop guard. we need to check whether the transaction
            #we just executed is a chain or not, if it is we should prepare
            #for another iteration of the enqueue loop body
            if isChain
                #then we have a chain, handle it
                if lastStatus is 'Committed'
                    if _hasAfter(transaction)
                        #then there are transaction yet to execute, we should move onto the next item in the chain
                        _shiftChain(transaction)
                        return true
                    else
                        #we are done with this chain, just return what we've got
                        return false 
                else
                    if not _hasBefore(transaction)
                        #then we are the first in the chain, so simply callback and let the client retry if they wish
                        return false
                    else
                        #we are not the chain's first transaction, so set the status to ChainFailed and return
                        lastStatus = 'ChainFailed'
                        return false
            else
                #we just executed a single transaction, so
                return false #to exit the loop and return the outcome
        , (err) ->
            if chainGuard?
                #tidy this up if we've just completed the last transaction in a chain
                decrementQueueCounter(chainGuard)
                chainGuard = null

            if err? 
                return done err, lastStatus, results...

            else if transaction.fnType is 'reader'
                #then there is no status to return, just the results.
                return done err, results...

            else if lastStatus is 'ChainFailed'
                #then we need to save this status and keep the real failure
                #status in transaction.execution.info. (we couldn't have done
                #this in the synchronous loop guard)
                return _updateTransactionStatus transaction, transaction.status, 'ChainFailed', null, null, transaction.status, (statusErr) ->
                    if statusErr? then return done statusErr, transaction.status, results...
                    return done err, 'ChainFailed', results...
            else
                #we simply return what we've got
                return done err, lastStatus, results...
        )


_checkTransaction = (transaction) ->
    if transaction.status? and transaction.status isnt 'Queued'
        return new Error("Can't queue a transaction which is at a status other than Queued (or null)")
    if typeof(transaction) isnt 'function'
        for i in transaction.instructions
            for key of i
                if key not in ['name', 'arguments'] 
                    return new Error("can't queue a transaction with an instruction that contains key '#{key}', instructions can contain only 'name' and 'arguments' keys")
        if not _instructionsExistFor(transaction)
            return new Error("Can't queue a transaction for which either its instructions, rollback instructions, or implied auto rollback instructions don't exist in the registry")
        if transaction.rollback? and transaction.rollback.length isnt transaction.instructions.length
            return new Error("Can't queue a transaction with an explicit rollback instructions whose length is not the same as its instructions array")
        if transaction.after? and not transaction.after.every( (x) -> x.status? and x.status is 'Queued' )
            return new Error("Can't queue a transaction chain where one transaction is at a status other than Queued")
    else
        if not transaction.fnType? or transaction.fnType not in ['reader', 'writer']
            return new Error("can't queue a function which doesn't have a fnType of 'reader' or 'writer'")

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
        return _enqueueAndHandleChains transaction, done
    else
        _pending.push [transaction, done]


exports.enqueue = (transaction, done) ->
    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run (eg. there's one on the GlobalLock queue)
    setImmediate () ->
        if _state is 'stopped'
            return done( new Error("unable to execute transaction immediately, committed was at state '#{_state}'"), null)
        else
            return _enqueue(transaction, done)


exports.immediately = (transaction, done) ->
    #no queue necessary, commit straight away, useful for inserts, but where you need a transaction as an audit record.
    # but as above, respect _state, and fail otherwise.
    #Yield to the event loop before performing state check. This ensures that
    #any drains are allowed time to run
    setImmediate () ->
        if _state is 'stopped'
            return done( new Error("unable to execute transaction immediately, committed was at state '#{_state}'"), null)
        else
            return _immediately(transaction, done)


_executeImmediate = (transaction, done) ->
    _immediateCounter += 1
    return commit transaction, (err, transaction, status, etc...) ->
        _immediateCounter -= 1
        done(err, status, etc...)

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
    return _enqueueAndHandleChains transaction, done



exports.withGlobalLock = (transaction, done) ->
    #useful if you want to safely update fixtures, or do aggregate calculations.
    # magic "GlobalLock" queue, with special drain function, that's set up during start
    #because exports.enqueue will yield to the event loop before pushing to
    #async.queue, we should also, otherwise we may inadvertently overtake a
    #previous call. setImmediate will preserve order. 
    setImmediate () ->
        if _state is 'stopped' 
            return done( new Error("unable to execute transaction immediately, committed was at state '#{_state}'"), null)
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
    statuses = []
    allResults = []
    iterator = ([instruction, state], iteratorDone) ->
        #assemble the arguments for the call of one of our registered functions
        args = if instruction.arguments? then instruction.arguments else []
        if not Array.isArray(args) then args = [args]
        fnArgs = [_config, transaction, state, args]
        #add the callback
        fnArgs.push (err, status, results...) ->
            #if an instruction has 'failed' by returning false, interrupt the series execution by returning an error
            if not err? and status is false
                err = 'instructionFailed'
            statuses.push status
            if results?
                allResults.push(result) for result in results
            return iteratorDone(err)
        #call the instruction's function
        applyToRegistered instruction.name, fnArgs
    #execute all the instructions in series
    instructionsWithState = ([instruction, state[i]] for instruction, i in instructions )
    async.eachSeries instructionsWithState, iterator, (err) ->
        #we may have terminated series by returning 'instructionFailed', however, it's necesary to separate failure from error
        if err is 'instructionFailed' then err = null
        return done(err, statuses, allResults)


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
    updateOneOptions = _makeUpdateOptions(true, false)
    _transactionsCollection.update { _id: transaction._id }, {$set: 'execution.state': transaction.execution.state}, updateOneOptions, (err,updated) ->
        if not err? and updated isnt 1 then err = new Error("transaction not correctly updated with instruction state before insert")
        done(err)

_clone = (obj) ->


    if not obj? or typeof obj isnt 'object'
        return obj

    if obj instanceof Array
        return ( _clone(x) for x in obj )

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
        newInstance[key] = _clone(value)

    return newInstance


#this is used to change a partial object into an array of mongo-dot-notation pairs
_mongoFlatten = (obj) ->
    results = []
    for key, value of obj
        #javascript you are so weak.
        if not value? or typeof value isnt 'object' or BuiltInObjects.some((x)-> value instanceof x) or MongoObjects.some((x)->value instanceof x)
            results.push [key, value]
        else
            flattened = _mongoFlatten(value)
            for [otherKey, otherValue] in flattened
                results.push ["#{key}.#{otherKey}", otherValue] 
    return results


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
                done(statusErr, transaction, newStatus)
            else if rollbackErr?
                _pushTransactionError transaction, rollbackErr, () ->
                    done(rollbackErr, transaction, newStatus)
            else
                done(null, transaction, newStatus)


#worker function for the GlobalLock queue
commitWithGlobalLock = (transaction, done) ->
    return _drainQueues ['GlobalLock'], (err) ->
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


#worker function for regular queues; manages transaction chains, delegates
#commit of the individual transaction to _commitCore
commit = (transactionOrFunction, done) ->
    #first, deal with the possibility that we've been given a function rather than a transaction object
    if typeof(transactionOrFunction) is 'function'
        #work out whether its a reader or a writer function
        if transactionOrFunction.fnType is 'reader'
            return transactionOrFunction (err, results...) ->
                #a reader function passes it's err and results straight to the callback
                done(err, transactionOrFunction, null, results...)
        if transactionOrFunction.fnType is 'writer'
            #if its a writer function then we should look at what the callback returns 
            return transactionOrFunction (err, transactionOrString, results...) ->
                #a writer function may either return a transaction, a chain, or a string
                if (typeof(transactionOrString) is 'string' or transactionOrString instanceof String or not transactionOrString?)
                    #if a string is returned then this should be the overall status of the transaction
                    return done(err, transactionOrFunction, transactionOrString, results...)
                else
                    #we have been given a transaction or chain as a result of calling our function.
                    if Array.isArray(transactionOrString)
                        #then we have been given a chain so make it into a transaction first
                        transaction = exports.chain transactionOrString
                    else
                        transaction = transactionOrString
                    #ensure that the queue specified on the original function is the same as that of the produced transaction
                    if transaction.queue isnt transactionOrFunction.queue
                        return done( new Error("""
                            transaction producing function assigned to a different queue from that of the transaction it produced 
                            (#{transaction.queue} isnt #{transactionOrFunction.queue})
                            """), transaction )
                    #make the same checks against the transaction as we would have done in committed.enqueue
                    error = _checkTransaction(transaction)
                    if error? 
                        return done( error, transaction ) 
                    #a writer function may have had a chain
                    if transactionOrFunction.before? or transactionOrFunction.after?
                        if transaction.before? or transaction.after? 
                            #then we have produced a chain as a result of the
                            #writer function, in this situation it isn't clear
                            #how to resolve the two chains into a sequential
                            #order, so error
                            return done( new Error(""" 
                                a committed.writer which was part of a chain has produced a transaction object which is
                                also part of a chain, it is not clear how to act sequentially on two chains.
                                """), transaction)
                        #copy the writer function's chain onto the object for onward processing. 
                        transaction.before = transactionOrFunction.before
                        transaction.after = transactionOrFunction.after
                    #if everything still looks good then do the commit.
                    return _commitCore transaction, results..., done
    else 
        #we must have been given a transaction object
        return _commitCore transactionOrFunction, done
 


#commits a single transaction
_commitCore = (transaction, writerResults..., done) ->
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
            execute transaction.instructions, transaction.execution.state, transaction, (err, statuses, transactionResults) ->
                if err?
                    return _pushTransactionError transaction, err, statuses, () ->
                        #if there's been an error in execution then we need to rollback everything
                        return rollback transaction, done
                else if statuses.length is transaction.instructions.length and statuses.every( (x) -> x )
                    #then the transaction has succeeded.
                    return _updateTransactionStatus transaction, 'Processing', 'Committed', statuses, (err) ->
                        # don't understand what these lines are for....
                        # if err?
                        #     _transactionsCollection.findOne {_id:transaction._id}, (err, doc) ->
                        writerResults.push(result) for result in transactionResults
                        return done(err, transaction, 'Committed', writerResults...)
                else
                    #the transaction has (legitimately) failed, write the statuses, and prepare for rollback 
                    _updateTransactionStatus transaction, transaction.status, transaction.status, statuses, null, "legitimate transaction failure", (statusErr) ->
                        #log these errors but don't pass them up
                        if err? then console.error "error saving transaction status change on legitimate failure: #{err}"
                        return rollback transaction, done


_makeUpdateOptions = (onlyOne, isUpsert) ->
    w:1
    journal: true
    upsert: isUpsert
    multi: not onlyOne
    serializeFunctions: false


_processRevisions = (instructionName, config, collectionName, revisions) ->
    #are the parameters consistent with the config?
    if not config.revisions? or not config.revisions[collectionName]?
        return [new Error("can't #{instructionName} documents in #{collectionName} without relevant config.revisions supplied at committed.start")]
    #revisions can be null, string, array, or object
    #if revisions is null, then grab all the revisions from config and use these
    if not revisions? then revisions = config.revisions[collectionName]
    #if it's a string, then make it an array
    if typeof revisions is 'string' then revisions = [revisions]
    if Array.isArray(revisions)
        #if we now have an array then we havn't been given specific version numbers, so these are null
        revisionNames = revisions
        revisionNumbers = null
    else
        #if we were given an object with exact revision numbers, make a names list, and return both
        revisionNames = ( key for key of revisions )
        revisionNumbers = revisions
    #now check the config is consistent with the revision names
    for revisionName in revisionNames
        if revisionName not in config.revisions[collectionName]
            return [new Error("can't #{instructionName} documents with a revision '#{revisionName}' not specified in the config.revisions given at committed.start")]
    return [null, revisionNames, revisionNumbers]

_mongoOpsFromTo = (mongoOps, fromDoc, toDoc) ->
    toFlattened = _mongoFlatten(toDoc)
    fromFlattened = _mongoFlatten(fromDoc)
    #the documents could contain too many revision keys, so don't copy these. 
    mongoOps.$set[key] = value for [key, value] in toFlattened when key.indexOf('revision.') isnt 0
    #make a list of fields to unset
    newDotPaths = ( key for key, value of mongoOps.$set )
    oldDotPaths = ( key for [key, value] in fromFlattened when key.indexOf('revision.') isnt 0 )
    unsetDotPaths = ( path for path in oldDotPaths when path not in newDotPaths )
    for path in unsetDotPaths
        mongoOps.$unset[path] = 1
    #remove _id from $set, we dont want to update this
    delete mongoOps.$set._id

_updateOpInstruction = (onlyOne, isUpsert, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done) ->
    #work out the instruction name from the onlyOne, isUpsert booleans
    instructionName = if isUpsert then 'upsert' else 'update'
    instructionName += if onlyOne then 'OneOp' else 'ManyOp'
    if etc.length isnt 0 then return done(new Error("too many values passed to #{instructionName}"))
    if not (collectionName? and selector? and updateOps?) then return done(new Error("null or missing argument to #{instructionName} instruction"))

    [error, revisionNames, revisionNumbers] = _processRevisions instructionName, config, collectionName, revisions
    if error?
        return done(error)

    if not onlyOne and revisionNumbers?
        return done( new Error("can't #{instructionName} with a revisions object that specifies exact revision numbers, that's only possible for singe updates"))
    #can't proceed if we've already got an $inc updateOps that involves the revisions 
    if updateOps.$inc? and revisionNames.some((name) -> updateOps.$inc["revision.#{name}"])
        return done(new Error("can't #{instructionName} with revisions #{revisionNames} when there are already $inc operations on the revision sub-document"))
    #sames true of the selector if we're going to match an exact version number, this isnt really that precise a check, but it's at least a basic check
    if revisionNumbers? and revisionNames.some((name) -> selector["revision.#{name}"])
        return done(new Error("can't #{instructionName} with revisions #{revisionNumbers} with a selector that already matches on the revision subdoc"))
    if revisionNumbers? and isUpsert
        return done(new Error("can't #{instructionName} (upsert) when specific revision numbers #{revisionNumbers} have been specified"))

    config.db.collection collectionName, {strict: true}, (err, collection) ->
        if err? then return done(err, false)

        #irrespective of whether we've been given required revision numbers, we need their current values written into the transaction right away
        mongoSelector = _clone(selector)
        if rollbackProjector?
            mongoProjector = _clone(rollbackProjector)
            mongoProjector._id = 1
            #always get the revision sub-doc - whittle this down to just the revisions of interest during rollback
            mongoProjector.revision = 1
        else
            #if we weren't given a rollback projector then grab the whole document.
            mongoProjector = {}

        #we're going to read current values into memory, then send it back out
        #to the transaction collection. this instruction consequently needs to
        #be used with care, if you need to update a million documents,
        #consider using a different instruction.
        collection.find(mongoSelector, mongoProjector).toArray (err, docs) ->
            if err? then return done(err, null)
            #look at the search results and see if these are consistent with the instruction we've been asked to execute
            if (onlyOne and docs.length > 1) or (not isUpsert and docs.length is 0)
                #the instruction has failed, we must find exactly one doc
                transaction.execution.info.push "#{instructionName} can update only one document, using #{JSON.stringify mongoSelector} #{docs.length} were found"
                return done(null, false) 
            else if isUpsert and docs.length is 0
                #then we are effectively about to do an insert, so record this
                #in the state what we need is to get hold of the id that's
                #going to be used and if we can't decide on it here
                if selector?._id?
                    #then we've specified an id in the selector, so use this
                    state.insertedId = selector._id
                else
                    #we should create an _id, save it to state for rollback,
                    #then put it in the selector, even though we know we wont
                    #find this _id, mongo will combine the selector and
                    #operations to make the new document
                    state.insertedId = new ObjectID()
                    selector ?= {}
                    selector._id = state.insertedId
            else if onlyOne
                #if we have a document to update, we should save its id and exact revision into the transaction
                state.updated = docs[0]
            else
                #we are updating many documents, so record these.
                state.updated = docs

            _updateTransactionState transaction, (err) ->
                if err? then return done(err, null)
                #now we're safe for a rollback 

                executeUpdate = () ->
                    options = _makeUpdateOptions(onlyOne, isUpsert)
                    mongoUpdateOps = _clone(updateOps)
                    if revisionNames.length > 0
                        mongoUpdateOps.$inc ?= {}
                        mongoUpdateOps.$inc["revision.#{revisionName}"] = 1 for revisionName in revisionNames
                    #note that we're not using $isolated. This doesn't work
                    #over shards and committed's queue structure should be
                    #used to ensure isolation.
                    collection.update mongoSelector, mongoUpdateOps, options, (err, updated) ->
                        if err? then return done(err, null)
                        if onlyOne and updated isnt 1 
                            transaction.execution.info.push """
                                #{instructionName} can update only one document, using #{JSON.stringify mongoSelector} #{updated} were updated
                                """
                            return done(null, false) #the instruction has failed
                        if onlyOne
                            return done(null, true)
                        else
                            #if this is an update many then return the number of records updated.
                            return done(null, true, updated)

                if revisionNumbers? and onlyOne #because we can't specify revision numbers for a multi-update
                    #then we should check to see if what we just got meets the requirements
                    matches = ( state.updated.revision[revisionName] is revisionNumber for revisionName, revisionNumber of revisionNumbers )
                    if not matches.every((x)->x)
                        #we have failed to meet the required version number
                        transaction.execution.info.push """
                            #{instructionName} failed to find documents to match the required revisions. 
                            Found: #{JSON.stringify state.updated}. 
                            Required: #{JSON.stringify revisionNumbers}.
                            """
                        return done(null, false)
                    else
                        #revision requirements met, proceed with update
                        #change the updateOps to actually make the revision increments
                        executeUpdate()
                else
                    #we can just go for the update
                    executeUpdate()


_updateOpRollback = (onlyOne, isUpsert, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done) ->
    instructionName = if onlyOne then 'updateOneOpRollback' else 'updateManyOpRollback'
    if etc.length isnt 0 then return done(new Error("too many values passed to #{instructionName}"))
    if not (collectionName? and selector? and updateOps?) then return done(new Error("null or missing argument to #{instructionName} command"))

    [error, revisionNames, revisionNumbers] = _processRevisions instructionName, config, collectionName, revisions
    if error?
        return done(error)

    config.db.collection collectionName, {strict: true}, (err, collection) ->
        if err? then return done(err, false)

        if not state.updated? and not state.insertedId?
            #then we never got started on this transaction, so nothing to rollback
            return done(null, true)
        if state.insertedId?
            #then we have done an upsert, but the document didn't already
            #exist, we really inserted, which means we need to remove a
            #matching document
            options =
                w:1
                journal: true
                serializeFunctions: false
            return collection.remove {_id: state.insertedId}, options, (err, result) ->
                return done(err)
            
        else
            #then there was an update; we wrote ids and revisions before the
            #update, can we still find these documents? we could easily have
            #failed in the middle of an updateMany, so check one by one.
            docs = if onlyOne then [state.updated] else state.updated

            rollbackDoc = (doc, rollbackDone) ->
                revisionSelector = _id: doc._id
                for revisionName, revisionNumber of doc.revision
                    revisionSelector["revision.#{revisionName}"] = revisionNumber
                collection.count revisionSelector, (err, count) ->
                    if count is 1
                        #then we have not made an update, so there's nothing to rollback
                        return rollbackDone(null, true)
                    else if count is 0
                        #then we must have updated, so we must rollback. This
                        #is true because we wrote our state.updated values
                        #from a within-queue read.
                        mongoRollbackOps = $set: {}, $inc: {}, $unset:{}
                        #only decrement the revision numbers that we specifically asked to increment
                        mongoRollbackOps.$inc["revision.#{revisionName}"] = -1 for revisionName in revisionNames
                        #take the projection that was stored and convert it to mongo dot notation
                        flattened = _mongoFlatten(doc)
                        #the stored projection could contain too many revision keys, so don't copy these. 
                        #also don't update _id.
                        mongoRollbackOps.$set[key] = value for [key, value] in flattened when key.indexOf('revision.') isnt 0
                        #make a list of fields to unset
                        #must include any operation that sets a field which wasnt in state.updated; take care of rename
                        originalDotPaths = ( key for key, value of mongoRollbackOps.$set when key.indexOf('revision.') isnt 0)
                        settyOperators = ['$inc', '$setOnInsert', '$set', '$addToSet', '$push', '$bit']
                        updatedDotPaths = []
                        for op in settyOperators
                            if updateOps[op]?
                                updatedDotPaths.push(path) for path of updateOps[op]
                        unsetDotPaths = ( path for path in updatedDotPaths when path not in originalDotPaths )
                        #$rename is a special case, it's the rename-to field that we're interested
                        if updateOps['$rename']?
                            renamedTo = ( to for from, to of updateOps['$rename'] )
                            unsetDotPaths.push(to) for to in renamedTo when to not in originalDotPaths
                        for path in unsetDotPaths
                            mongoRollbackOps.$unset[path] = 1
                        #remove _id from $set, we dont want to update this
                        delete mongoRollbackOps.$set._id

                        #choose just the document that was updated from it's _id
                        mongoSelector = _id: doc._id

                        # since we're updating one document at a time, we want restricted options
                        updateOneOptions = _makeUpdateOptions(true, false)
                        return collection.update mongoSelector, mongoRollbackOps, options, (err, updated) ->
                            if err? then return rollbackDone(err, false)
                            if updated isnt 1
                                transaction.execution.info.push """
                                    updateOneOpRollback failed to update exactly one document (actually #{updated}) during rollback
                                    """
                                return rollbackDone(null, false) #the rollback has failed
                            return rollbackDone(null, true)
                    else
                        #general rollback failure
                        transaction.execution.info.push """
                            updateOneOpRollback found too many documents (#{count}) when trying to find out if an updateOneOp had occurred
                            """
                        return rollbackDone(null, false)

            #apply rollbackDoc to each doc we recorded in state
            async.mapSeries docs, rollbackDoc, (err, rollbackResults) ->
                if err? then return done(err, null)
                #rollback is a success if each individual rollback returns status true.
                return done(err, rollbackResults.every((x) -> x))


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

# if you decide to fail an instruction by returning done(null, false) you should push a reason to transaction.execution.info

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

        [error, revisionNames, revisionNumbers] = _processRevisions 'insert', config, collectionName, revisions
        if error?
            return done(error)
        if revisionNumbers? then return done(new Error("can't insert using specific revision numbers #{revisionNumbers}"))

        #check the docs dont have any revision info if we're intending to automatically set revision info
        if documents.some((d) -> d.revision?)
            return done(new Error("can't insert documents when some already have a revision property"))

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
                if revisionNames.length > 0 and not d.revision?
                    d.revision = {}
                    for revisionName in revisionNames
                        d.revision[revisionName] = 1
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
    # mongoProjector is used to take a copy of the pre-commit values, if it's null, the whole document will be copied
    # the object must not contain projection operations.
    updateOneOp: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpInstruction(true, false, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)

    updateOneOpRollback: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpRollback(true, false, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)

    upsertOneOp: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpInstruction(true, true, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)

    upsertOneOpRollback: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpRollback(true, true, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)        

    updateManyOp: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpInstruction(false, false, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)

    updateManyOpRollback: (config, transaction, state, [collectionName, selector, updateOps, rollbackProjector, revisions, etc...], done) ->
        return _updateOpRollback(false, false, config, transaction, state, collectionName, selector, updateOps, rollbackProjector, revisions, etc, done)

    #updateOneDoc updates a parts of a document. It isnt save, if you want to
    #save the whole document you should use a different instruction.

    updateOneDoc: (config, transaction, state, [collectionName, newPartialDocument, oldPartialDocument, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneDoc"))
        if not (collectionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to updateOneDoc command"))
        if not oldPartialDocument._id? or not newPartialDocument._id? or not oldPartialDocument._id.equals(newPartialDocument._id)
            return done(new Error("both old and new partial documents must have _ids, and they must match to updateOneDoc"))
        if not oldPartialDocument.revision? then return done(new Error("oldPartialDocument must contain revision information to updateOneDoc"))

        #take the revision sub-doc of the old partial doc to be the revisions that we're interested in
        revisionNames = ( revisionName for revisionName of oldPartialDocument.revision )

        [error, x, y] = _processRevisions 'updateOneDoc', config, collectionName, revisionNames
        if error?
            return done(error)

        #first we must establish whether the revision requirements for this
        #command can be satisfied. specifically, whether the document revision
        #in oldPartialDocument is current
        config.db.collection collectionName, {strict: true}, (err, collection) ->
            if err? then return done(err, false)
            oldSelector = {}
            oldSelector[key] = value for [key, value] in _mongoFlatten(oldPartialDocument)
            collection.count oldSelector, (err, count) ->
                if err? then return done(err, false)
                #this will also confirm that in the event of rollback we'll be writing something valid.
                if count isnt 1
                    transaction.execution.info.push """
                        updateOneDoc failed to find exactly one document (found #{count}) matching the given oldPartialDocument using #{oldSelector})
                        """
                    return done(null, false)
                #if we find exactly one match then our revision requirement is met, we should record the fact that the instruction is safe to proceed
                state.safeToExecuteInstruction = true
                _updateTransactionState transaction, (err) ->
                    if err? then return done(err, null)
                    #the state is now saved - we'll know what to do in the event of rollback.
                    #now work out the operations necessary to do a partialdoc update
                    mongoOps = $set: {}, $unset:{}, $inc:{}
                    _mongoOpsFromTo(mongoOps, oldPartialDocument, newPartialDocument)
                    #update all the relevant revisions
                    mongoOps.$inc["revision.#{revisionName}"] = 1 for revisionName of oldPartialDocument.revision
                    updateOneOptions = _makeUpdateOptions(true, false)
                    collection.update {_id: oldPartialDocument._id}, mongoOps, updateOneOptions, (err, updated) ->
                        if err? then return done(err, null)
                        if updated isnt 1 
                            transaction.execution.info.push """
                                updateOneDoc must update exactly one document, using #{JSON.stringify mongoOps} #{updated} were updated
                                """
                            return done(null, false) #the instruction has failed
                        return done(null, true)


    updateOneDocRollback: (config, transaction, state, [collectionName, newPartialDocument, oldPartialDocument, etc...], done) ->
        if etc.length isnt 0 then return done(new Error("too many values passed to updateOneDocRollback"))
        if not (collectionName? and newPartialDocument? and oldPartialDocument?) then return done(new Error("null or missing argument to updateOneDocRollback command"))
        if not oldPartialDocument._id? or not newPartialDocument._id? or not oldPartialDocument._id.equals(newPartialDocument._id)
            return done(new Error("both old and new partial documents must have _ids, and they must match to updateOneDocRollback"))
        if not oldPartialDocument.revision? then return done(new Error("oldPartialDocument must contain revision information to updateOneDocRollback"))

        if not state.safeToExecuteInstruction
            #then the revision requierments for this instruction were not met,
            #or the instruction was interrupted early, so there is nothing to
            #rollback
            return done(null, true)
        else
            #revision requirments for the original instruction were met
            config.db.collection collectionName, {strict: true}, (err, collection) ->
                if err? then return done(err, false)

                #because revision requirements were met at start of
                #instruction, if the revision numbers was bumped up, it means
                #the instruction was executed
                selector = {_id: oldPartialDocument._id }
                selector["revision.#{revisionName}"] = oldPartialDocument.revision[revisionName] + 1 for revisionName of oldPartialDocument.revision
                collection.count selector, (err, count) ->
                    if err? then return done(err, false)
                    if count is 0
                        #then the update was never completed, there's nothing to rollback
                        return done(null, true)
                    else
                        #the update must have gone through, rollback
                        mongoOps = $set: {}, $unset:{}
                        _mongoOpsFromTo(mongoOps, newPartialDocument, oldPartialDocument)
                        #reset all the relevant revisions
                        mongoOps.$set["revision.#{revisionName}"] = oldPartialDocument.revision[revisionName] for revisionName of oldPartialDocument.revision
                        updateOneOptions = _makeUpdateOptions(true, false)
                        collection.update {_id: oldPartialDocument._id}, mongoOps, updateOneOptions, (err, updated) ->
                                if err? then return done(err, null)
                                if updated isnt 1 
                                    transaction.execution.info.push """
                                        updateOneDocRollback must update exactly one document, using #{JSON.stringify mongoOps} #{updated} were updated
                                        """
                                    return done(null, false) #the instruction has failed
                                return done(null, true)



# helper function to produce a basic object to represent a transaction; what
# the transactions collection will contain. Use additionalFields to add stuff
# like which user or which REST route executed the transaction.

exports.transaction = (queueName, additionalFields, instructions, rollback) ->
    t = 
        softwareVersion: _softwareVersion
        queue: queueName
        position: null
        startedAt: null
        enqueuedAt: null
        lastUpdatedAt: null
        status: "Queued"
        instructions: if instructions? then instructions else []
        rollback: rollback
        execution:
            state: []
            errors: []
            info: []
            results: null
            rollback: null
    if additionalFields?
        t[key] = value for key, value of additionalFields
    return t

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
    if transactions.some((x) -> typeof x is 'function')
        throw new Error("cannot form a chain from reader/writer functions; chains must be composed of only transaction objects")
    first = transactions[0]
    first.before = []
    first.after = transactions[1..]
    return first


#create a function for execution within a given queue. a writer takes a function:
#   fn = (done) -> ...
# and whose callback looks like:
#   done(err, transactionChainOrString)
# if done is given:
#  - a string or null: this is taken to be the transaction's final status, and is returned; no transaction is written to the db
#  - a transaction object: this is saved then executed immediately within the current queue
#  - an array of transactions: this is converted to a chain and executed immediately within the current queue

exports.writer = (queueName, fn) ->
    if not typeof fn is 'function'
        throw new Error("to create a committed.writer a function must be given as the second argument")
    fn.fnType = 'writer'
    fn.queue = queueName
    fn.status = "Queued"
    return fn

#create a function for execution within a given queue. a reader takes a function:
#   fn = (done) -> ...
# its done callback is to be used:
#   done(err, result1, result2, result...)
# where the results are whatever has been created from a set of read operations within the queue

exports.reader = (queueName, fn) ->
    if not typeof fn is 'function'
        throw new Error("to create a committed.reader a function must be given as the second argument")
    fn.fnType = 'reader'
    fn.queue = queueName
    fn.status = "Queued"
    return fn